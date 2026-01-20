// index.js - backend único ENEL / GLPI

require('dotenv').config();

const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');
const axios = require('axios');
const multer = require('multer');
const sharp = require('sharp');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const FormData = require('form-data');
const mime = require('mime-types');

const app = express();

// ---------------------------------------------------------------------------
// CONFIG GERAIS
// ---------------------------------------------------------------------------

const PORT = process.env.PORT || 3000;
const PROJECT_ID = Number(process.env.PROJECT_ID || '0') || 0;

const MAX_PART_BYTES = 20 * 1024 * 1024; // 20MB

const OPTS = {
    maxDim: 2000,
    jpegQuality: 80,
    webpQuality: 80,
    minQuality: 40,
    qualityStep: 10,
};

// pasta temporária de upload local
const UPLOAD_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) {
    fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}

const upload = multer({
    dest: UPLOAD_DIR,
    limits: {
        files: 10,
        fileSize: 100 * 1024 * 1024, // 100MB bruto, antes de otimizar/split
    },
});

// cache de subtarefas GLPI em disco
const SUBTASK_CACHE_FILE = path.join(__dirname, 'subtask-cache.json');
let SUBTASK_CACHE = null;

// ---------------------------------------------------------------------------
// POOLS DE BANCO (ENEL + GLPI)
// ---------------------------------------------------------------------------

const enelPool = mysql.createPool({
    host: process.env.ENEL_DB_HOST,
    port: Number(process.env.ENEL_DB_PORT) || 3306,
    user: process.env.ENEL_DB_USER,
    password: process.env.ENEL_DB_PASS,
    database: process.env.ENEL_DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
});

const glpiPool = mysql.createPool({
  host: process.env.GLPI_DB_HOST,
  port: process.env.GLPI_DB_PORT,
  user: process.env.GLPI_DB_USER,
  password: process.env.GLPI_DB_PASS,
  database: process.env.GLPI_DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  // give the driver time to reconnect on slow setups
  connectTimeout: 20000,
  ssl: {
    rejectUnauthorized: false,
  },
});

// helper simples de retry em queries
async function execOnceWithRetry(sql, params = [], retries = 2) {
    let attempt = 0;
    while (true) {
        try {
            const [result] = await enelPool.query(sql, params);
            return result;
        } catch (err) {
            if (attempt >= retries) throw err;
            console.warn('DB retry', attempt + 1, err.code || err.message);
            attempt += 1;
            await new Promise((res) => setTimeout(res, 500 * attempt));
        }
    }
}
async function queryWithRetry(pool, sql, params = [], attempts = 2) {
    let lastErr;
    for (let i = 0; i < attempts; i++) {
        try {
            const [rows] = await pool.query(sql, params); // call the real pool
            return rows;
        } catch (err) {
            lastErr = err;
            const transient = [
                'ECONNRESET',
                'PROTOCOL_CONNECTION_LOST',
                'ER_SERVER_SHUTDOWN',
                'ETIMEDOUT',
                'EPIPE',
            ];
            if (transient.includes(err.code) && i < attempts - 1) {
                console.warn(`[mysql] transient ${err.code}; retrying ${i + 1}/${attempts - 1}`);
                continue;
            }
            throw err;
        }
    }
    throw lastErr;
}
// ---------------------------------------------------------------------------
// CLIENTE GLPI (API REST)
// ---------------------------------------------------------------------------

const glpiBaseUrl = process.env.GLPI_URL;
const APP_TOKEN = process.env.APP_TOKEN;
const USER_TOKEN = process.env.USER_TOKEN;
const GLPI_FRONT_URL = process.env.GLPI_FRONT_URL || '';

if (!glpiBaseUrl || !APP_TOKEN || !USER_TOKEN) {
    console.warn('GLPI_URL / APP_TOKEN / USER_TOKEN não configurados no .env');
}

// sessão GLPI
async function glpiInitSession() {
    const { data } = await axios.get(`${glpiBaseUrl}/initSession`, {
        headers: {
            'App-Token': APP_TOKEN,
            'Authorization': `user_token ${USER_TOKEN}`,
        },
    });
    const token = data && data.session_token;
    if (!token) throw new Error('Failed to get GLPI Session-Token');
    return token;
}

async function glpiKillSession(sessionToken) {
    try {
        await axios.get(`${glpiBaseUrl}/killSession`, {
            headers: {
                'App-Token': APP_TOKEN,
                'Session-Token': sessionToken,
            },
        });
    } catch (_) {
        // ignora
    }
}

async function glpiGetProjectTask(sessionToken, id) {
    const { data } = await axios.get(`${glpiBaseUrl}/ProjectTask/${id}`, {
        headers: {
            'App-Token': APP_TOKEN,
            'Session-Token': sessionToken,
        },
    });
    return data;
}

// ---------------------------------------------------------------------------
// CACHE DE SUBTAREFAS GLPI
// ---------------------------------------------------------------------------

async function loadSubtaskCache() {
    if (SUBTASK_CACHE) return SUBTASK_CACHE;
    try {
        const raw = await fsp.readFile(SUBTASK_CACHE_FILE, 'utf8');
        SUBTASK_CACHE = JSON.parse(raw);
    } catch {
        SUBTASK_CACHE = {};
    }
    return SUBTASK_CACHE;
}

async function saveSubtaskCache() {
    if (!SUBTASK_CACHE) return;
    await fsp.writeFile(
        SUBTASK_CACHE_FILE,
        JSON.stringify(SUBTASK_CACHE, null, 2),
        'utf8'
    );
}

// ---------------------------------------------------------------------------
// HELPER: OTIMIZAR IMAGEM + SPLIT
// ---------------------------------------------------------------------------

async function optimizeImageFile(inputPath, originalName, mimeType, sizeLimitBytes = MAX_PART_BYTES) {
    if (!/^image\//i.test(mimeType)) {
        return { path: inputPath, filename: originalName, optimized: false };
    }

    let image = sharp(inputPath, { failOnError: false });
    const meta = await image.metadata();

    if (meta.width && meta.height) {
        const larger = Math.max(meta.width, meta.height);
        if (larger > OPTS.maxDim) {
            image = image.resize({
                width: meta.width >= meta.height ? OPTS.maxDim : null,
                height: meta.height > meta.width ? OPTS.maxDim : null,
                withoutEnlargement: true,
                fit: 'inside',
            });
        }
    }

    const hasAlpha = !!meta.hasAlpha;
    let target = 'jpeg';

    let quality = target === 'webp' ? OPTS.webpQuality : OPTS.jpegQuality;

    const encode = (q) => {
        let pipeline = image.clone().withMetadata({ orientation: meta.orientation });
        pipeline = pipeline.rotate();
        if (target === 'jpeg') {
            pipeline = pipeline.jpeg({
                quality: q,
                progressive: true,
                mozjpeg: true,
                chromaSubsampling: '4:2:0',
            });
        } else if (target === 'webp') {
            pipeline = pipeline.webp({
                quality: q,
                alphaQuality: hasAlpha ? 75 : undefined,
                smartSubsample: true,
            });
        } else {
            target = 'jpeg';
            pipeline = pipeline.jpeg({ quality: q, progressive: true, mozjpeg: true });
        }
        return pipeline.toBuffer();
    };

    let out = await encode(quality);
    while (out.length > sizeLimitBytes && quality > OPTS.minQuality) {
        quality = Math.max(OPTS.minQuality, quality - OPTS.qualityStep);
        out = await encode(quality);
    }

    const newExt = '.jpg';
    const newName = originalName.replace(/\.[^.]+$/g, '') + newExt;
    const outPath = path.join(path.dirname(inputPath), `${Date.now()}-opt-${newName}`);
    await fsp.writeFile(outPath, out);

    try { await fsp.unlink(inputPath); } catch { }

    return {
        path: outPath,
        filename: newName,
        optimized: true,
        finalBytes: out.length,
    };
}

async function splitFile(filePath, sizeLimitBytes = MAX_PART_BYTES) {
    const stats = await fsp.stat(filePath);
    if (stats.size <= sizeLimitBytes) {
        return [filePath];
    }

    const buf = await fsp.readFile(filePath);
    const total = buf.length;
    const partsCount = Math.ceil(total / sizeLimitBytes);
    const partPaths = [];

    for (let i = 0; i < partsCount; i++) {
        const start = i * sizeLimitBytes;
        const end = Math.min(start + sizeLimitBytes, total);
        const partBuf = buf.subarray(start, end);

        const partPath = path.join(
            path.dirname(filePath),
            `${Date.now()}-part-${String(i + 1).padStart(2, '0')}-${path.basename(filePath)}`
        );
        await fsp.writeFile(partPath, partBuf);
        partPaths.push(partPath);
    }

    try { await fsp.unlink(filePath); } catch { }

    return partPaths;
}

// Upload de um arquivo físico para GLPI Document
// FIXED: glpiUploadDocument (Buffer version)
async function glpiUploadDocument(sessionToken, filePath, displayName) {
    const niceName = displayName || path.basename(filePath);
    const uploadField = "filename[0]";

    const fileBuffer = await fsp.readFile(filePath); // ← FIX: read as buffer

    const form = new FormData();

    const manifest = {
        input: {
            name: niceName,
            _filename: [niceName],
        },
    };

    form.append("uploadManifest", JSON.stringify(manifest), {
        contentType: "application/json",
    });

    form.append(uploadField, fileBuffer, {
        filename: niceName,
        contentType: mime.lookup(niceName) || "application/octet-stream",
    });

    const contentLength = await new Promise((resolve, reject) => {
        form.getLength((err, length) => {
            if (err) return reject(err);
            resolve(length);
        });
    });

    const resp = await axios.post(`${glpiBaseUrl}/Document`, form, {
        headers: {
            "App-Token": APP_TOKEN,
            "Session-Token": sessionToken,
            ...form.getHeaders(),
            "Content-Length": contentLength,
        },
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
    });

    const docId = resp.data?.id ?? resp.data?.[0]?.id;
    if (!docId) throw new Error("GLPI did not return ID");

    return Number(docId);
}

// Linkar o document a um item (ProjectTask, Ticket, etc.)
async function glpiLinkDocument(sessionToken, { documentId, itemtype, itemsId }) {
    const payload = {
        input: {
            documents_id: documentId,
            itemtype,
            items_id: itemsId,
        },
    };

    const resp = await axios.post(`${glpiBaseUrl}/Document_Item`, payload, {
        headers: {
            'App-Token': APP_TOKEN,
            'Session-Token': sessionToken,
            'Content-Type': 'application/json',
        },
    });

    return resp.data;
}

// ---------------------------------------------------------------------------
// ATUALIZAÇÃO DE LINKS EM TABELA despesas_arquivos
// ---------------------------------------------------------------------------
// Table: despesas_arquivos
//  id int AI PK
//  despesas_id int
//  user_id int
//  glpi_links text
//  created_at timestamp
//  updated_at timestamp

async function updateDespesaPhotoLink({ despesaId, docId }) {
    try {
        // busca user_id na tabela despesas
        const [rows] = await enelPool.query(
            'SELECT user_id FROM despesas WHERE despesa_id = ?',
            [despesaId]
        );
        const userId = rows[0]?.user_id || null;

        // monta link GLPI para o documento
        const link = GLPI_FRONT_URL
            ? `${GLPI_FRONT_URL}/front/document.form.php?id=${docId}`
            : String(docId);

        const sql = `
      INSERT INTO despesas_arquivos
        (despesas_id, user_id, glpi_links, photo_docid, created_at, updated_at)
      VALUES (?, ?, ?, ?, NOW(), NOW())
    `;
        await execOnceWithRetry(sql, [despesaId, userId, link, docId]);

        return link; // devolve o link para quem chamou
    } catch (err) {
        console.warn('updateDespesaPhotoLink falhou (ajuste schema se necessário):', err.message);
        return null;
    }
}

// ---------------------------------------------------------------------------
// MIDDLEWARES EXPRESS
// ---------------------------------------------------------------------------

app.use(cors());
app.use(express.json({ limit: '20mb' }));
app.use(express.urlencoded({ extended: true }));

// ---------------------------------------------------------------------------
// PROXY ROUTE - Download document from GLPI
// ---------------------------------------------------------------------------

app.get('/proxy/document/:id', async (req, res) => {
    const docid = Number(req.params.id);
    if (!Number.isFinite(docid) || docid <= 0) {
        return res.status(400).json({ error: 'Invalid document ID' });
    }

    let sessionToken;

    // derive "front" base from GLPI_URL when GLPI_FRONT_URL is not provided
    const glpiFrontBase = () => {
        if (process.env.GLPI_FRONT_URL && process.env.GLPI_FRONT_URL.trim()) {
            return process.env.GLPI_FRONT_URL.replace(/\/+$/, '');
        }
        // glpiBaseUrl may include /apirest.php, strip it like in the old app
        return String(glpiBaseUrl).replace(/\/apirest\.php\/?$/, '');
    };

    // headers helper
    const setDownloadHeaders = ({
        filename,
        mime,
        length,
        dispositionFromServer,
        typeFromServer,
    }) => {
        if (dispositionFromServer) {
            res.setHeader('Content-Disposition', dispositionFromServer);
        } else {
            // RFC 5987 for UTF-8 filenames
            const safe = filename.replace(/"/g, "'");
            res.setHeader(
                'Content-Disposition',
                `attachment; filename="${safe}"; filename*=UTF-8''${encodeURIComponent(
                    filename
                )}`
            );
        }
        res.setHeader(
            'Content-Type',
            typeFromServer || mime || 'application/octet-stream'
        );
        if (length) res.setHeader('Content-Length', length);
        // avoid caching sensitive files
        res.setHeader(
            'Cache-Control',
            'no-store, no-cache, must-revalidate, proxy-revalidate'
        );
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
    };

    const inferExt = (mime) => {
        if (!mime) return null;
        const m = mime.toLowerCase();
        if (m.includes('jpeg') || m === 'image/jpg') return 'jpg';
        if (m.includes('png')) return 'png';
        if (m.includes('gif')) return 'gif';
        if (m.includes('pdf')) return 'pdf';
        if (m.includes('plain')) return 'txt';
        if (m.includes('json')) return 'json';
        return null;
    };

    const looksLikeHtml = (ctype) =>
        typeof ctype === 'string' && ctype.toLowerCase().startsWith('text/html');

    try {
        // 1) Start REST session via helper
        sessionToken = await glpiInitSession();
        if (!sessionToken) {
            return res
                .status(502)
                .json({ error: 'Failed to start GLPI session (no token returned)' });
        }

        // 2) Fetch Document metadata (to build nice filename)
        let meta = {};
        try {
            const metaResp = await axios.get(`${glpiBaseUrl}/Document/${docid}`, {
                headers: {
                    'App-Token': APP_TOKEN,
                    'Session-Token': sessionToken,
                },
                validateStatus: () => true,
            });
            if (metaResp.status === 200 && metaResp.data) {
                meta = metaResp.data;
            }
        } catch (e) {
            // non-fatal, continue with defaults
            console.error('[doc proxy] meta error:', e?.message);
        }

        const stored = (meta.filename || '').toString().trim();
        const display = (meta.name || '').toString().trim();
        const mime =
            (meta.mime || '').toString().trim() || 'application/octet-stream';

        const displayHasExt = /\.[a-z0-9]{2,8}$/i.test(display);
        let filename = displayHasExt ? display : stored || `document-${docid}`;

        if (!/\.[a-z0-9]{2,8}$/i.test(filename)) {
            const ext = inferExt(mime);
            if (ext) filename = `${filename}.${ext}`;
        }

        // 3) Try official REST download endpoints first
        const tryRestUrl = async (url) => {
            const r = await axios.get(url, {
                responseType: 'stream',
                headers: {
                    'App-Token': APP_TOKEN,
                    'Session-Token': sessionToken,
                    Accept: 'application/octet-stream',
                },
                maxRedirects: 3,
                validateStatus: (s) => s >= 200 && s < 400,
            });
            return r;
        };

        const restCandidates = [
            `${glpiBaseUrl}/Document/${docid}/download`,
            `${glpiBaseUrl}/Document/${docid}?download=1`,
        ];

        let dlResp = null;
        for (const url of restCandidates) {
            try {
                const r = await tryRestUrl(url);
                const ctype = r.headers['content-type'] || '';
                if (!looksLikeHtml(ctype)) {
                    dlResp = r;
                    break;
                }
            } catch (e) {
                // try next candidate
                console.error('[doc proxy] REST download candidate failed:', url, e?.message);
            }
        }

        // 4) Fallback to Front (some setups require this)
        if (!dlResp) {
            try {
                dlResp = await axios.get(
                    `${glpiFrontBase()}/front/document.send.php?docid=${docid}`,
                    {
                        responseType: 'stream',
                        headers: {
                            'App-Token': APP_TOKEN,
                            'Session-Token': sessionToken,
                            Accept: '*/*',
                        },
                        maxRedirects: 5,
                        validateStatus: (s) => s >= 200 && s < 400,
                    }
                );
                const ctype = dlResp.headers['content-type'] || '';
                if (looksLikeHtml(ctype)) {
                    return res.status(403).json({
                        error: 'Access denied by GLPI front (document.send.php).',
                        hint:
                            'Enable the REST download endpoint (Document/:id/download) or allow front access for the REST session.',
                    });
                }
            } catch (e) {
                console.error('[doc proxy] Front download failed:', e?.message);
                return res.status(502).json({
                    error: 'Failed to download document via GLPI Front.',
                    detail: e?.message || 'unknown',
                });
            }
        }

        // 5) Stream to client with proper headers
        setDownloadHeaders({
            filename,
            mime,
            length: dlResp.headers['content-length'],
            dispositionFromServer: dlResp.headers['content-disposition'],
            typeFromServer: dlResp.headers['content-type'],
        });

        dlResp.data.pipe(res);
        dlResp.data.on('error', (err) => {
            console.error('[doc proxy] stream error:', err?.message);
            if (!res.headersSent) {
                res.status(502).json({ error: 'Failed to stream file' });
            } else {
                res.end();
            }
        });
    } catch (e) {
        console.error(
            '[doc proxy] ERR:',
            e?.response?.status,
            e?.response?.data || e?.message
        );
        if (!res.headersSent) {
            res.status(502).json({
                error: 'Failed to download document',
                detail: e?.message || 'unknown',
            });
        } else {
            res.end();
        }
    } finally {
        // close REST session (best effort)
        if (sessionToken) {
            try {
                await glpiKillSession(sessionToken);
            } catch (e) {
                console.error('[doc proxy] killSession error:', e?.message);
            }
        }
    }
});

// ---------------------------------------------------------------------------
// ROTAS GLPI USERS (via DB glpi_users)
// ---------------------------------------------------------------------------

app.get('/glpi/users', async (req, res) => {
    try {
        const [rows] = await glpiPool.query(
            `SELECT id, name
         FROM glpi_users
        WHERE is_deleted = 0
          AND name IS NOT NULL
        ORDER BY name ASC`
        );
        res.json(rows);
    } catch (e) {
        console.error('GET /glpi/users error:', e);
        res.status(500).json({ error: e.message || 'Erro ao buscar usuários GLPI' });
    }
});

// ---------------------------------------------------------------------------
// GET /tipo-de-despesa - Listar tipos de despesa
// ---------------------------------------------------------------------------
app.get('/tipo-de-despesa', async (req, res) => {
    try {
        const [rows] = await enelPool.query(
            `SELECT id, tipo
         FROM tipo_de_despesa
        ORDER BY tipo ASC`
        );
        res.json(rows);
    } catch (e) {
        console.error('GET /tipo-de-despesa error:', e);
        res.status(500).json({ error: e.message || 'Erro ao buscar tipos de despesa' });
    }
});

// ---------------------------------------------------------------------------
// GET /operacoes - Listar todas as operações para auto-fill
// ---------------------------------------------------------------------------
app.get('/operacoes', async (req, res) => {
    try {
        const [rows] = await enelPool.query(
            `SELECT 
        id_operacao, 
        tipo_documento, 
        denominacao, 
        centro_custo, 
        pep, 
        responsavel 
       FROM operacao`
        );
        res.json(rows);
    } catch (e) {
        console.error('GET /operacoes error:', e);
        res.status(500).json({ error: e.message || 'Erro ao buscar operacoes' });
    }
});

// ---------------------------------------------------------------------------
// GET /despesas - Listar despesas (opcional: ?user_id=X)
// ---------------------------------------------------------------------------
app.get('/despesas', async (req, res) => {
    try {
        const userId = req.query.user_id ? Number(req.query.user_id) : null;
        console.log('DEBUG: GET /despesas called with user_id:', userId);

        // LEFT JOIN with despesas_arquivos to get file info
        let sql = `
      SELECT 
        d.despesa_id,
        d.user_id,
        d.TECNICO as user_name,
        d.tipo_de_despesa,
        d.VALOR as valor_despesa,
        d.DATA as data_consumo,
        d.SERVICOS as justificativa,
        d.aprovacao,
        d.denominacao,
        d.responsavel,
        d.quem_aprovou,
        d.aprovado_em,
        da.glpi_links as photo_url,
        da.glpi_subtask_id,
        da.photo_docid,
        d.\`TCF/DATA\` as tcf_data,
        DATE_FORMAT(d.\`DATA DE LANÇAMENTO SAP\`, '%Y-%m-%d') as data_lancamento_sap
      FROM despesas d
        LEFT JOIN(
            SELECT 
            da1.despesas_id,
            da1.glpi_links,
            da1.glpi_subtask_id,
            da1.photo_docid,
            da1.user_id
          FROM despesas_arquivos da1
          INNER JOIN(
                SELECT despesas_id, MAX(created_at) as latest_created
            FROM despesas_arquivos
            GROUP BY despesas_id
            ) da2 ON da1.despesas_id = da2.despesas_id 
            AND da1.created_at = da2.latest_created
        ) da ON d.despesa_id = da.despesas_id
            `;
        const params = [];

        if (userId) {
            sql += ' WHERE d.user_id = ?';
            params.push(userId);
        }

        sql += ' ORDER BY d.despesa_id DESC';

        console.log('DEBUG: Executing SQL with params:', params);
        const rows = await queryWithRetry(enelPool, sql, params);
        console.log('DEBUG: Query returned', rows.length, 'rows');
        if (rows.length > 0) {
            console.log('DEBUG: First row:', rows[0]);
        }
        return res.json(rows);
    } catch (e) {
        console.error('GET /despesas error:', e);
        return res.status(500).json({ error: e.message || 'Erro ao listar despesas' });
    }
});

// ---------------------------------------------------------------------------
// POST /despesas - ENEL.tabela despesas
// ---------------------------------------------------------------------------
app.post('/despesas', async (req, res) => {
    try {
        const {
            tecnico,
            user_id,
            data,
            servicos,
            tipo_de_despesa,
            tipo_doc2,
            centro_custo,
            pep,
            valor,
            data_lancamento_sap,
            denominacao,
            responsavel,
        } = req.body || {};

        if (!tecnico ||
            !user_id ||
            !data ||
            !servicos ||
            !tipo_de_despesa ||
            !tipo_doc2 ||
            !centro_custo ||
            !pep ||
            valor === undefined ||
            valor === null ||
            !denominacao ||
            !responsavel) {
            return res.status(400).json({ error: 'Campos obrigatórios faltando!' });
        }

        const valorNum = Number(
            typeof valor === 'string' ? valor.replace(',', '.') : valor
        );
        if (Number.isNaN(valorNum)) {
            return res.status(400).json({ error: 'Valor inválido' });
        }

        // Parse and validate the date
        // FIXED: Frontend sends YYYY-MM-DD, parse directly or use UTC methods to avoid timezone conversion
        // When doing new Date("2025-12-17"), JS treats it as UTC, but getDate() converts to local time
        // causing -1 day offset in UTC-3 timezone
        let dateOnly;

        // Try to parse as YYYY-MM-DD directly first
        const dateMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(data);
        if (dateMatch) {
            // Direct string parsing - no timezone conversion
            dateOnly = data; // Already in correct format
        } else {
            // Fallback: parse as Date and use UTC methods
            const d = new Date(data);
            if (Number.isNaN(d.getTime())) {
                return res.status(400).json({ error: 'Data inválida' });
            }
            // Use UTC methods to avoid local timezone conversion
            const yyyy = d.getUTCFullYear();
            const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
            const dd = String(d.getUTCDate()).padStart(2, '0');
            dateOnly = `${yyyy} -${mm} -${dd} `;
        }

        let sapDate = null;
        if (data_lancamento_sap) {
            // Same fix as above - use direct parsing or UTC methods
            const sapMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(data_lancamento_sap);
            if (sapMatch) {
                sapDate = data_lancamento_sap;
            } else {
                const s = new Date(data_lancamento_sap);
                if (!Number.isNaN(s.getTime())) {
                    const sy = s.getUTCFullYear();
                    const sm = String(s.getUTCMonth() + 1).padStart(2, '0');
                    const sd = String(s.getUTCDate()).padStart(2, '0');
                    sapDate = `${sy} -${sm} -${sd} `;
                }
            }
        }

        const sql = `
      INSERT INTO despesas
            (\`TECNICO\`,
         \`user_id\`,
         \`tipo_de_despesa\`,
         \`DATA\`,
         \`SERVICOS\`,
        \`TIPO DE DOC2\`,
         \`CENTRO DE CUSTO\`,
         \`PEP\`,
         \`VALOR\`,
         \`DATA DE LANÇAMENTO SAP\`,
         \`denominacao\`,
         \`responsavel\`)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

        const params = [
            tecnico,
            user_id,
            tipo_de_despesa,
            dateOnly,
            servicos,
            tipo_doc2,
            centro_custo,
            pep,
            valorNum,
            sapDate,
            denominacao,
            responsavel,
        ];

        const result = await execOnceWithRetry(sql, params);
        return res.status(201).json({ success: true, id: result.insertId });
    } catch (e) {
        console.error('POST /despesas exception:', e);
        return res.status(500).json({ error: e.message || 'Erro interno' });
    }
});

// ---------------------------------------------------------------------------
// POST /glpi/subtask - criação de subtarefa com cache
// ---------------------------------------------------------------------------

app.post('/glpi/subtask', async (req, res) => {
    const { projecttasks_id, name, plan_start_date, plan_end_date } = req.body || {};
    if (!projecttasks_id || !name) {
        return res.status(400).json({ error: 'projecttasks_id and name are required' });
    }

    const cache = await loadSubtaskCache();
    const cacheKey = `${projecttasks_id}::${name}`;

    if (cache[cacheKey]?.id) {
        return res.json({ success: true, reused: true, task: { id: cache[cacheKey].id } });
    }

    let sessionToken;
    try {
        sessionToken = await glpiInitSession();

        const parentTask = await glpiGetProjectTask(sessionToken, projecttasks_id);
        const projectId = parentTask?.projects_id;
        if (!projectId) {
            return res.status(400).json({ error: 'Invalid parent task or missing project ID' });
        }

        const today = new Date().toISOString().split('T')[0];
        const payload = {
            input: {
                projects_id: projectId,
                name,
                projecttasks_id,
                percent_done: 0,
                plan_start_date: plan_start_date || today,
                plan_end_date: plan_end_date || today,
            },
        };

        const response = await axios.post(`${glpiBaseUrl}/ProjectTask`, payload, {
            headers: {
                'App-Token': APP_TOKEN,
                'Session-Token': sessionToken,
                'Content-Type': 'application/json',
            },
        });

        const createdId = response?.data?.id;
        if (!createdId) {
            return res.status(500).json({ error: 'GLPI did not return a task id' });
        }

        cache[cacheKey] = { id: createdId, createdAt: new Date().toISOString() };
        await saveSubtaskCache();

        return res.json({ success: true, reused: false, task: { id: createdId } });
    } catch (err) {
        console.error('Subtask creation failed:', err.response?.data || err.message);
        return res.status(500).json({ error: err.response?.data?.message || 'Failed to create subtask' });
    } finally {
        if (sessionToken) await glpiKillSession(sessionToken);
    }
});

// ---------------------------------------------------------------------------
// POST /upload-documents - upload para GLPI + grava em despesas_arquivos
// ---------------------------------------------------------------------------

app.post('/upload-documents', upload.array('files', 10), async (req, res) => {
    const rawItemtype = req.body.itemtype || 'ProjectTask';
    const itemtype =
        typeof rawItemtype === 'string' ? rawItemtype.trim() : 'ProjectTask';
    const itemsId = Number(req.body.itemsId);
    const despesaId = req.body.despesaId ? Number(req.body.despesaId) : null;

    // Blindagem do partLimit (NUNCA deixar 0 ou NaN)
    let partLimit = MAX_PART_BYTES;
    if (req.body.maxPartBytes !== undefined && req.body.maxPartBytes !== null) {
        const candidate = Number(req.body.maxPartBytes);
        if (!Number.isNaN(candidate) && candidate > 0) {
            partLimit = candidate;
        } else {
            console.warn(
                '[UPLOAD-DOCS] maxPartBytes inválido recebido:',
                req.body.maxPartBytes,
                '-> usando MAX_PART_BYTES padrão =',
                MAX_PART_BYTES
            );
        }
    }

    if (!itemsId || !['ProjectTask', 'Ticket', 'TicketTask'].includes(itemtype)) {
        return res.status(400).json({ error: 'Invalid itemtype or itemsId' });
    }
    if (!req.files || req.files.length === 0) {
        return res.status(400).json({ error: 'At least one file is required' });
    }

    let sessionToken;
    const results = [];
    const failed = [];
    const splitNotes = [];

    try {
        sessionToken = await glpiInitSession();

        for (const file of req.files) {
            const original = file.originalname;
            let uploadedPath = file.path;
            let displayOriginalName = original;

            const isImage = /^image\//i.test(file.mimetype);

            // LOG: tamanho logo após o multer salvar
            try {
                const statBefore = await fsp.stat(uploadedPath);
                console.log(
                    '[UPLOAD-DOCS] received file:',
                    original,
                    'size =',
                    statBefore.size,
                    'bytes'
                );
                if (statBefore.size === 0) {
                    console.error('[UPLOAD-DOCS] skipping empty uploaded file:', original);
                    failed.push({
                        original,
                        part: 0,
                        error: 'Uploaded file is empty (0 bytes)',
                    });
                    try {
                        await fsp.unlink(uploadedPath);
                    } catch (_) { }
                    continue;
                }
            } catch (e) {
                console.error(
                    '[UPLOAD-DOCS] stat before optimize failed for',
                    original,
                    e.message || e
                );
            }

            // Otimização da imagem (se der erro, continua com o original)
            try {
                const optimized = await optimizeImageFile(
                    uploadedPath,
                    original,
                    file.mimetype,
                    partLimit
                );
                uploadedPath = optimized.path;
                displayOriginalName = optimized.filename;

                try {
                    const statAfter = await fsp.stat(uploadedPath);
                    console.log(
                        '[UPLOAD-DOCS] after optimize:',
                        displayOriginalName,
                        'size =',
                        statAfter.size,
                        'bytes'
                    );
                    if (statAfter.size === 0) {
                        console.error(
                            '[UPLOAD-DOCS] optimized file is empty, skipping:',
                            displayOriginalName
                        );
                        failed.push({
                            original,
                            part: 0,
                            error: 'Optimized file is empty (0 bytes)',
                        });
                        try {
                            await fsp.unlink(uploadedPath);
                        } catch (_) { }
                        continue;
                    }
                } catch (e) {
                    console.error(
                        '[UPLOAD-DOCS] stat after optimize failed for',
                        uploadedPath,
                        e.message || e
                    );
                }
            } catch (err) {
                console.warn(
                    `[UPLOAD-DOCS] Skipping optimization for ${original}:`,
                    err.message || err
                );
            }

            // Split only if it’s an image (or better: don't split at all)
            let parts;
            if (isImage) {
                parts = await splitFile(uploadedPath, partLimit);
                if (parts.length > 1) {
                    splitNotes.push(
                        `${displayOriginalName} split into ${parts.length} parts (≤ ${(partLimit / 1024 / 1024).toFixed(0)}MB)`
                    );
                }
            } else {
                // For pdf/doc/txt/... NEVER split
                parts = [uploadedPath];
            }

            // Send parts
            for (let i = 0; i < parts.length; i++) {
                const partPath = parts[i];
                const displayName =
                    parts.length > 1
                        ? `${displayOriginalName}.part${String(i + 1).padStart(2, '0')}`
                        : displayOriginalName;
                try {
                    // LOG: tamanho de cada parte antes de enviar ao GLPI
                    try {
                        const statPart = await fsp.stat(partPath);
                        console.log(
                            '[UPLOAD-DOCS] sending part',
                            i + 1,
                            'of',
                            displayName,
                            'size =',
                            statPart.size,
                            'bytes'
                        );
                        if (statPart.size === 0) {
                            console.error(
                                '[UPLOAD-DOCS] skipping empty part:',
                                displayName,
                                'part',
                                i + 1
                            );
                            failed.push({
                                original,
                                part: i + 1,
                                error: 'Empty file part (0 bytes), not uploaded to GLPI',
                            });
                            continue;
                        }
                    } catch (e) {
                        console.error(
                            '[UPLOAD-DOCS] stat for part failed:',
                            displayName,
                            'part',
                            i + 1,
                            e.message || e
                        );
                    }

                    const docId = await glpiUploadDocument(
                        sessionToken,
                        partPath,
                        displayName
                    );
                    await glpiLinkDocument(sessionToken, {
                        documentId: docId,
                        itemtype,
                        itemsId,
                    });
                    results.push({
                        original,
                        optimizedName: displayOriginalName,
                        part: i + 1,
                        documentId: docId,
                    });
                } catch (err) {
                    console.error(
                        '[UPLOAD-DOCS] upload/link failed for',
                        displayName,
                        'part',
                        i + 1,
                        err.response?.data || err.message || err
                    );
                    failed.push({
                        original,
                        part: i + 1,
                        error: err.message || 'upload/link failed',
                    });
                } finally {
                    // remove part file (se diferente do arquivo otimizado "pai")
                    if (partPath !== uploadedPath) {
                        try {
                            await fsp.unlink(partPath);
                        } catch (_) { }
                    }
                }
            }

            // Remove arquivo otimizado/original ao final
            try {
                await fsp.unlink(uploadedPath);
            } catch (_) { }
        }

        // Atualiza tabela despesas_arquivos se despesaId informado
        let savedUrl = null;
        if (despesaId && results.length) {
            const winner =
                [...results].reverse().find((r) => r.part === 1) ||
                results[results.length - 1];

            savedUrl = await updateDespesaPhotoLink({
                despesaId,
                docId: winner.documentId,
            });
        }

        res.json({
            ok: true,
            count: results.length,
            attachments: results,
            failed,
            notes: splitNotes,
            saved: despesaId
                ? { despesaId, glpi_subtask_id: itemsId, photo_url: savedUrl }
                : null,
        });
    } catch (err) {
        console.error('Upload error:', err.response?.data || err.message || err);
        res.status(500).json({
            error: err.response?.data || err.message || 'Upload failed',
        });
    } finally {
        if (sessionToken) {
            try {
                await glpiKillSession(sessionToken);
            } catch (e) {
                console.error('[UPLOAD-DOCS] killSession error:', e?.message || e);
            }
        }
    }
});

// ---------------------------------------------------------------------------
// DELETE /despesas/:id - Excluir despesa
// ---------------------------------------------------------------------------
app.delete('/despesas/:id', async (req, res) => {
    try {
        const despesaId = Number(req.params.id);
        if (!despesaId) {
            return res.status(400).json({ error: 'Invalid despesa id' });
        }

        // Check if exists
        const [rows] = await queryWithRetry(
            enelPool,
            'SELECT aprovacao FROM despesas WHERE despesa_id = ?',
            [despesaId]
        );
        if (!rows || rows.length === 0) {
            return res.status(404).json({ error: 'Despesa não encontrada' });
        }
        const { aprovacao } = rows[0];

        if (aprovacao === 'Aprovado') {
            return res.status(403).json({ error: 'Não é possível excluir despesa aprovada' });
        }

        // Delete from DB
        await execOnceWithRetry('DELETE FROM despesas WHERE despesa_id = ?', [despesaId]);
        // Also delete from despesas_arquivos if exists
        await execOnceWithRetry('DELETE FROM despesas_arquivos WHERE despesas_id = ?', [despesaId]);

        return res.json({ success: true });
    } catch (e) {
        console.error('DELETE /despesas/:id error:', e);
        return res.status(500).json({ error: e.message || 'Erro ao excluir despesa' });
    }
});

// ---------------------------------------------------------------------------
// DELETE /despesas/:id/file - Remover arquivo da despesa
// ---------------------------------------------------------------------------
app.delete('/despesas/:id/file', async (req, res) => {
    try {
        const despesaId = Number(req.params.id);
        if (!despesaId) {
            return res.status(400).json({ error: 'Invalid despesa id' });
        }

        // Check if exists
        const [rows] = await queryWithRetry(
            enelPool,
            'SELECT aprovacao FROM despesas WHERE despesa_id = ?',
            [despesaId]
        );
        if (!rows || rows.length === 0) {
            return res.status(404).json({ error: 'Despesa não encontrada' });
        }
        const { aprovacao } = rows[0];

        if (aprovacao === 'Aprovado') {
            return res.status(403).json({ error: 'Não é possível alterar despesa aprovada' });
        }

        // Check if there are files in despesas_arquivos
        const [arquivos] = await queryWithRetry(
            enelPool,
            'SELECT id FROM despesas_arquivos WHERE despesas_id = ?',
            [despesaId]
        );
        if (!arquivos || arquivos.length === 0) {
            return res.status(400).json({ error: 'Nenhum arquivo para remover' });
        }
        // Also remove from despesas_arquivos
        await execOnceWithRetry('DELETE FROM despesas_arquivos WHERE despesas_id = ?', [despesaId]);

        return res.json({ success: true });
    } catch (e) {
        console.error('DELETE /despesas/:id/file error:', e);
        return res.status(500).json({ error: e.message || 'Erro ao remover arquivo' });
    }
});

// ---------------------------------------------------------------------------
// PUT /despesas/:id - Atualizar despesa
// ---------------------------------------------------------------------------
app.put('/despesas/:id', async (req, res) => {
    try {
        const despesaId = Number(req.params.id);
        if (!despesaId) {
            return res.status(400).json({ error: 'Invalid despesa id' });
        }

        // Get current row to know approval status
        const rows = await queryWithRetry(
            enelPool,
            'SELECT aprovacao FROM despesas WHERE despesa_id = ?',
            [despesaId]
        );

        if (!rows || rows.length === 0) {
            return res.status(404).json({ error: 'Despesa não encontrada' });
        }

        const currentAprov = rows[0].aprovacao ?? null;

        // Allowed fields
        const {
            tipo_de_despesa,
            valor_despesa,
            valor,
            data,
            servicos,
            quantidade,
            justificativa,
            aprovacao,  // <-- TO JEST NOWY STATUS
            denominacao,
            responsavel,
            tcf_data, // <--- New field
        } = req.body || {};

        // Jeśli próbujemy zmienić status na 'Aprovado' lub 'Reprovado'
        if (aprovacao === 'Aprovado' || aprovacao === 'Reprovado') {
            // Sprawdź czy użytkownik ma uprawnienia (możesz dodać logikę autoryzacji)
            // Na razie pozwalamy na zmianę statusu
            const sets = ['aprovacao = ?', 'aprovado_em = NOW()'];
            const params = [aprovacao];

            const { quem_aprovou } = req.body;
            if (quem_aprovou) {
                sets.push('quem_aprovou = ?');
                params.push(quem_aprovou);
            }

            const sql = `UPDATE despesas SET ${sets.join(', ')} WHERE despesa_id = ?`;
            params.push(despesaId);

            await execOnceWithRetry(sql, params);

            const [updatedRows] = await queryWithRetry(
                enelPool,
                'SELECT * FROM despesas WHERE despesa_id = ?',
                [despesaId]
            );

            return res.json({ success: true, data: updatedRows[0] });
        }

        // Reszta istniejącej logiki (edycja przez technika)
        if (currentAprov === 'Aprovado' && !aprovacao && tcf_data === undefined) {
            return res.status(403).json({ error: 'Registro aprovado não pode ser editado.' });
        }

        const sets = [];
        const params = [];

        if (tipo_de_despesa !== undefined) {
            sets.push('`tipo_de_despesa` = ?');
            params.push(String(tipo_de_despesa));
        }

        // Handle valor
        const val = valor !== undefined ? valor : valor_despesa;
        if (val !== undefined) {
            const valorNum = Number(
                typeof val === 'string' ? val.replace(',', '.') : val
            );
            if (Number.isNaN(valorNum)) {
                return res.status(400).json({ error: 'valor inválido' });
            }
            sets.push('`VALOR` = ?');
            params.push(valorNum);
        }

        // Handle data
        const dt = data !== undefined ? data : req.body.data_consumo;
        if (dt !== undefined) {
            const d = new Date(dt);
            if (Number.isNaN(d.getTime())) {
                return res.status(400).json({ error: 'data inválida' });
            }
            const yyyy = d.getFullYear();
            const mm = String(d.getMonth() + 1).padStart(2, '0');
            const dd = String(d.getDate()).padStart(2, '0');
            sets.push('`DATA` = ?');
            params.push(`${yyyy}-${mm}-${dd}`);
        }

        // Handle servicos (justificativa)
        const just = servicos !== undefined ? servicos : justificativa;
        if (just !== undefined) {
            sets.push('`SERVICOS` = ?');
            params.push(String(just));
        }

        // Handle denominacao
        if (denominacao !== undefined) {
            sets.push('`denominacao` = ?');
            params.push(String(denominacao));
        }

        // Handle responsavel
        if (responsavel !== undefined) {
            sets.push('`responsavel` = ?');
            params.push(String(responsavel));
        }

        // Handle tcf_data
        if (tcf_data !== undefined) {
            sets.push('`TCF/DATA` = ?');
            params.push(String(tcf_data));
        }

        // Handle data_lancamento_sap
        const sap = req.body.data_lancamento_sap;
        if (sap !== undefined) {
            // If null/empty sent, maybe clear it? But usually we expect a date string or null.
            if (!sap) {
                // If strict clearing is needed: sets.push('`DATA DE LANÇAMENTO SAP` = NULL');
                // For now, let's treat empty string as ignore or maybe clear if intended.
                // Assuming logic similar to others: if it's explicitly sent as null, clear it.
                if (sap === null) {
                    sets.push('`DATA DE LANÇAMENTO SAP` = NULL');
                }
            } else {
                let sapDate = null;
                const sapMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(sap);
                if (sapMatch) {
                    sapDate = sap;
                } else {
                    const s = new Date(sap);
                    if (!Number.isNaN(s.getTime())) {
                        const sy = s.getUTCFullYear();
                        const sm = String(s.getUTCMonth() + 1).padStart(2, '0');
                        const sd = String(s.getUTCDate()).padStart(2, '0');
                        sapDate = `${sy}-${sm}-${sd}`;
                    }
                }
                if (sapDate) {
                    sets.push('`DATA DE LANÇAMENTO SAP` = ?');
                    params.push(sapDate);
                }
            }
        }

        if (sets.length === 0) {
            return res.status(400).json({ error: 'Nenhum campo para atualizar' });
        }

        if (currentAprov === 'Reprovado' && aprovacao === undefined) {
            // edição pelo técnico: volta para fila de aprovação
            sets.push('aprovacao = ?');
            params.push('pendente');
        }

        const sql = `UPDATE despesas SET ${sets.join(', ')} WHERE despesa_id = ?`;
        params.push(despesaId);

        await execOnceWithRetry(sql, params);

        const [updatedRows] = await queryWithRetry(
            enelPool,
            'SELECT * FROM despesas WHERE despesa_id = ?',
            [despesaId]
        );

        return res.json({ success: true, data: updatedRows[0] });
    } catch (e) {
        console.error('PUT /despesas/:id exception:', e);
        return res.status(500).json({ error: e.message || 'Erro interno' });
    }
});
app.get('/despesas-arquivos', async (req, res) => {
    try {
        const despesaId = req.query.despesas_id
            ? Number(req.query.despesas_id)
            : null;
        const userId = req.query.user_id ? Number(req.query.user_id) : null;

        let sql = `
      SELECT
        id,
        despesas_id,
        user_id,
        glpi_links,
        created_at,
        updated_at,
        glpi_subtask_id,
        photo_docid
      FROM despesas_arquivos
    `;
        const params = [];

        const where = [];
        if (despesaId) {
            where.push('despesas_id = ?');
            params.push(despesaId);
        }
        if (userId) {
            where.push('user_id = ?');
            params.push(userId);
        }
        if (where.length > 0) {
            sql += ' WHERE ' + where.join(' AND ');
        }

        sql += ' ORDER BY despesas_id ASC, created_at ASC';

        const rows = await queryWithRetry(enelPool, sql, params);
        return res.json(rows);
    } catch (e) {
        console.error('GET /despesas-arquivos error:', e);
        return res
            .status(500)
            .json({ error: e.message || 'Erro ao listar despesas_arquivos' });
    }
});

// ---------------------------------------------------------------------------
// START SERVER
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
    console.log(`Backend ENEL rodando na porta ${PORT}`);
});
