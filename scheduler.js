/**
 * MONEYLIVE 스케줄러 v5.8 (휴장일 처리 + 수급 날짜 버그 수정)
 *
 * [v5.7 → v5.8 변경사항]
 *
 * ① 휴장일 완전 스킵 (isMarketClosed)
 *    - 주말(토/일) + 한국 법정공휴일이면 scheduleAt 내부에서 job 자체를 실행 안 함
 *    - 토큰 발급 포함 모든 수집 차단
 *    - 서버 시작 시 즉시 수집(setTimeout)도 휴장일이면 스킵
 *
 * ② fetchSupply — 전 영업일 날짜 fallback 수정
 *    - 장중 API가 0이고 현재 시각이 09:00 이전이면
 *      일별 API 날짜를 당일이 아닌 마지막 영업일로 조회
 *    - 09:00 이후에도 당일 데이터가 없으면 전 영업일로 fallback
 *
 * ③ buildSupplyResult — 휴장일 history 오염 방지
 *    - 외국인+기관+개인 합산이 0이면 history에 추가하지 않음
 *    - 공휴일/주말 날짜가 프론트 미니차트에 끼어드는 현상 제거
 */

const fs    = require('fs');
const path  = require('path');
const https = require('https');

// ─────────────────────────────────────────────
// 저장 경로
// ─────────────────────────────────────────────
const STORAGE_DIR = path.join(__dirname, 'storage');
const DATA_DIR    = path.join(STORAGE_DIR, 'data');
const LOG_DIR     = path.join(STORAGE_DIR, 'logs');
const RAW_DIR     = path.join(STORAGE_DIR, 'raw');
const DATA_FILE   = path.join(__dirname, 'data.json');
const TOKEN_FILE  = path.join(__dirname, 'token.json');

[STORAGE_DIR, DATA_DIR, LOG_DIR, RAW_DIR].forEach(d => {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
});

const APP_KEY    = process.env.KIS_APP_KEY;
const APP_SECRET = process.env.KIS_APP_SECRET;

// ─────────────────────────────────────────────
// [v5.8 NEW] 한국 법정공휴일 + 휴장일 판단
// ─────────────────────────────────────────────
// YYYYMMDD 형식. 매년 연말에 다음 해 날짜를 추가하세요.
const KR_HOLIDAYS = new Set([
  // 2025년
  '20250101','20250128','20250129','20250130',
  '20250301','20250505','20250506','20250606',
  '20250815','20251003','20251005','20251006',
  '20251007','20251009','20251225',
  // 2026년
  '20260101','20260216','20260217','20260218',
  '20260301','20260505','20260525','20260606',
  '20260815','20260924','20260925','20260926',
  '20261009','20261225',
  // 2027년
  '20270101','20270205','20270206','20270207',
  '20270301','20270505','20270513','20270606',
  '20270816','20271015','20271016','20271017',
  '20271011','20271225',
]);

function isMarketClosed(d) {
  const dow = d.getDay(); // 0=일, 6=토
  if (dow === 0 || dow === 6) return true;
  const ymd = `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}${String(d.getDate()).padStart(2,'0')}`;
  return KR_HOLIDAYS.has(ymd);
}

// 기준 날짜에서 N 영업일 전 날짜를 YYYYMMDD 형식으로 반환
function getPrevTradingDate(offset = 1) {
  const d = kstNow();
  let count = 0;
  while (count < offset) {
    d.setDate(d.getDate() - 1);
    if (!isMarketClosed(d)) count++;
  }
  return `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}${String(d.getDate()).padStart(2,'0')}`;
}

// ─────────────────────────────────────────────
// 유틸
// ─────────────────────────────────────────────
let logBuffer = [];

function log(msg) {
  const now = new Date().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' });
  const line = `[${now}] ${msg}`;
  console.log(line);
  logBuffer.push(line);
}

function flushLog() {
  if (!logBuffer.length) return;
  const today = getDateStr(0);
  const logFile = path.join(LOG_DIR, `${today}.log`);
  fs.appendFileSync(logFile, logBuffer.join('\n') + '\n');
  logBuffer = [];
}

function kstNow() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
}
function getDateStr(offset = 0) {
  const d = kstNow();
  d.setDate(d.getDate() + offset);
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
function getDate(offset = 0) { return getDateStr(offset).replace(/-/g, ''); }
function pad(n) { return String(n).padStart(2, '0'); }

function getTimeStr() {
  const n = kstNow();
  return `${pad(n.getMonth()+1)}/${pad(n.getDate())} ${pad(n.getHours())}:${pad(n.getMinutes())}:${pad(n.getSeconds())}`;
}

function loadPrevData() {
  try {
    return fs.existsSync(DATA_FILE) ? JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) : {};
  } catch { return {}; }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  fs.writeFileSync(path.join(DATA_DIR, `${getDateStr(0)}.json`), JSON.stringify(data, null, 2));
  flushLog();
}

// ─────────────────────────────────────────────
// RAW 데이터 저장 함수
// ─────────────────────────────────────────────
function saveRaw(label, data) {
  try {
    const today = getDateStr(0);
    const dir   = path.join(RAW_DIR, today);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const n   = kstNow();
    const hms = `${pad(n.getHours())}-${pad(n.getMinutes())}-${pad(n.getSeconds())}`;
    const file = path.join(dir, `${hms}_${label}.json`);
    fs.writeFileSync(file, JSON.stringify(data, null, 2));
    log(`  💾 RAW 저장: storage/raw/${today}/${hms}_${label}.json`);
  } catch (e) {
    log(`  ⚠️ RAW 저장 실패 (${label}): ${e.message}`);
  }
}

// ─────────────────────────────────────────────
// HTTP (KIS 전용)
// ─────────────────────────────────────────────
function httpPost(hostname, reqPath, body) {
  return new Promise((resolve, reject) => {
    const bodyStr = JSON.stringify(body);
    const req = https.request({
      hostname, port: 9443, path: reqPath, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('JSON 파싱실패')); } });
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}

function kisGet(apiPath, token, trId, params = {}) {
  const qs = Object.entries(params).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join('&');
  return new Promise((resolve, reject) => {
    https.get({
      hostname: 'openapi.koreainvestment.com', port: 9443,
      path: qs ? `${apiPath}?${qs}` : apiPath,
      headers: {
        'content-type': 'application/json; charset=utf-8',
        'authorization': `Bearer ${token}`,
        'appkey': APP_KEY,
        'appsecret': APP_SECRET,
        'tr_id': trId,
        'custtype': 'P'
      }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) {
          console.error(`[kisGet 파싱실패] TR:${trId} 원본응답:`, data.substring(0, 300));
          reject(new Error('JSON 파싱실패'));
        }
      });
    }).on('error', reject);
  });
}

async function getToken() {
  try {
    if (fs.existsSync(TOKEN_FILE)) {
      const t = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
      if (Date.now() < new Date(t.expires_at).getTime() - 30*60*1000) return t.access_token;
    }
  } catch {}

  if (!APP_KEY || !APP_SECRET) { log('❌ 환경변수 KIS_APP_KEY/SECRET 없음!'); return null; }
  log('🔑 토큰 발급 시작...');
  try {
    const res = await httpPost('openapi.koreainvestment.com', '/oauth2/tokenP', {
      grant_type: 'client_credentials', appkey: APP_KEY, appsecret: APP_SECRET
    });
    if (res.access_token) {
      fs.writeFileSync(TOKEN_FILE, JSON.stringify({
        access_token: res.access_token,
        expires_at: res.access_token_token_expired,
        issued_at: new Date().toISOString()
      }));
      log('✅ 토큰 발급 완료');
      return res.access_token;
    }
  } catch(e) { log('❌ 토큰 오류: ' + e.message); }
  return null;
}

// ─────────────────────────────────────────────
// Yahoo Finance
// ─────────────────────────────────────────────
function fetchYahoo(symbol) {
  return new Promise((resolve) => {
    https.get({
      hostname: 'query1.finance.yahoo.com',
      path: `/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=14d`,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const result = JSON.parse(data)?.chart?.result?.[0];
          if (!result || !result.indicators?.quote?.[0]) return resolve(null);

          const closes     = result.indicators.quote[0].close;
          const timestamps = result.timestamp;
          const pairs      = timestamps.map((ts,i) => ({ts, c:closes[i]})).filter(p => p.c!=null && !isNaN(p.c));
          if (!pairs.length) return resolve(null);

          // [v5.8] 주말/공휴일 필터링: 거래일 데이터만 남김
          const tradingPairs = pairs.filter(p => {
            const d = new Date(p.ts * 1000);
            const kst = new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
            return !isMarketClosed(kst);
          });
          if (!tradingPairs.length) return resolve(null);

          const last   = tradingPairs[tradingPairs.length - 1];
          const prev   = tradingPairs.length > 1 ? tradingPairs[tradingPairs.length - 2] : null;
          const change = prev ? ((last.c - prev.c) / prev.c * 100) : 0;
          const toKst  = (ts) => {
            const d   = new Date(ts*1000);
            const kst = new Date(d.toLocaleString('en-US', {timeZone:'Asia/Seoul'}));
            return `${kst.getFullYear()}-${String(kst.getMonth()+1).padStart(2,'0')}-${String(kst.getDate()).padStart(2,'0')}`;
          };

          resolve({
            value:   last.c.toFixed(2),
            change:  parseFloat(change.toFixed(2)),
            rate:    parseFloat(change.toFixed(2)),
            history: tradingPairs.map((p,i) => ({
              date:  toKst(p.ts),
              close: p.c,
              change: i > 0 ? parseFloat(((p.c-tradingPairs[i-1].c)/tradingPairs[i-1].c*100).toFixed(2)) : 0
            }))
          });
        } catch { resolve(null); }
      });
    }).on('error', () => resolve(null));
  });
}

async function fetchCrypto(prev, times) {
  try {
    const res = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true');
    if (!res.ok) throw new Error(res.status);
    const data = await res.json();
    times.crypto = getTimeStr();
    return data;
  } catch (e) { return prev || null; }
}

// ─────────────────────────────────────────────
// Yahoo 수집 모듈
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchIndex(prev, times) {
  log('📊 지수 수집 중 (Yahoo)...');
  const kospi  = await fetchYahoo('^KS11'); await sleep(300);
  const kosdaq = await fetchYahoo('^KQ11');
  if (kospi || kosdaq) times.krIndex = getTimeStr();
  return { kospi: kospi || prev?.kospi, kosdaq: kosdaq || prev?.kosdaq };
}

async function fetchKrEtf(prev, times) {
  log('🇰🇷 한국 ETF 수집 중 (Yahoo)...');
  const symbols = {
    kodex200: '069500.KS', kodexkosdaq: '229200.KS', kodexlev: '122630.KS',
    kodexinv: '114800.KS', kodexinv2: '251340.KS', kodexsp: '379800.KS'
  };
  const etf = { ...(prev?.etf||{}) };
  let isUpdated = false;
  for (const [key, sym] of Object.entries(symbols)) {
    const data = await fetchYahoo(sym);
    if (data) { etf[key] = data; isUpdated = true; }
    await sleep(300);
  }
  if (isUpdated) times.krEtf = getTimeStr();
  return etf;
}

async function fetchUsData(prev, times) {
  log('🇺🇸 미국 데이터 수집 중 (Yahoo)...');
  const symbols = {
    sp500:'^GSPC', nasdaq:'^IXIC', dow:'^DJI', vix:'^VIX', tnx:'^TNX', dxy:'DX-Y.NYB',
    spy:'SPY', qqq:'QQQ', tqqq:'TQQQ', sqqq:'SQQQ',
    aapl:'AAPL', msft:'MSFT', nvda:'NVDA', amzn:'AMZN', googl:'GOOGL', meta:'META', tsla:'TSLA',
    ewy:'EWY', koru:'KORU', korz:'KORZ',
    wti:'CL=F', gold:'GC=F', copper:'HG=F',
    xlk:'XLK', xlc:'XLC', xly:'XLY', xlf:'XLF', xlv:'XLV', xli:'XLI',
    xlp:'XLP', xlre:'XLRE', xlu:'XLU', xlb:'XLB', xle:'XLE'
  };
  const results = {};
  let isUpdated = false;
  for (const [key, sym] of Object.entries(symbols)) {
    const data = await fetchYahoo(sym);
    if (data) { results[key] = data; isUpdated = true; }
    await sleep(300);
  }
  if (isUpdated) times.usData = getTimeStr();
  return results;
}

// ─────────────────────────────────────────────
// KIS 수집 모듈
// ─────────────────────────────────────────────

// ── 수급 동향 ─────────────────────────────────
async function fetchSupply(token, prev, times) {
  log('💰 수급 동향 수집 중 (KIS)...');
  try {
    // 1차: 장중 시세성 API
    const params1 = {
      FID_INPUT_ISCD:   'KSP',
      FID_INPUT_ISCD_2: '0001'
    };
    let r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market',
      token, 'FHPTJ04030000', params1
    );
    saveRaw('supply_intraday_FHPTJ04030000', { params: params1, response: r });

    let o = Array.isArray(r?.output) ? r.output[0] : r?.output;
    const allZero = !o || (
      parseInt(o.frgn_ntby_tr_pbmn||0) === 0 &&
      parseInt(o.orgn_ntby_tr_pbmn||0) === 0 &&
      parseInt(o.prsn_ntby_tr_pbmn||0) === 0
    );

    if (!r || r.rt_cd !== '0' || allZero) {
      log(`  ℹ️ 장중 시세성 API 값 없음 (rt_cd:${r?.rt_cd}, 전부0:${allZero}) → 일별 API 시도`);

      // [v5.8] 장 시작 전(09:00 이전)이면 전 영업일 날짜로 조회
      const now = kstNow();
      const isBeforeOpen = now.getHours() < 9;
      const targetDate = isBeforeOpen ? getPrevTradingDate(1) : getDate(0);
      log(`  ℹ️ 일별 API 조회 날짜: ${targetDate} (${isBeforeOpen ? '장 전 → 전 영업일' : '장 중/후 → 당일'})`);

      const params2 = {
        FID_COND_MRKT_DIV_CODE: 'U',
        FID_INPUT_ISCD:         '0001',
        FID_INPUT_DATE_1:       targetDate,
        FID_INPUT_ISCD_1:       'KSP',
        FID_INPUT_DATE_2:       targetDate,
        FID_INPUT_ISCD_2:       '0001'
      };
      r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market',
        token, 'FHPTJ04040000', params2
      );
      saveRaw('supply_daily_FHPTJ04040000', { params: params2, response: r });

      if (!r || r.rt_cd !== '0' || !r.output?.length) {
        log(`  ⚠️ 일별 API도 실패 (rt_cd:${r?.rt_cd}, msg:${r?.msg1}) → 이전 데이터 유지`);
        return prev?.supply || null;
      }

      o = r.output[0];
      const ff = Math.round(parseInt(o.frgn_ntby_tr_pbmn || 0) / 100);
      const fo = Math.round(parseInt(o.orgn_ntby_tr_pbmn  || 0) / 100);
      const fi = Math.round(parseInt(o.prsn_ntby_tr_pbmn  || 0) / 100);
      log(`  ✅ 일별 API 성공 (날짜: ${o.stck_bsop_date}, 외국인: ${ff}억, 기관: ${fo}억, 개인: ${fi}억)`);
      times.krSupply = getTimeStr();
      return buildSupplyResult({ foreign: ff, individual: fi, institution: fo }, prev, o.stck_bsop_date);
    }

    // 1차 성공: 장중 시세성 API
    const f    = Math.round(parseInt(o.frgn_ntby_tr_pbmn || 0) / 100);
    const ind  = Math.round(parseInt(o.prsn_ntby_tr_pbmn || 0) / 100);
    const inst = Math.round(parseInt(o.orgn_ntby_tr_pbmn || 0) / 100);
    log(`  ✅ 장중 시세성 API 성공 (외국인: ${f}억, 기관: ${inst}억, 개인: ${ind}억)`);
    times.krSupply = getTimeStr();
    return buildSupplyResult({ foreign: f, individual: ind, institution: inst }, prev, null);

  } catch(e) {
    log(`  ⚠️ 수급 실패 → 이전 유지: ${e.message}`);
    return prev?.supply || null;
  }
}

// [v5.8] dataDate: 일별 API 사용 시 실제 거래일 날짜(YYYYMMDD), 없으면 오늘
function buildSupplyResult(newVals, prev, dataDate) {
  const { foreign, individual, institution } = newVals;
  const total = foreign + individual + institution;

  // [v5.8] 합산이 0이면 휴장일 데이터 → history에 추가하지 않음
  if (foreign === 0 && individual === 0 && institution === 0) {
    log('  ℹ️ 수급 합산 0 → history 미추가 (휴장일 데이터)');
    return { foreign, individual, institution, history: prev?.supply?.history || [] };
  }

  // 실제 데이터 날짜 결정: 일별 API 응답의 stck_bsop_date 우선, 없으면 오늘
  let recordDate;
  if (dataDate && dataDate.length === 8) {
    // YYYYMMDD → YYYY-MM-DD
    recordDate = `${dataDate.slice(0,4)}-${dataDate.slice(4,6)}-${dataDate.slice(6,8)}`;
  } else {
    recordDate = getDateStr(0);
  }

  const prevHistory = prev?.supply?.history || [];
  const history = [...prevHistory.filter(h => h.date !== recordDate), { date: recordDate, total }]
    .sort((a, b) => a.date.localeCompare(b.date))
    .slice(-5);
  return { foreign, individual, institution, history };
}

// ── 마켓 뎁스 ─────────────────────────────────
async function fetchDepth(token, prev, times) {
  log('📉 마켓 뎁스 수집 중 (KIS)...');
  try {
    const params = {
      FID_COND_MRKT_DIV_CODE: 'U',
      FID_INPUT_ISCD:         '0001',
      FID_INPUT_DATE_1:       getDate(0),
      FID_PERIOD_DIV_CODE:    'D'
    };
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-index-daily-price',
      token, 'FHPUP02120000', params
    );
    saveRaw('depth_FHPUP02120000', { params, response: r });

    if (!r?.output1 || r.rt_cd !== '0') {
      log(`  ⚠️ 마켓뎁스 output1 없음 (rt_cd:${r?.rt_cd}, msg:${r?.msg1})`);
      throw new Error('output1 없음');
    }

    const o   = r.output1;
    const up  = parseInt(o.ascn_issu_cnt || 0);
    const neu = parseInt(o.stnr_issu_cnt || 0);
    const dn  = parseInt(o.down_issu_cnt || 0);

    log(`  ✅ 마켓뎁스 성공 (상승:${up} 보합:${neu} 하락:${dn})`);
    times.krDepth = getTimeStr();
    return { up, neu, dn };
  } catch(e) {
    log(`  ⚠️ 마켓뎁스 실패 → 이전 유지: ${e.message}`);
    return prev?.depth || null;
  }
}

// ─────────────────────────────────────────────
// KR 섹터 (대장주 기반)
// ─────────────────────────────────────────────
async function fetchKrSectors(token, prev, times) {
  log('📊 KR 섹터 수집 중 (KIS)...');

  const sectors = [
    { code: '005930', name: '반도체/IT',    reps: '대표종목: 삼성전자, SK하이닉스, 한미반도체 등' },
    { code: '373220', name: '2차전지',      reps: '대표종목: LG에너지솔루션, 에코프로, 포스코퓨처엠 등' },
    { code: '207940', name: '제약/바이오',  reps: '대표종목: 삼성바이오로직스, 셀트리온, 알테오젠 등' },
    { code: '105560', name: '금융업',       reps: '대표종목: KB금융, 신한지주, 메리츠금융지주 등' },
    { code: '005380', name: '자동차',       reps: '대표종목: 현대차, 기아, 현대모비스 등' },
    { code: '051910', name: '석유/화학',    reps: '대표종목: LG화학, 금호석유, S-Oil 등' },
    { code: '005490', name: '철강/금속',    reps: '대표종목: POSCO홀딩스, 고려아연, 현대제철 등' },
    { code: '000720', name: '건설업',       reps: '대표종목: 현대건설, GS건설, HDC현대산업개발 등' },
    { code: '097950', name: '음식료품',     reps: '대표종목: CJ제일제당, 삼양식품, 농심 등' },
    { code: '282330', name: '유통업',       reps: '대표종목: BGF리테일, 이마트, 신세계 등' },
    { code: '035420', name: '인터넷/게임',  reps: '대표종목: NAVER, 카카오, 크래프톤 등' }
  ];

  const results = [];
  let isUpdated = false;

  for (const s of sectors) {
    try {
      const r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-price',
        token, 'FHKST01010100',
        { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: s.code }
      );
      if (r?.output?.stck_prpr) {
        results.push({
          name: s.name,
          chg:  parseFloat(r.output.prdy_ctrt || 0),
          vol:  Math.round(parseInt(r.output.acml_tr_pbmn || 0) / 100000000),
          reps: s.reps
        });
        isUpdated = true;
      }
    } catch {}
    await sleep(400);
  }

  if (isUpdated) times.krSector = getTimeStr();
  if (results.length > 0) {
    results.sort((a, b) => b.chg - a.chg);
    const allZero = results.every(r => r.chg === 0);
    if (allZero && prev?.sectors?.length) {
      const prevAllZero = prev.sectors.every(r => r.chg === 0);
      if (!prevAllZero) {
        log('  ℹ️ 섹터 전부 0.00% → 이전 유효 데이터 유지');
        return prev.sectors;
      }
    }
    return results;
  }
  return prev?.sectors || null;
}

// ─────────────────────────────────────────────
// KR 종목 순위
// ─────────────────────────────────────────────
async function fetchKrStocks(token, prev, times) {
  log('📋 KR 순위 종목 수집 중 (KIS)...');
  const prevStocks = prev?.stocks || {};
  let isUpdated = false;

  try {
    const volRes = await kisGet(
      '/uapi/domestic-stock/v1/quotations/volume-rank',
      token, 'FHPST01710000',
      {
        FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20171',
        FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0', FID_BLNG_CLS_CODE: '3',
        FID_TRGT_CLS_CODE: '111111111', FID_TRGT_EXLS_CLS_CODE: '000000',
        FID_INPUT_PRICE_1: '', FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: ''
      }
    );

    const volumeRaw = volRes?.output || [];
    const volume = volumeRaw
      .filter(s => !/KODEX|TIGER|KBSTAR|ACE|ARIRANG|HANARO|KOSEF|PLUS|RISE|KCGI|ETN|선물|인버스|레버리지|액티브|채권|리츠/i.test(s.hts_kor_isnm))
      .filter(s => !/^[0-9]{4}[A-Z]/.test(s.mksc_shrn_iscd))
      .sort((a, b) => parseInt(b.acml_tr_pbmn||0) - parseInt(a.acml_tr_pbmn||0))
      .slice(0, 10)
      .map(s => ({
        name: s.hts_kor_isnm,
        code: s.mksc_shrn_iscd,
        chg:  parseFloat(s.prdy_ctrt || 0),
        amt:  Math.round(parseInt(s.acml_tr_pbmn || 0) / 100000000)
      }));

    if (volume.length > 0) isUpdated = true;
    await sleep(800);

    const AMT_FIELD = { 1: 'frgn_ntby_tr_pbmn', 2: 'orgn_ntby_tr_pbmn' };

    const fetchRank = async (etcCls, rankSort) => {
      try {
        const r = await kisGet(
          '/uapi/domestic-stock/v1/quotations/foreign-institution-total',
          token, 'FHPTJ04400000',
          {
            FID_COND_MRKT_DIV_CODE: 'V', FID_COND_SCR_DIV_CODE: '16449',
            FID_INPUT_ISCD: '0001', FID_DIV_CLS_CODE: '0',
            FID_RANK_SORT_CLS_CODE: String(rankSort),
            FID_ETC_CLS_CODE: String(etcCls)
          }
        );
        return (r?.output||[]).slice(0, 5).map(s => {
          let rawAmt;
          if (etcCls === 3) {
            const frgn = parseInt(s.frgn_ntby_tr_pbmn || 0);
            const orgn = parseInt(s.orgn_ntby_tr_pbmn || 0);
            const absAmt = Math.abs(frgn + orgn);
            rawAmt = rankSort === 0 ? absAmt : -absAmt;
          } else {
            rawAmt = parseInt(s[AMT_FIELD[etcCls]] || 0);
          }
          return {
            name: s.hts_kor_isnm,
            code: s.mksc_shrn_iscd,
            chg:  parseFloat(s.prdy_ctrt || 0),
            amt:  Math.round(rawAmt / 100)
          };
        });
      } catch(e) {
        log(`  [fetchRank] 오류: etcCls=${etcCls} sort=${rankSort} ${e.message}`);
        return null;
      }
    };

    const fB = await fetchRank(1, 0); await sleep(800);
    const fS = await fetchRank(1, 1); await sleep(800);
    const iB = await fetchRank(2, 0); await sleep(800);
    const iS = await fetchRank(2, 1); await sleep(800);

    if (isUpdated) times.krStock = getTimeStr();
    return {
      volume:      volume.length      ? volume      : prevStocks.volume,
      foreignBuy:  fB?.length         ? fB          : prevStocks.foreignBuy,
      foreignSell: fS?.length         ? fS          : prevStocks.foreignSell,
      instBuy:     iB?.length         ? iB          : prevStocks.instBuy,
      instSell:    iS?.length         ? iS          : prevStocks.instSell
    };
  } catch(e) {
    log(`  ⚠️ KR종목 실패 → 이전 유지: ${e.message}`);
    return prevStocks || null;
  }
}

// ─────────────────────────────────────────────
// 전체 수집 실행
// ─────────────────────────────────────────────
async function collect() {
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  log('📡 데이터 수집 시작');

  const systemStatus = { kis: '확인 중...', kr: '확인 중...', us: '확인 중...', crypto: '확인 중...' };

  const token = await getToken();
  if (!token) { systemStatus.kis = 'ERROR'; log('❌ 토큰 없음 - KIS 수집 중단'); }
  else { systemStatus.kis = 'OK'; }

  const prev        = loadPrevData();
  const updateTimes = prev.updateTimes || {
    krIndex: '-', krSupply: '-', krProgram: '-', krDepth: '-',
    krEtf: '-', krSector: '-', krStock: '-', usData: '-', crypto: '-'
  };

  // 1. Yahoo & CoinGecko
  const krIndex = await fetchIndex(prev, updateTimes);      await sleep(1000);
  const etf     = await fetchKrEtf(prev, updateTimes);      await sleep(1000);
  const us      = await fetchUsData(prev, updateTimes);      await sleep(1000);
  const crypto  = await fetchCrypto(prev.crypto, updateTimes); await sleep(1000);

  // 2. KIS
  let supply, depth, sectors, stocks;
  if (token) {
    supply   = await fetchSupply(token, prev, updateTimes);   await sleep(1000);
    depth    = await fetchDepth(token, prev, updateTimes);    await sleep(1000);
    sectors  = await fetchKrSectors(token, prev, updateTimes); await sleep(1000);
    stocks   = await fetchKrStocks(token, prev, updateTimes);
  }

  systemStatus.kr     = (krIndex.kospi && krIndex.kosdaq) ? 'OK' : 'ERROR';
  systemStatus.us     = Object.keys(us).length > 0         ? 'OK' : 'ERROR';
  systemStatus.crypto = crypto                              ? 'OK' : 'ERROR';

  const pick      = (newVal, prevVal) => newVal ? { value: newVal.value, change: newVal.change, rate: newVal.rate, history: newVal.history } : (prevVal||null);
  const prevUs    = prev.us || {};
  const prevM7    = prev.m7 || {};
  const prevKrEtf = prev.koreaEtf || {};
  const prevCom   = prev.commodities || {};

  const finalData = {
    time:         kstNow().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }),
    updateTimes,
    systemStatus,

    kospi:   krIndex.kospi,
    kosdaq:  krIndex.kosdaq,

    supply:  supply  || prev.supply,
    depth:   depth   || prev.depth,
    sectors: sectors || prev.sectors,
    stocks:  stocks  || prev.stocks,

    etf,
    crypto: crypto || prev.crypto,

    us: {
      sp500:  pick(us.sp500,  prevUs.sp500),  nasdaq: pick(us.nasdaq, prevUs.nasdaq),
      dow:    pick(us.dow,    prevUs.dow),    vix:    pick(us.vix,    prevUs.vix),
      tnx:    pick(us.tnx,    prevUs.tnx),    dxy:    pick(us.dxy,    prevUs.dxy),
      spy:    pick(us.spy,    prevUs.spy),    qqq:    pick(us.qqq,    prevUs.qqq),
      tqqq:   pick(us.tqqq,   prevUs.tqqq),   sqqq:   pick(us.sqqq,   prevUs.sqqq)
    },

    m7: {
      aapl:  { name: 'Apple',     ...pick(us.aapl,  prevM7.aapl)  },
      msft:  { name: 'Microsoft', ...pick(us.msft,  prevM7.msft)  },
      nvda:  { name: 'Nvidia',    ...pick(us.nvda,  prevM7.nvda)  },
      amzn:  { name: 'Amazon',    ...pick(us.amzn,  prevM7.amzn)  },
      googl: { name: 'Alphabet',  ...pick(us.googl, prevM7.googl) },
      meta:  { name: 'Meta',      ...pick(us.meta,  prevM7.meta)  },
      tsla:  { name: 'Tesla',     ...pick(us.tsla,  prevM7.tsla)  }
    },

    koreaEtf: {
      ewy:  pick(us.ewy,  prevKrEtf.ewy),
      koru: pick(us.koru, prevKrEtf.koru),
      korz: pick(us.korz, prevKrEtf.korz)
    },

    commodities: {
      wti:    pick(us.wti,    prevCom.wti),
      gold:   pick(us.gold,   prevCom.gold),
      copper: pick(us.copper, prevCom.copper)
    },

    usSectors: [
      { name: 'IT',       chg: us.xlk?.rate  || 0 },
      { name: '커뮤니케이션',  chg: us.xlc?.rate  || 0 },
      { name: '임의소비재',   chg: us.xly?.rate  || 0 },
      { name: '금융',      chg: us.xlf?.rate  || 0 },
      { name: '헬스케어',    chg: us.xlv?.rate  || 0 },
      { name: '산업재',     chg: us.xli?.rate  || 0 },
      { name: '필수소비재',   chg: us.xlp?.rate  || 0 },
      { name: '부동산',     chg: us.xlre?.rate || 0 },
      { name: '유틸리티',    chg: us.xlu?.rate  || 0 },
      { name: '소재',      chg: us.xlb?.rate  || 0 },
      { name: '에너지',     chg: us.xle?.rate  || 0 }
    ].sort((a, b) => b.chg - a.chg)
  };

  saveData(finalData);

  if (supply) {
    fs.writeFileSync(path.join(DATA_DIR, 'supply.json'), JSON.stringify(supply, null, 2));
  }

  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
}

// ─────────────────────────────────────────────
// 스케줄러
// ─────────────────────────────────────────────
function scheduleAt(hour, minute, job) {
  let lastRunDay = -1;
  setInterval(() => {
    const now   = kstNow();
    const today = now.getFullYear()*10000 + now.getMonth()*100 + now.getDate();
    if (now.getHours()===hour && now.getMinutes()===minute && lastRunDay!==today) {
      lastRunDay = today;
      // [v5.8] 휴장일이면 job 실행 안 함
      if (isMarketClosed(now)) {
        log(`🗓️ 휴장일 (${today}) — 수집 스킵`);
        flushLog();
        return;
      }
      job();
    }
  }, 30000);
  log(`🕐 스케줄 등록: KST ${pad(hour)}:${pad(minute)}`);
}

log('🚀 MONEYLIVE 스케줄러 v5.8 시작');
log('📁 저장경로: storage/data/ + storage/logs/ + storage/raw/');
log('스케줄: 15:00 토큰 / 15:35 / 18:05 / 06:05 / 07:55');
log('🗓️ 주말 + 법정공휴일 자동 스킵');

scheduleAt(15,  0, async () => { log('[15:00] 토큰 발급'); await getToken(); flushLog(); });
scheduleAt(15, 35, () => { log('[15:35] 1차 수집'); collect(); });
scheduleAt(18,  5, () => { log('[18:05] 2차 수집'); collect(); });
scheduleAt( 6,  5, () => { log('[06:05] 3차 수집'); collect(); });
scheduleAt( 7, 55, () => { log('[07:55] 4차 수집 (최종)'); collect(); });

// 서버 시작 시 즉시 수집 (휴장일이면 스킵)
log('📂 시작 시 즉시 수집 (3초 후)');
setTimeout(() => {
  if (isMarketClosed(kstNow())) {
    log('🗓️ 서버 시작: 휴장일 — 즉시 수집 스킵');
    flushLog();
    return;
  }
  collect();
}, 3000);
