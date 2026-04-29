/**
 * MONEYLIVE 스케줄러 v5.7 (RAW 로그 저장 + 버그 수정)
 * 
 * [v5.6 → v5.7 변경사항]
 * 
 * ① RAW 데이터 저장
 *    - 모든 KIS API 응답을 storage/raw/YYYY-MM-DD/ 폴더에 원본 그대로 저장
 *    - 파일명: HH-MM-SS_{TR_ID}.json
 *    - 수급, 프로그램매매, 마켓뎁스 각각 별도 파일로 저장
 *    → 어떤 값이 실제로 내려오는지 직접 눈으로 확인 가능
 * 
 * ② fetchSupply 수정
 *    - [버그] 1차 API 파라미터 오류: fid_input_iscd → FID_INPUT_ISCD (대소문자)
 *    - [버그] fid_input_iscd_2 파라미터 누락 → "0001" 추가
 *    - [버그] 시세성 API가 0 반환 시 일별 API 폴백을 엉뚱한 TR(FHKST01010900)로 호출
 *             → 올바른 일별 TR: FHPTJ04040000 / URL: inquire-investor-daily-by-market
 *    - 일별 API 파라미터 수정 (문서 기준)
 * 
 * ③ fetchProgram 수정
 *    - [버그] 구버전 TR_ID FHPPG04600001 사용 → 신버전 FHPPG04600101 로 변경
 *    - [버그] URL comp-program-trade-daily → comp-program-trade-today 로 변경
 *    - [버그] 응답키 output → output1 로 변경 (API 문서 기준)
 *    - [버그] 응답 필드명 오류: arbt_buy_amt/nabt_buy_amt → 실제 필드명으로 수정
 * 
 * ④ fetchDepth 수정
 *    - [...r.output2].reverse()[r.output2.length-1] → 로직 오류, slice(-1)[0] 로 수정
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
const RAW_DIR     = path.join(STORAGE_DIR, 'raw');   // ← NEW: RAW 원본 저장
const DATA_FILE   = path.join(__dirname, 'data.json');
const TOKEN_FILE  = path.join(__dirname, 'token.json');

[STORAGE_DIR, DATA_DIR, LOG_DIR, RAW_DIR].forEach(d => {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
});

const APP_KEY    = process.env.KIS_APP_KEY;
const APP_SECRET = process.env.KIS_APP_SECRET;

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
// [NEW] RAW 데이터 저장 함수
// 모든 KIS API 응답을 원본 그대로 파일로 저장
// storage/raw/YYYY-MM-DD/HH-MM-SS_{label}.json
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
      path: `/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=7d`,
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

          const last   = pairs[pairs.length-1];
          const prev   = pairs.length > 1 ? pairs[pairs.length-2] : null;
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
            history: pairs.map((p,i) => ({
              date:  toKst(p.ts),
              close: p.c,
              change: i > 0 ? parseFloat(((p.c-pairs[i-1].c)/pairs[i-1].c*100).toFixed(2)) : 0
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
    // ─── 1차: 장중 시세성 API (FHPTJ04030000)
    // 파라미터명 대소문자 수정: FID_INPUT_ISCD, FID_INPUT_ISCD_2 추가
    const params1 = {
      FID_INPUT_ISCD:   'KSP',   // 코스피
      FID_INPUT_ISCD_2: '0001'   // 종합 (필수 파라미터 — 이전 버전 누락)
    };
    let r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market',
      token, 'FHPTJ04030000', params1
    );

    // [RAW 저장] 1차 응답 원본
    saveRaw('supply_intraday_FHPTJ04030000', { params: params1, response: r });

    // output 배열 첫 번째 항목 추출 (시세성은 output 배열)
    let o = Array.isArray(r?.output) ? r.output[0] : r?.output;

    const allZero = !o || (
      parseInt(o.frgn_ntby_tr_pbmn||0) === 0 &&
      parseInt(o.orgn_ntby_tr_pbmn||0) === 0 &&
      parseInt(o.prsn_ntby_tr_pbmn||0) === 0
    );

    if (!r || r.rt_cd !== '0' || allZero) {
      log(`  ℹ️ 장중 시세성 API 값 없음 (rt_cd:${r?.rt_cd}, 전부0:${allZero}) → 일별 API 시도`);

      // ─── 2차: 장 마감 후 일별 확정 데이터 (FHPTJ04040000)
      // [버그 수정] 이전 코드: 엉뚱한 URL + TR_ID 사용
      // URL:  /uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market
      // 파라미터: 문서 기준 (FID_INPUT_DATE_1 == FID_INPUT_DATE_2: 당일 날짜)
      const today  = getDate(0);
      const params2 = {
        FID_COND_MRKT_DIV_CODE: 'U',
        FID_INPUT_ISCD:         '0001',   // 코스피 종합
        FID_INPUT_DATE_1:       today,
        FID_INPUT_ISCD_1:       'KSP',
        FID_INPUT_DATE_2:       today,    // 당일 = 당일 (동일 날짜)
        FID_INPUT_ISCD_2:       '0001'
      };
      r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market',
        token, 'FHPTJ04040000', params2
      );

      // [RAW 저장] 2차 응답 원본
      saveRaw('supply_daily_FHPTJ04040000', { params: params2, response: r });

      if (!r || r.rt_cd !== '0' || !r.output?.length) {
        log(`  ⚠️ 일별 API도 실패 (rt_cd:${r?.rt_cd}, msg:${r?.msg1}) → 이전 데이터 유지`);
        return prev?.supply || null;
      }

      // output 배열 첫 번째 행 = 가장 최근 거래일
      o = r.output[0];
      log(`  ✅ 일별 API 성공 (날짜: ${o.stck_bsop_date})`);

      // 일별 API 단위: 백만원 (÷100 → 억원) → 여기서는 원 단위 유지 후 프론트에서 변환
      const ff = parseInt(o.frgn_ntby_tr_pbmn || 0);
      const fo = parseInt(o.orgn_ntby_tr_pbmn  || 0);
      const fi = parseInt(o.prsn_ntby_tr_pbmn  || 0);
      times.krSupply = getTimeStr();
      return buildSupplyResult({ foreign: ff, individual: fi, institution: fo }, prev);
    }

    // ─── 1차 성공: 시세성 API (단위: 백원 → 원 변환)
    const f    = parseInt(o.frgn_ntby_tr_pbmn || 0) * 100;
    const ind  = parseInt(o.prsn_ntby_tr_pbmn || 0) * 100;
    const inst = parseInt(o.orgn_ntby_tr_pbmn || 0) * 100;
    log(`  ✅ 장중 시세성 API 성공 (외국인: ${f}, 기관: ${inst}, 개인: ${ind})`);
    times.krSupply = getTimeStr();
    return buildSupplyResult({ foreign: f, individual: ind, institution: inst }, prev);

  } catch(e) {
    log(`  ⚠️ 수급 실패 → 이전 유지: ${e.message}`);
    return prev?.supply || null;
  }
}

function buildSupplyResult(newVals, prev) {
  const { foreign, individual, institution } = newVals;
  const today       = getDateStr(0);
  const total       = foreign + individual + institution;
  const prevHistory = prev?.supply?.history || [];
  const history     = [...prevHistory.filter(h => h.date !== today), { date: today, total }]
    .sort((a, b) => a.date.localeCompare(b.date))
    .slice(-4);
  return { foreign, individual, institution, history };
}

// ── 프로그램 매매 ─────────────────────────────
async function fetchProgram(token, prev, times) {
  log('🤖 프로그램 매매 수집 중 (KIS)...');
  try {
    // [버그 수정]
    // 구버전 TR:  FHPPG04600001, URL: comp-program-trade-daily
    // 신버전 TR:  FHPPG04600101, URL: comp-program-trade-today  ← 변경
    // 응답 키:    output → output1                              ← 변경
    // 파라미터:   FID_SCTN_CLS_CODE 공백, FID_INPUT_ISCD 공백 등 추가
    const params = {
      FID_COND_MRKT_DIV_CODE:  'J',   // KRX
      FID_MRKT_CLS_CODE:       'K',   // 코스피
      FID_SCTN_CLS_CODE:       '',    // 공백 입력 (문서 기준)
      FID_INPUT_ISCD:          '',    // 공백 입력 (문서 기준)
      FID_COND_MRKT_DIV_CODE1: '',    // 공백 입력 (문서 기준)
      FID_INPUT_HOUR_1:        ''     // 공백 입력 (문서 기준)
    };
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/comp-program-trade-today',
      token, 'FHPPG04600101', params
    );

    // [RAW 저장]
    saveRaw('program_FHPPG04600101', { params, response: r });

    // [버그 수정] 응답 키: output → output1
    if (!r?.output1?.length) {
      log(`  ⚠️ 프로그램매매 output1 없음 (rt_cd:${r?.rt_cd}, msg:${r?.msg1})`);
      throw new Error('output1 없음');
    }

    // 가장 최근 시간 데이터 (index 0 = 최근)
    // 비어있지 않은 첫 번째 행 사용
    const d = r.output1.find(row =>
      parseInt(row.arbt_smtn_shnu_tr_pbmn||0) !== 0 ||
      parseInt(row.nabt_smtn_shnu_tr_pbmn||0) !== 0
    ) || r.output1[0];

    log(`  ✅ 프로그램매매 성공 (시간: ${d.bsop_hour})`);
    times.krProgram = getTimeStr();

    // 응답 필드명 (API 문서 확인):
    // 차익 매수: arbt_smtn_shnu_tr_pbmn
    // 차익 매도: arbt_smtn_seln_tr_pbmn
    // 비차익 매수: nabt_smtn_shnu_tr_pbmn
    // 비차익 매도: nabt_smtn_seln_tr_pbmn
    return {
      buyArb:  d.arbt_smtn_shnu_tr_pbmn || '0',
      sellArb: d.arbt_smtn_seln_tr_pbmn || '0',
      buyNon:  d.nabt_smtn_shnu_tr_pbmn || '0',
      sellNon: d.nabt_smtn_seln_tr_pbmn || '0'
    };
  } catch(e) {
    log(`  ⚠️ 프로그램매매 실패 → 이전 유지: ${e.message}`);
    return prev?.program || null;
  }
}

// ── 마켓 뎁스 ─────────────────────────────────
async function fetchDepth(token, prev, times) {
  log('📉 마켓 뎁스 수집 중 (KIS)...');
  try {
    const params = {
      FID_COND_MRKT_DIV_CODE: 'U',
      FID_INPUT_ISCD:         '0001',
      FID_INPUT_DATE_1:       getDate(-5),
      FID_INPUT_DATE_2:       getDate(0),
      FID_PERIOD_DIV_CODE:    'D'
    };
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice',
      token, 'FHKUP03500100', params
    );

    // [RAW 저장]
    saveRaw('depth_FHKUP03500100', { params, response: r });

    if (!r?.output2?.length) {
      log(`  ⚠️ 마켓뎁스 output2 없음 (rt_cd:${r?.rt_cd})`);
      throw new Error('output2 없음');
    }

    // [버그 수정]
    // 이전: [...r.output2].reverse()[r.output2.length-1]
    //       → reverse() 후 마지막 인덱스 접근 = 원본 첫 번째 요소 (가장 과거 날짜)
    // 수정: slice(-1)[0] → 배열 마지막 요소 (가장 최근 날짜)
    const sorted = [...r.output2].sort((a, b) =>
      (b.stck_bsop_date||'').localeCompare(a.stck_bsop_date||'')
    );
    const last = sorted[0]; // 가장 최근 날짜

    log(`  ✅ 마켓뎁스 성공 (날짜: ${last.stck_bsop_date}, 상승:${last.fsts_nmix_prpr_updt_stck_cnt} 보합:${last.fsts_nmix_prpr_same_stck_cnt} 하락:${last.fsts_nmix_prpr_down_stck_cnt})`);
    times.krDepth = getTimeStr();

    return {
      up:  parseInt(last.fsts_nmix_prpr_updt_stck_cnt || 0),
      neu: parseInt(last.fsts_nmix_prpr_same_stck_cnt || 0),
      dn:  parseInt(last.fsts_nmix_prpr_down_stck_cnt || 0)
    };
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
    
    // 전부 0.00%이면 → 장 전/후 미수집 상태
    // 이전에 유효한 데이터(0이 아닌 값)가 있으면 이전 데이터 유지
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
  let supply, program, depth, sectors, stocks;
  if (token) {
    supply   = await fetchSupply(token, prev, updateTimes);   await sleep(1000);
    program  = await fetchProgram(token, prev, updateTimes);  await sleep(1000);
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
    program: program || prev.program,
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

  // 수급 데이터 별도 저장 (기존 기능 유지)
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
      job();
    }
  }, 30000);
  log(`🕐 스케줄 등록: KST ${pad(hour)}:${pad(minute)}`);
}

log('🚀 MONEYLIVE 스케줄러 v5.7 시작');
log('📁 저장경로: storage/data/ + storage/logs/ + storage/raw/');
log('스케줄: 15:00 토큰 / 15:35 / 18:05 / 06:05 / 07:55');

scheduleAt(15,  0, async () => { log('[15:00] 토큰 발급'); await getToken(); flushLog(); });
scheduleAt(15, 35, () => { log('[15:35] 1차 수집'); collect(); });
scheduleAt(18,  5, () => { log('[18:05] 2차 수집'); collect(); });
scheduleAt( 6,  5, () => { log('[06:05] 3차 수집'); collect(); });
scheduleAt( 7, 55, () => { log('[07:55] 4차 수집 (최종)'); collect(); });

log('📂 시작 시 즉시 수집 (3초 후)');
setTimeout(collect, 3000);
