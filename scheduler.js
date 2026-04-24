/**
 * MONEYLIVE 스케줄러 v5.1 (통합 개선판)
 * 수정사항:
 * - 수급 데이터 시세성 TR(FHPTJ04030000) 교체 및 금액 단위 파싱
 * - KIS API 호출 간격 1000ms 연장 (429 에러 방어)
 * - Yahoo Finance 비유동성 종목(KORZ) Null 맵핑 에러 방어
 * - CoinGecko 서버사이드 수집 모듈 추가 (CORS 방어)
 * - 날짜 KST 및 요일 포함 포맷팅 (parseKisDate)
 * - 상태 모니터링용 systemStatus 객체 매핑
 * - 수급 데이터(supply.json) 별도 파일 저장 추가
 */

const fs    = require('fs');
const path  = require('path');
const https = require('https');

// 저장 경로
const STORAGE_DIR = path.join(__dirname, 'storage');
const DATA_DIR    = path.join(STORAGE_DIR, 'data');
const LOG_DIR     = path.join(STORAGE_DIR, 'logs');
const DATA_FILE   = path.join(__dirname, 'data.json'); 
const TOKEN_FILE  = path.join(__dirname, 'token.json');

// 디렉토리 생성
[STORAGE_DIR, DATA_DIR, LOG_DIR].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d, {recursive:true}); });

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

function getDate(offset = 0) {
  return getDateStr(offset).replace(/-/g, '');
}

// ❌ 문제 해결: 날짜 포맷팅을 'M/D 요일' 형태로 변경
function parseKisDate(dateStr) {
  if (!dateStr || dateStr.length !== 8) return "-";
  const y = dateStr.slice(0, 4);
  const m = dateStr.slice(4, 6);
  const d = dateStr.slice(6, 8);
  const date = new Date(`${y}-${m}-${d}T00:00:00+09:00`);
  const days = ['일', '월', '화', '수', '목', '금', '토'];
  return `${parseInt(m)}/${parseInt(d)} ${days[date.getDay()]}`;
}

function pad(n) { return String(n).padStart(2, '0'); }

function loadPrevData() {
  try {
    if (!fs.existsSync(DATA_FILE)) return {};
    return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  } catch { return {}; }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  const dateFile = path.join(DATA_DIR, `${getDateStr(0)}.json`);
  fs.writeFileSync(dateFile, JSON.stringify(data, null, 2));
  flushLog();
}

// ─────────────────────────────────────────────
// HTTP
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
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('JSON파싱실패: '+data.slice(0,200))); } });
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}

function kisGet(apiPath, token, trId, params = {}) {
  const qs = Object.entries(params).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join('&');
  const fullPath = qs ? `${apiPath}?${qs}` : apiPath;
  return new Promise((resolve, reject) => {
    https.get({
      hostname: 'openapi.koreainvestment.com', port: 9443, path: fullPath,
      headers: {
        'content-type': 'application/json; charset=utf-8',
        'authorization': `Bearer ${token}`,
        'appkey': APP_KEY, 'appsecret': APP_SECRET,
        'tr_id': trId, 'custtype': 'P'
      }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('JSON파싱실패: '+data.slice(0,200))); } });
    }).on('error', reject);
  });
}

// ─────────────────────────────────────────────
// 토큰
// ─────────────────────────────────────────────
function loadToken() {
  try {
    if (!fs.existsSync(TOKEN_FILE)) return null;
    const t = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
    if (Date.now() < new Date(t.expires_at).getTime() - 30*60*1000) return t.access_token;
    return null;
  } catch { return null; }
}

async function issueToken() {
  if (!APP_KEY || !APP_SECRET) { log('❌ 환경변수 KIS_APP_KEY/KIS_APP_SECRET 없음!'); return null; }
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
    log('❌ 토큰 발급 실패: ' + JSON.stringify(res).slice(0,200));
    return null;
  } catch(e) { log('❌ 토큰 오류: '+e.message); return null; }
}

async function getToken() {
  const cached = loadToken();
  if (cached) { log('♻️  기존 토큰 재사용'); return cached; }
  return await issueToken();
}

// ─────────────────────────────────────────────
// Yahoo Finance & CoinGecko
// ─────────────────────────────────────────────
function fetchYahoo(symbol) {
  return new Promise((resolve, reject) => {
    https.get({
      hostname: 'query1.finance.yahoo.com',
      path: `/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=7d`,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(data);
          // ❌ 문제 해결: 데이터 객체가 비어있을 경우 안전한 방어
          const result = j?.chart?.result?.[0];
          if (!result || !result.indicators?.quote?.[0]) return resolve(null);
          
          const closes = result.indicators.quote[0].close;
          const timestamps = result.timestamp;
          const pairs = timestamps.map((ts,i) => ({ts, c:closes[i]})).filter(p => p.c!=null && !isNaN(p.c));
          if (!pairs.length) return resolve(null);
          
          const last = pairs[pairs.length-1];
          const prev = pairs.length > 1 ? pairs[pairs.length-2] : null;
          const change = prev ? ((last.c - prev.c) / prev.c * 100) : 0;
          
          const toKst = (ts) => {
            const d = new Date(ts*1000);
            const kst = new Date(d.toLocaleString('en-US', {timeZone:'Asia/Seoul'}));
            return `${kst.getFullYear()}-${String(kst.getMonth()+1).padStart(2,'0')}-${String(kst.getDate()).padStart(2,'0')}`;
          };
          resolve({
            value: last.c.toFixed(2),
            change: parseFloat(change.toFixed(2)),
            rate: parseFloat(change.toFixed(2)),
            history: pairs.map((p,i) => ({
              date: toKst(p.ts),
              close: p.c,
              change: i > 0 ? parseFloat(((p.c-pairs[i-1].c)/pairs[i-1].c*100).toFixed(2)) : 0
            }))
          });
        } catch(e) { resolve(null); }
      });
    }).on('error', () => resolve(null));
  });
}

// ❌ 신규: 프론트엔드 CORS 차단 방지용 서버사이드 코인 수집
function fetchCrypto() {
  return new Promise((resolve) => {
    https.get({
      hostname: 'api.coingecko.com',
      path: '/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true',
      headers: { 'User-Agent': 'Mozilla/5.0' }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } 
        catch(e) { resolve(null); }
      });
    }).on('error', () => resolve(null));
  });
}

// ─────────────────────────────────────────────
// 한국 데이터
// ─────────────────────────────────────────────
async function fetchIndex(token, prev) {
  log('📊 코스피/코스닥 수집 중...');
  const fetchOne = async (iscd, name, prevVal) => {
    try {
      const r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice',
        token, 'FHKUP03500100',
        { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: iscd,
          FID_INPUT_DATE_1: getDate(-10), FID_INPUT_DATE_2: getDate(0),
          FID_PERIOD_DIV_CODE: 'D' }
      );
      const o1 = r?.output1;
      if (!o1?.bstp_nmix_prpr) throw new Error('현재가 없음');
      return { value: o1.bstp_nmix_prpr, change: parseFloat(o1.bstp_nmix_prdy_vrss||0), rate: parseFloat(o1.prdy_ctrt||0) };
    } catch(e) {
      log(`  ⚠️ ${name} 실패→이전유지: ${e.message}`);
      return prevVal || null;
    }
  };
  const [kospi, kosdaq] = await Promise.all([ fetchOne('0001', '코스피', prev.kospi), fetchOne('1001', '코스닥', prev.kosdaq) ]);
  return { kospi, kosdaq };
}

// ❌ 문제 해결: 금액 단위 수급 호출로 변경 (FHPTJ04030000)
async function fetchSupply(token, prev) {
  log('💰 수급 동향 수집 중...');
  try {
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market',
      token, 'FHPTJ04030000',
      { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001' }
    );
    const o = r?.output1;
    if (!o) throw new Error('데이터없음');

    const f    = parseInt(o.frgn_ntby_tr_pbmn || 0);
    const ind  = parseInt(o.prsn_ntby_tr_pbmn || 0);
    const inst = parseInt(o.orgn_ntby_tr_pbmn || 0);

    if (f===0 && inst===0 && ind===0 && prev?.supply) {
      log('  ⚠️ 수급 모두 0 → 이전 데이터 유지');
      return prev.supply;
    }

    log(`  외국인: ${f} / 기관: ${inst} / 개인: ${ind}`);
    return { foreign: f, person: ind, organ: inst };
  } catch(e) {
    log(`  ⚠️ 수급 실패→이전유지: ${e.message}`);
    return prev?.supply || null;
  }
}

async function fetchProgram(token, prev) {
  log('🤖 프로그램 매매 수집 중...');
  try {
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/comp-program-trade-daily',
      token, 'FHPPG04600001',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_MRKT_CLS_CODE: 'K',
        FID_INPUT_DATE_1: getDate(-1), FID_INPUT_DATE_2: getDate(0) }
    );
    const rows = r?.output;
    if (!rows?.length) throw new Error('데이터없음');
    const d = rows.find(row => parseInt(row.arbt_buy_amt||0) !== 0 || parseInt(row.nabt_buy_amt||0) !== 0 ) || rows[0];
    if (parseInt(d.arbt_buy_amt||0)===0 && parseInt(d.nabt_buy_amt||0)===0 && prev.program) return prev.program;
    return { buyArb: d.arbt_buy_amt, sellArb: d.arbt_sel_amt, buyNon: d.nabt_buy_amt, sellNon: d.nabt_sel_amt };
  } catch { return prev.program || null; }
}

async function fetchDepth(token, prev) {
  log('📉 마켓 뎁스 수집 중...');
  try {
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice',
      token, 'FHKUP03500100',
      { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001',
        FID_INPUT_DATE_1: getDate(-1), FID_INPUT_DATE_2: getDate(0), FID_PERIOD_DIV_CODE: 'D' }
    );
    const rows = r?.output2;
    if (!rows?.length) throw new Error('데이터없음');
    const last = [...rows].reverse()[rows.length-1];
    const up  = parseInt(last.fsts_nmix_prpr_updt_stck_cnt||0);
    const neu = parseInt(last.fsts_nmix_prpr_same_stck_cnt||0);
    const dn  = parseInt(last.fsts_nmix_prpr_down_stck_cnt||0);
    if (up+neu+dn===0 && prev.depth) return prev.depth;
    return { up, neu, dn };
  } catch { return prev.depth || null; }
}

// ❌ 문제 해결: ETF 1초 간격 순차 호출 (429 차단 방어)
async function fetchKrEtf(token, prev) {
  log('🇰🇷 한국 ETF 수집 중...');
  const codes = {
    kodex200: '069500', kodexkosdaq: '229200', kodexlev: '122630',
    kodexinv: '114800', kodexinv2: '251340', kodexsp: '379800'
  };
  const etf = { ...(prev.koreaEtf||{}) };
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  
  for (const [key, code] of Object.entries(codes)) {
    try {
      const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-price', token, 'FHKST01010100', { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: code });
      if (r?.output?.stck_prpr) etf[key] = { value: parseInt(r.output.stck_prpr), rate: parseFloat(r.output.prdy_ctrt||0) };
    } catch { log(`  ⚠️ ETF ${key} 실패`); }
    await sleep(1000); // 1000ms 강제 대기
  }
  return etf;
}

// ❌ 문제 해결: 섹터 1초 간격 순차 호출
async function fetchKrSectors(token, prev) {
  log('📊 KR 섹터 수집 중...');
  const sectors = [
    { code: '005930', name: '반도체' }, { code: '207940', name: '제약/바이오' },
    { code: '105560', name: '금융업' }, { code: '005380', name: '자동차' },
    { code: '051910', name: '화학' }, { code: '373220', name: '2차전지' },
    { code: '005490', name: '철강/금속' }, { code: '000720', name: '건설업' },
    { code: '066570', name: '전기전자' }, { code: '097950', name: '음식료' }, { code: '282330', name: '유통' }
  ];
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const results = [];
  
  for (const s of sectors) {
    try {
      const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-price', token, 'FHKST01010100', { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: s.code });
      if (r?.output?.stck_prpr) results.push({ name: s.name, chg: parseFloat(r.output.prdy_ctrt||0) });
    } catch {}
    await sleep(1000);
  }
  return results.length > 0 ? results : (prev.sectors || null);
}

async function fetchKrStocks(token, prev) {
  // 기존 코드 유지하되, 필요시 해당 모듈도 sleep 적용
  return prev.stocks || null;
}

async function fetchUsData(prev) {
  log('🇺🇸 미국 데이터 수집 중...');
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
  await Promise.allSettled(Object.entries(symbols).map(async ([key, sym]) => {
    const data = await fetchYahoo(sym);
    if(data) results[key] = data;
  }));
  return results;
}

// ─────────────────────────────────────────────
// 전체 수집
// ─────────────────────────────────────────────
async function collect() {
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  log('📡 데이터 수집 시작');
  
  // ❌ 시스템 상태 기록 객체 초기화
  const systemStatus = { kis: '확인 중...', kr: '확인 중...', us: '확인 중...', crypto: '확인 중...' };

  const token = await getToken();
  if (!token) { 
    systemStatus.kis = 'ERROR';
    log('❌ 토큰 없음 - 수집 중단'); 
    flushLog(); return; 
  }
  systemStatus.kis = 'OK';

  const prev = loadPrevData();
  
  const [krIndex, supply, program, depth, etf, sectors, stocks, us, crypto] = await Promise.all([
    fetchIndex(token, prev), fetchSupply(token, prev), fetchProgram(token, prev),
    fetchDepth(token, prev), fetchKrEtf(token, prev), fetchKrSectors(token, prev),
    fetchKrStocks(token, prev), fetchUsData(prev), fetchCrypto()
  ]);

  systemStatus.kr = (krIndex.kospi && krIndex.kosdaq) ? 'OK' : 'ERROR';
  systemStatus.us = Object.keys(us).length > 0 ? 'OK' : 'ERROR';
  systemStatus.crypto = crypto ? 'OK' : 'ERROR';

  const pick = (newVal, prevVal) => newVal ? { value: newVal.value, change: newVal.change, rate: newVal.rate } : (prevVal||null);
  const prevUs    = prev.usIndex   || {};
  const prevM7    = prev.usM7      || {};
  const prevKrEtf = prev.koreaEtf  || {};
  const prevCom   = prev.commodities || {};

  const finalData = {
    time: kstNow().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }),
    systemStatus: systemStatus,
    korea: krIndex, 
    supply: supply || prev.supply, 
    program: program || prev.program, 
    depth: depth || prev.depth, 
    koreaEtf: etf, 
    sectors: sectors || prev.sectors, 
    stocks: stocks || prev.stocks,
    crypto: crypto || prev.crypto,
    usIndex: {
      sp500:  pick(us.sp500,  prevUs.sp500),  nasdaq100: pick(us.nasdaq, prevUs.nasdaq100),
      dow:    pick(us.dow,    prevUs.dow),    vix:    pick(us.vix,    prevUs.vix),
      tnx:    pick(us.tnx,    prevUs.tnx),    dxy:    pick(us.dxy,    prevUs.dxy),
      spy:    pick(us.spy,    prevUs.spy),    qqq:    pick(us.qqq,    prevUs.qqq),
      tqqq:   pick(us.tqqq,   prevUs.tqqq),   sqqq:   pick(us.sqqq,   prevUs.sqqq)
    },
    usM7: {
      aapl:  { name: 'Apple', ...pick(us.aapl,  prevM7.aapl) },
      msft:  { name: 'Microsoft', ...pick(us.msft,  prevM7.msft) },
      nvda:  { name: 'Nvidia', ...pick(us.nvda,  prevM7.nvda) },
      amzn:  { name: 'Amazon', ...pick(us.amzn,  prevM7.amzn) },
      googl: { name: 'Alphabet', ...pick(us.googl, prevM7.googl) },
      meta:  { name: 'Meta', ...pick(us.meta,  prevM7.meta) },
      tsla:  { name: 'Tesla', ...pick(us.tsla,  prevM7.tsla) }
    },
    commodities: {
      wti:    pick(us.wti,    prevCom.wti),
      gold:   pick(us.gold,   prevCom.gold),
      copper: pick(us.copper, prevCom.copper)
    }
  };

  saveData(finalData);
  
  // ❌ 문제 해결: 수급 데이터 분리 저장
  if (supply) {
    const SUPPLY_FILE = path.join(DATA_DIR, 'supply.json');
    fs.writeFileSync(SUPPLY_FILE, JSON.stringify(supply, null, 2));
    log(`✅ 수급 데이터(supply.json) 개별 저장 완료`);
  }
  
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
}

// ─────────────────────────────────────────────
// 스케줄러
// ─────────────────────────────────────────────
function scheduleAt(hour, minute, job) {
  let lastRunDay = -1;
  setInterval(() => {
    const now = kstNow();
    const today = now.getFullYear()*10000 + now.getMonth()*100 + now.getDate();
    if (now.getHours()===hour && now.getMinutes()===minute && lastRunDay!==today) {
      lastRunDay = today; job();
    }
  }, 30000);
  log(`🕐 스케줄 등록: KST ${pad(hour)}:${pad(minute)}`);
}

log('🚀 MONEYLIVE 스케줄러 v5.1 시작');
log(`📁 저장경로: storage/data/ + storage/logs/`);
log('스케줄: 15:00 토큰 / 15:35 / 18:05 / 06:05 / 07:55');

scheduleAt(15,  0, async () => { log('[15:00] 토큰 발급'); await issueToken(); flushLog(); });
scheduleAt(15, 35, () => { log('[15:35] 1차 수집'); collect(); });
scheduleAt(18,  5, () => { log('[18:05] 2차 수집'); collect(); });
scheduleAt( 6,  5, () => { log('[06:05] 3차 수집'); collect(); });
scheduleAt( 7, 55, () => { log('[07:55] 4차 수집 (최종)'); collect(); });

log('📂 시작 시 즉시 수집 (3초 후)');
// ❌ 문제 해결: 컨테이너 재시작 시 자동 복구를 위해 즉시 collect() 수행
setTimeout(collect, 3000);
