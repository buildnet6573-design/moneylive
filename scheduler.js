/**
 * MONEYLIVE 스케줄러 v5.4 (UI 변수 매핑 및 억 단위 환산 완벽 동기화)
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

function kstNow() { return new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' })); }
function getDateStr(offset = 0) {
  const d = kstNow(); d.setDate(d.getDate() + offset);
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}
function getDate(offset = 0) { return getDateStr(offset).replace(/-/g, ''); }
function pad(n) { return String(n).padStart(2, '0'); }

function getTimeStr() {
  const n = kstNow();
  return `${pad(n.getMonth()+1)}/${pad(n.getDate())} ${pad(n.getHours())}:${pad(n.getMinutes())}:${pad(n.getSeconds())}`;
}

function loadPrevData() {
  try { return fs.existsSync(DATA_FILE) ? JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) : {}; } 
  catch { return {}; }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  fs.writeFileSync(path.join(DATA_DIR, `${getDateStr(0)}.json`), JSON.stringify(data, null, 2));
  flushLog();
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
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('JSON 파싱실패')); } });
    });
    req.on('error', reject); req.write(bodyStr); req.end();
  });
}

function kisGet(apiPath, token, trId, params = {}) {
  const qs = Object.entries(params).map(([k,v]) => `${k}=${encodeURIComponent(v)}`).join('&');
  return new Promise((resolve, reject) => {
    https.get({
      hostname: 'openapi.koreainvestment.com', port: 9443, path: qs ? `${apiPath}?${qs}` : apiPath,
      headers: { 'content-type': 'application/json; charset=utf-8', 'authorization': `Bearer ${token}`, 'appkey': APP_KEY, 'appsecret': APP_SECRET, 'tr_id': trId, 'custtype': 'P' }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(new Error('JSON 파싱실패')); } });
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
    const res = await httpPost('openapi.koreainvestment.com', '/oauth2/tokenP', { grant_type: 'client_credentials', appkey: APP_KEY, appsecret: APP_SECRET });
    if (res.access_token) {
      fs.writeFileSync(TOKEN_FILE, JSON.stringify({ access_token: res.access_token, expires_at: res.access_token_token_expired, issued_at: new Date().toISOString() }));
      log('✅ 토큰 발급 완료'); return res.access_token;
    }
  } catch(e) { log('❌ 토큰 오류: '+e.message); }
  return null;
}

// ─────────────────────────────────────────────
// Yahoo Finance & CoinGecko
// ─────────────────────────────────────────────
function fetchYahoo(symbol) {
  return new Promise((resolve) => {
    https.get({
      hostname: 'query1.finance.yahoo.com', path: `/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=7d`, headers: { 'User-Agent': 'Mozilla/5.0' }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const result = JSON.parse(data)?.chart?.result?.[0];
          if (!result || !result.indicators?.quote?.[0]) return resolve(null);
          
          const closes = result.indicators.quote[0].close;
          const timestamps = result.timestamp;
          const pairs = timestamps.map((ts,i) => ({ts, c:closes[i]})).filter(p => p.c!=null && !isNaN(p.c));
          if (!pairs.length) return resolve(null);
          
          const last = pairs[pairs.length-1];
          const prev = pairs.length > 1 ? pairs[pairs.length-2] : null;
          const change = prev ? ((last.c - prev.c) / prev.c * 100) : 0;
          const toKst = (ts) => { const d = new Date(ts*1000); const kst = new Date(d.toLocaleString('en-US', {timeZone:'Asia/Seoul'})); return `${kst.getFullYear()}-${String(kst.getMonth()+1).padStart(2,'0')}-${String(kst.getDate()).padStart(2,'0')}`; };
          
          resolve({
            value: last.c.toFixed(2), change: parseFloat(change.toFixed(2)), rate: parseFloat(change.toFixed(2)),
            history: pairs.map((p,i) => ({ date: toKst(p.ts), close: p.c, change: i > 0 ? parseFloat(((p.c-pairs[i-1].c)/pairs[i-1].c*100).toFixed(2)) : 0 }))
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
// 수집 모듈 (Yahoo)
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function fetchIndex(prev, times) {
  log('📊 지수 수집 중 (Yahoo)...');
  const kospi = await fetchYahoo('^KS11'); await sleep(300);
  const kosdaq = await fetchYahoo('^KQ11');
  if (kospi || kosdaq) times.krIndex = getTimeStr();
  return { kospi: kospi || prev?.kospi, kosdaq: kosdaq || prev?.kosdaq };
}

async function fetchKrEtf(prev, times) {
  log('🇰🇷 한국 ETF 수집 중 (Yahoo)...');
  const symbols = { kodex200: '069500.KS', kodexkosdaq: '229200.KS', kodexlev: '122630.KS', kodexinv: '114800.KS', kodexinv2: '251340.KS', kodexsp: '379800.KS' };
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
    xlk:'XLK', xlc:'XLC', xly:'XLY', xlf:'XLF', xlv:'XLV', xli:'XLI', xlp:'XLP', xlre:'XLRE', xlu:'XLU', xlb:'XLB', xle:'XLE'
  };
  const results = {};
  let isUpdated = false;
  for (const [key, sym] of Object.entries(symbols)) {
    const data = await fetchYahoo(sym);
    if(data) { results[key] = data; isUpdated = true; }
    await sleep(300); 
  }
  if (isUpdated) times.usData = getTimeStr();
  return results;
}

// ─────────────────────────────────────────────
// 수집 모듈 (KIS)
// ─────────────────────────────────────────────
async function fetchSupply(token, prev, times) {
  log('💰 수급 동향 수집 중 (KIS)...');
  try {
    // 기본 TR
    let r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market', token, 'FHPTJ04030000', { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001' });
    
    // 거절 시 대체 TR (업종별 투자자 매매동향)
    if (!r || r.rt_cd !== '0' || !r.output1) {
      log('  ⚠️ 기본 수급 API 거절 -> Fallback TR 호출');
      r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-investor-sector-trend', token, 'FHPUP02110000', { FID_COND_MRKT_DIV_CODE: 'U', FID_COND_SCR_DIV_CODE: '20211', FID_INPUT_ISCD: '0001' });
      if (!r || r.rt_cd !== '0' || !r.output || r.output.length === 0) throw new Error('Fallback API도 거절됨');
      
      const o = r.output[0];
      // 백만원 단위 -> UI 억 단위 대응을 위해 /100
      times.krSupply = getTimeStr();
      return { 
        foreign: parseInt(o.frgn_ntby_tr_pbmn||0) * 10000, 
        person: parseInt(o.prsn_ntby_tr_pbmn||0) * 10000, 
        organ: parseInt(o.orgn_ntby_tr_pbmn||0) * 10000 
      };
    }

    const o = r.output1;
    // 원 단위 -> UI 억 단위 환산을 위한 변환 (/100)
    const f = parseInt(o.frgn_ntby_tr_pbmn || 0) / 100;
    const ind = parseInt(o.prsn_ntby_tr_pbmn || 0) / 100;
    const inst = parseInt(o.orgn_ntby_tr_pbmn || 0) / 100;

    if (f===0 && inst===0 && ind===0 && prev?.supply) return prev.supply;
    times.krSupply = getTimeStr();
    return { foreign: f, person: ind, organ: inst };
  } catch(e) {
    log(`  ⚠️ 수급 실패→이전유지: ${e.message}`);
    return prev?.supply || null;
  }
}

async function fetchProgram(token, prev, times) {
  log('🤖 프로그램 매매 수집 중 (KIS)...');
  try {
    const r = await kisGet('/uapi/domestic-stock/v1/quotations/comp-program-trade-daily', token, 'FHPPG04600001', { FID_COND_MRKT_DIV_CODE: 'J', FID_MRKT_CLS_CODE: 'K', FID_INPUT_DATE_1: getDate(-1), FID_INPUT_DATE_2: getDate(0) });
    if (!r?.output?.length) throw new Error('데이터없음');
    const d = r.output.find(row => parseInt(row.arbt_buy_amt||0) !== 0 || parseInt(row.nabt_buy_amt||0) !== 0 ) || r.output[0];
    times.krProgram = getTimeStr();
    return { buyArb: d.arbt_buy_amt, sellArb: d.arbt_sel_amt, buyNon: d.nabt_buy_amt, sellNon: d.nabt_sel_amt };
  } catch { return prev?.program || null; }
}

async function fetchDepth(token, prev, times) {
  log('📉 마켓 뎁스 수집 중 (KIS)...');
  try {
    const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice', token, 'FHKUP03500100', { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001', FID_INPUT_DATE_1: getDate(-1), FID_INPUT_DATE_2: getDate(0), FID_PERIOD_DIV_CODE: 'D' });
    if (!r?.output2?.length) throw new Error('데이터없음');
    const last = [...r.output2].reverse()[r.output2.length-1];
    times.krDepth = getTimeStr();
    return { up: parseInt(last.fsts_nmix_prpr_updt_stck_cnt||0), neu: parseInt(last.fsts_nmix_prpr_same_stck_cnt||0), dn: parseInt(last.fsts_nmix_prpr_down_stck_cnt||0) };
  } catch { return prev?.depth || null; }
}

async function fetchKrSectors(token, prev, times) {
  log('📊 KR 섹터 수집 중 (KIS)...');
  const sectors = [
    { code: '005930', name: '반도체' }, { code: '207940', name: '제약/바이오' }, { code: '105560', name: '금융업' }, { code: '005380', name: '자동차' },
    { code: '051910', name: '화학' }, { code: '373220', name: '2차전지' }, { code: '005490', name: '철강/금속' }, { code: '000720', name: '건설업' },
    { code: '066570', name: '전기전자' }, { code: '097950', name: '음식료' }, { code: '282330', name: '유통' }
  ];
  const results = [];
  let isUpdated = false;
  for (const s of sectors) {
    try {
      const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-price', token, 'FHKST01010100', { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: s.code });
      if (r?.output?.stck_prpr) {
         results.push({ 
           name: s.name, 
           chg: parseFloat(r.output.prdy_ctrt||0),
           vol: Math.round(parseInt(r.output.acml_tr_pbmn||0) / 100000000) // 누적대금(원) -> 억 단위
         });
         isUpdated = true;
      }
    } catch {}
    await sleep(500); 
  }
  if (isUpdated) times.krSector = getTimeStr();
  return results.length > 0 ? results : (prev?.sectors || null);
}

async function fetchKrStocks(token, prev, times) {
  log('📋 KR 순위 종목 수집 중 (KIS)...');
  const prevStocks = prev?.stocks || {};
  let isUpdated = false;

  try {
    const volRes = await kisGet('/uapi/domestic-stock/v1/quotations/volume-rank', token, 'FHPST01710000',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20171', FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0', FID_BLNG_CLS_CODE: '0', FID_TRGT_CLS_CODE: '111111111', FID_TRGT_EXLS_CLS_CODE: '000000', FID_INPUT_PRICE_1: '', FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: '' });
    
    const volume = (volRes?.output||[]).slice(0,10).map(s => ({
      name: s.hts_kor_isnm, code: s.mksc_shrn_iscd, 
      chg: parseFloat(s.prdy_ctrt||0), 
      amt: Math.round(parseInt(s.acml_tr_pbmn||0) / 100000000) // 누적대금(원) -> 억 단위
    }));
    
    if(volume.length > 0) isUpdated = true;
    await sleep(1000); 

    const fetchRank = async (etcCls, rankSort) => {
      try {
        const r = await kisGet('/uapi/domestic-stock/v1/quotations/foreign-institution-total', token, 'FHPTJ04400000',
          { FID_COND_MRKT_DIV_CODE: 'V', FID_COND_SCR_DIV_CODE: '16449', FID_INPUT_ISCD: '0001', FID_DIV_CLS_CODE: '0', FID_RANK_SORT_CLS_CODE: String(rankSort), FID_ETC_CLS_CODE: String(etcCls) });
        return (r?.output||[]).slice(0,5).map(s => ({ 
          name: s.hts_kor_isnm, code: s.mksc_shrn_iscd, 
          chg: parseFloat(s.prdy_ctrt||0), 
          amt: Math.round(parseInt(s.ntby_tr_pbmn||s.frgn_ntby_tr_pbmn||0) / 100000000) // 매매대금(원) -> 억 단위
        }));
      } catch(e) { return null; }
    };

    const fB = await fetchRank(1, 0); await sleep(1000);
    const fS = await fetchRank(1, 1); await sleep(1000);
    const iB = await fetchRank(2, 0); await sleep(1000);
    const iS = await fetchRank(2, 1);

    if (isUpdated) times.krStock = getTimeStr();
    return {
      volume: volume.length ? volume : prevStocks.volume,
      foreignBuy: fB?.length ? fB : prevStocks.foreignBuy,
      foreignSell:fS?.length ? fS : prevStocks.foreignSell,
      instBuy: iB?.length ? iB : prevStocks.instBuy,
      instSell: iS?.length ? iS : prevStocks.instSell,
      indvBuy: prevStocks.indvBuy || null, indvSell: prevStocks.indvSell || null,
    };
  } catch(e) {
    log(`  ⚠️ KR종목 실패→이전유지: ${e.message}`);
    return prevStocks || null;
  }
}

// ─────────────────────────────────────────────
// 전체 수집 실행 모듈
// ─────────────────────────────────────────────
async function collect() {
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  log('📡 데이터 수집 시작');
  
  const systemStatus = { kis: '확인 중...', kr: '확인 중...', us: '확인 중...', crypto: '확인 중...' };

  const token = await getToken();
  if (!token) { systemStatus.kis = 'ERROR'; log('❌ 토큰 없음 - KIS 수집 중단'); } 
  else { systemStatus.kis = 'OK'; }

  const prev = loadPrevData();
  const updateTimes = prev.updateTimes || { krIndex: '-', krSupply: '-', krProgram: '-', krDepth: '-', krEtf: '-', krSector: '-', krStock: '-', usData: '-', crypto: '-' };

  // 1. Yahoo & CoinGecko 수집 (토큰 불필요)
  const krIndex = await fetchIndex(prev, updateTimes); await sleep(1000);
  const etf = await fetchKrEtf(prev, updateTimes); await sleep(1000);
  const us = await fetchUsData(prev, updateTimes); await sleep(1000);
  const crypto = await fetchCrypto(prev.crypto, updateTimes); await sleep(1000);

  // 2. KIS 수집 (토큰 필요)
  let supply, program, depth, sectors, stocks;
  if (token) {
    supply = await fetchSupply(token, prev, updateTimes); await sleep(1000);
    program = await fetchProgram(token, prev, updateTimes); await sleep(1000);
    depth = await fetchDepth(token, prev, updateTimes); await sleep(1000);
    sectors = await fetchKrSectors(token, prev, updateTimes); await sleep(1000);
    stocks = await fetchKrStocks(token, prev, updateTimes);
  }

  systemStatus.kr = (krIndex.kospi && krIndex.kosdaq) ? 'OK' : 'ERROR';
  systemStatus.us = Object.keys(us).length > 0 ? 'OK' : 'ERROR';
  systemStatus.crypto = crypto ? 'OK' : 'ERROR';

  const pick = (newVal, prevVal) => newVal ? { value: newVal.value, change: newVal.change, rate: newVal.rate, history: newVal.history } : (prevVal||null);
  const prevUs    = prev.us || {};
  const prevM7    = prev.m7 || {};
  const prevKrEtf = prev.koreaEtf || {};
  const prevCom   = prev.commodities || {};

  // 프론트엔드(index.html)가 요구하는 정확한 변수명으로 구조화
  const finalData = {
    time: kstNow().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }),
    updateTimes: updateTimes, 
    systemStatus: systemStatus,
    
    // 국내 지수
    kospi: krIndex.kospi, 
    kosdaq: krIndex.kosdaq,
    
    // KIS 데이터
    supply: supply || prev.supply, 
    program: program || prev.program, 
    depth: depth || prev.depth, 
    sectors: sectors || prev.sectors, 
    stocks: stocks || prev.stocks,
    
    // 한국 ETF (KODEX 등)
    etf: etf, 
    
    // 암호화폐
    crypto: crypto || prev.crypto,
    
    // 미국 시장 데이터
    us: {
      sp500:  pick(us.sp500,  prevUs.sp500),  nasdaq: pick(us.nasdaq, prevUs.nasdaq), // index.html 명세 동기화
      dow:    pick(us.dow,    prevUs.dow),    vix:    pick(us.vix,    prevUs.vix),
      tnx:    pick(us.tnx,    prevUs.tnx),    dxy:    pick(us.dxy,    prevUs.dxy),
      spy:    pick(us.spy,    prevUs.spy),    qqq:    pick(us.qqq,    prevUs.qqq),
      tqqq:   pick(us.tqqq,   prevUs.tqqq),   sqqq:   pick(us.sqqq,   prevUs.sqqq)
    },
    
    // 빅테크 M7
    m7: {
      aapl:  { name: 'Apple', ...pick(us.aapl,  prevM7.aapl) },
      msft:  { name: 'Microsoft', ...pick(us.msft,  prevM7.msft) },
      nvda:  { name: 'Nvidia', ...pick(us.nvda,  prevM7.nvda) },
      amzn:  { name: 'Amazon', ...pick(us.amzn,  prevM7.amzn) },
      googl: { name: 'Alphabet', ...pick(us.googl, prevM7.googl) },
      meta:  { name: 'Meta', ...pick(us.meta,  prevM7.meta) },
      tsla:  { name: 'Tesla', ...pick(us.tsla,  prevM7.tsla) }
    },
    
    // 한국 관련 미국 ETF
    koreaEtf: {
      ewy:  pick(us.ewy,  prevKrEtf.ewy),
      koru: pick(us.koru, prevKrEtf.koru),
      korz: pick(us.korz, prevKrEtf.korz)
    },
    
    // 원자재
    commodities: {
      wti:    pick(us.wti,    prevCom.wti),
      gold:   pick(us.gold,   prevCom.gold),
      copper: pick(us.copper, prevCom.copper)
    },

    // 미국 섹터
    usSectors: [
      { name: 'IT', chg: us.xlk?.rate || 0 },
      { name: '커뮤니케이션', chg: us.xlc?.rate || 0 },
      { name: '임의소비재', chg: us.xly?.rate || 0 },
      { name: '금융', chg: us.xlf?.rate || 0 },
      { name: '헬스케어', chg: us.xlv?.rate || 0 },
      { name: '산업재', chg: us.xli?.rate || 0 },
      { name: '필수소비재', chg: us.xlp?.rate || 0 },
      { name: '부동산', chg: us.xlre?.rate || 0 },
      { name: '유틸리티', chg: us.xlu?.rate || 0 },
      { name: '소재', chg: us.xlb?.rate || 0 },
      { name: '에너지', chg: us.xle?.rate || 0 },
    ]
  };

  saveData(finalData);
  if (supply) { fs.writeFileSync(path.join(DATA_DIR, 'supply.json'), JSON.stringify(supply, null, 2)); }
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

log('🚀 MONEYLIVE 스케줄러 v5.4 시작');
log(`📁 저장경로: storage/data/ + storage/logs/`);
log('스케줄: 15:00 토큰 / 15:35 / 18:05 / 06:05 / 07:55');

scheduleAt(15,  0, async () => { log('[15:00] 토큰 발급'); await issueToken(); flushLog(); });
scheduleAt(15, 35, () => { log('[15:35] 1차 수집'); collect(); });
scheduleAt(18,  5, () => { log('[18:05] 2차 수집'); collect(); });
scheduleAt( 6,  5, () => { log('[06:05] 3차 수집'); collect(); });
scheduleAt( 7, 55, () => { log('[07:55] 4차 수집 (최종)'); collect(); });

log('📂 시작 시 즉시 수집 (3초 후)');
setTimeout(collect, 3000);
