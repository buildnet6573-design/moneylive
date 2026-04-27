/**
 * MONEYLIVE 스케줄러 v5.6 (정밀 데이터 교정 및 ETF 필터링)
 * 업데이트 내역:
 * - 거래대금 TOP 10: ETF/ETN/선물 완벽 필터링 및 실제 거래금액 기준 정렬
 * - 개인 수급 추가: 외국인/기관 외에 개인(코드 3) 순매수/순매도 조회 로직 반영
 * - 섹터 데이터: 대장주 기반 안정성 확보 및 툴팁용 '대표종목' 텍스트(reps) 추가
 * - 모든 데이터 배열 내림차순 강제 정렬 보장
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
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) {
          // 원본 응답 앞 300자 로그 출력 → 어떤 응답이 오는지 즉시 확인 가능
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
    // ─── 1차: 장중 실시간 투자자 매매현황 (FID_INPUT_ISCD_2 필수 파라미터 추가)
    let r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market',
      token, 'FHPTJ04030000',
      { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001', FID_INPUT_ISCD_2: '0002' }
    );

    let o = r?.output1 || (Array.isArray(r?.output) ? r.output[0] : r?.output);

    if (!r || r.rt_cd !== '0' || !o) {
      log(`  ⚠️ 장중 TR 거절 (rt_cd:${r?.rt_cd}, msg:${r?.msg1}) -> 일별 TR 시도`);

      // ─── 2차: 장 마감 후 확정 데이터 — 시장별 투자자 일별 매매현황
      // TR: FHKST01010900 / 경로: inquire-investor (종목별이지만 시장 전체 코드 0000 사용)
      r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-investor',
        token, 'FHKST01010900',
        { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: '0000' }
      );

      if (!r || r.rt_cd !== '0' || !r.output) {
        log(`  ⚠️ 일별 TR도 거절 (rt_cd:${r?.rt_cd}, msg:${r?.msg1})`);
        throw new Error('일별 TR도 거절됨');
      }

      // output 배열에서 가장 최근 행 사용
      o = Array.isArray(r.output) ? r.output[0] : r.output;
      // 단위: 원
      const ff = parseInt(o.frgn_ntby_tr_pbmn || 0);
      const fo = parseInt(o.orgn_ntby_tr_pbmn  || 0);
      const fi = parseInt(o.prsn_ntby_tr_pbmn  || 0);
      times.krSupply = getTimeStr();
      return buildSupplyResult({ foreign: ff, individual: fi, institution: fo }, prev);
    }

    // ─── 1차 성공: output1 기준 (단위: 백원 → 원 변환)
    const f    = parseInt(o.frgn_ntby_tr_pbmn || 0) * 100;
    const ind  = parseInt(o.prsn_ntby_tr_pbmn || 0) * 100;
    const inst = parseInt(o.orgn_ntby_tr_pbmn || 0) * 100;

    if (f === 0 && inst === 0 && ind === 0 && prev?.supply) {
      log('  ℹ️ 수급 값 전부 0 → 이전 데이터 유지');
      return prev.supply;
    }
    times.krSupply = getTimeStr();
    return buildSupplyResult({ foreign: f, individual: ind, institution: inst }, prev);

  } catch(e) {
    log(`  ⚠️ 수급 실패→이전유지: ${e.message}`);
    return prev?.supply || null;
  }
}

// 수급 결과 조립: 키 통일(individual/institution) + history 누적(최근 4일)
function buildSupplyResult(newVals, prev) {
  const { foreign, individual, institution } = newVals;
  const today  = getDateStr(0);
  const total  = foreign + individual + institution;
  const prevHistory = prev?.supply?.history || [];
  const history = [...prevHistory.filter(h => h.date !== today), { date: today, total }]
    .sort((a, b) => a.date.localeCompare(b.date))
    .slice(-4);
  return { foreign, individual, institution, history };
}

async function fetchProgram(token, prev, times) {
  log('🤖 프로그램 매매 수집 중 (KIS)...');
  try {
    const r = await kisGet('/uapi/domestic-stock/v1/quotations/comp-program-trade-daily', token, 'FHPPG04600001', { FID_COND_MRKT_DIV_CODE: 'J', FID_MRKT_CLS_CODE: 'K', FID_INPUT_DATE_1: getDate(-5), FID_INPUT_DATE_2: getDate(0) });
    if (!r?.output?.length) throw new Error(`데이터없음`);
    const d = r.output.find(row => parseInt(row.arbt_buy_amt||0) !== 0 || parseInt(row.nabt_buy_amt||0) !== 0 ) || r.output[0];
    times.krProgram = getTimeStr();
    return { buyArb: d.arbt_buy_amt, sellArb: d.arbt_sel_amt, buyNon: d.nabt_buy_amt, sellNon: d.nabt_sel_amt };
  } catch(e) { 
    return prev?.program || null; 
  }
}

async function fetchDepth(token, prev, times) {
  log('📉 마켓 뎁스 수집 중 (KIS)...');
  try {
    const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice', token, 'FHKUP03500100', { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001', FID_INPUT_DATE_1: getDate(-5), FID_INPUT_DATE_2: getDate(0), FID_PERIOD_DIV_CODE: 'D' });
    if (!r?.output2?.length) throw new Error(`데이터없음`);
    const last = [...r.output2].reverse()[r.output2.length-1];
    times.krDepth = getTimeStr();
    return { up: parseInt(last.fsts_nmix_prpr_updt_stck_cnt||0), neu: parseInt(last.fsts_nmix_prpr_same_stck_cnt||0), dn: parseInt(last.fsts_nmix_prpr_down_stck_cnt||0) };
  } catch(e) { 
    return prev?.depth || null; 
  }
}

// ─────────────────────────────────────────────
// KR 섹터 (대장주 기반 절대 안정성 + 툴팁 텍스트 제공)
// ─────────────────────────────────────────────
async function fetchKrSectors(token, prev, times) {
  log('📊 KR 섹터 수집 중 (KIS)...');
  
  const sectors = [
    { code: '005930', name: '반도체/IT', reps: '대표종목: 삼성전자, SK하이닉스, 한미반도체 등' },
    { code: '373220', name: '2차전지', reps: '대표종목: LG에너지솔루션, 에코프로, 포스코퓨처엠 등' },
    { code: '207940', name: '제약/바이오', reps: '대표종목: 삼성바이오로직스, 셀트리온, 알테오젠 등' },
    { code: '105560', name: '금융업', reps: '대표종목: KB금융, 신한지주, 메리츠금융지주 등' },
    { code: '005380', name: '자동차', reps: '대표종목: 현대차, 기아, 현대모비스 등' },
    { code: '051910', name: '석유/화학', reps: '대표종목: LG화학, 금호석유, S-Oil 등' },
    { code: '005490', name: '철강/금속', reps: '대표종목: POSCO홀딩스, 고려아연, 현대제철 등' },
    { code: '000720', name: '건설업', reps: '대표종목: 현대건설, GS건설, HDC현대산업개발 등' },
    { code: '097950', name: '음식료품', reps: '대표종목: CJ제일제당, 삼양식품, 농심 등' },
    { code: '282330', name: '유통업', reps: '대표종목: BGF리테일, 이마트, 신세계 등' },
    { code: '035420', name: '인터넷/게임', reps: '대표종목: NAVER, 카카오, 크래프톤 등' }
  ];
  
  const results = [];
  let isUpdated = false;
  
  for (const s of sectors) {
    try {
      // 절대 실패하지 않는 주식 현재가 범용 API 사용
      const r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-price', 
        token, 'FHKST01010100', 
        { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: s.code }
      );
      
      if (r?.output?.stck_prpr) {
         results.push({ 
           name: s.name, 
           chg: parseFloat(r.output.prdy_ctrt||0),
           vol: Math.round(parseInt(r.output.acml_tr_pbmn||0) / 100000000), // 원 -> 억 단위
           reps: s.reps // 프론트엔드 툴팁 매핑용 문자열
         });
         isUpdated = true;
      }
    } catch {}
    await sleep(400); 
  }
  
  if (isUpdated) times.krSector = getTimeStr();
  
  if (results.length > 0) {
    results.sort((a, b) => b.chg - a.chg); // 등락률 기준 완벽 정렬 보장
    return results;
  }
  return prev?.sectors || null;
}

// ─────────────────────────────────────────────
// KR 종목 순위 (ETF 완벽 제거 및 개인 수급 포함)
// ─────────────────────────────────────────────
async function fetchKrStocks(token, prev, times) {
  log('📋 KR 순위 종목 수집 중 (KIS)...');
  const prevStocks = prev?.stocks || {};
  let isUpdated = false;

  try {
    const volRes = await kisGet('/uapi/domestic-stock/v1/quotations/volume-rank', token, 'FHPST01710000',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20171', FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0', FID_BLNG_CLS_CODE: '0', FID_TRGT_CLS_CODE: '111111111', FID_TRGT_EXLS_CLS_CODE: '000000', FID_INPUT_PRICE_1: '', FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: '' });
    
    const volumeRaw = volRes?.output || [];
    
    // 1. ETF, ETN, 인버스, 레버리지 완벽 필터링 후 2. 누적거래대금(acml_tr_pbmn) 기준으로 진짜 TOP 10 추출
    const volume = volumeRaw
      .filter(s => !/KODEX|TIGER|KBSTAR|ACE|ARIRANG|HANARO|KOSEF|ETN|선물|인버스|레버리지/i.test(s.hts_kor_isnm))
      .sort((a, b) => parseInt(b.acml_tr_pbmn||0) - parseInt(a.acml_tr_pbmn||0))
      .slice(0, 10)
      .map(s => ({
        name: s.hts_kor_isnm, code: s.mksc_shrn_iscd, 
        chg: parseFloat(s.prdy_ctrt||0), 
        amt: Math.round(parseInt(s.acml_tr_pbmn||0) / 100000000) 
      }));
    
    if(volume.length > 0) isUpdated = true;
    await sleep(800); 

    const fetchRank = async (etcCls, rankSort) => {
      try {
        const r = await kisGet('/uapi/domestic-stock/v1/quotations/foreign-institution-total', token, 'FHPTJ04400000',
          { FID_COND_MRKT_DIV_CODE: 'V', FID_COND_SCR_DIV_CODE: '16449', FID_INPUT_ISCD: '0001', FID_DIV_CLS_CODE: '0', FID_RANK_SORT_CLS_CODE: String(rankSort), FID_ETC_CLS_CODE: String(etcCls) });
        return (r?.output||[]).slice(0,5).map(s => ({ 
          name: s.hts_kor_isnm, code: s.mksc_shrn_iscd, 
          chg: parseFloat(s.prdy_ctrt||0), 
          amt: Math.round(parseInt(s.ntby_tr_pbmn||s.frgn_ntby_tr_pbmn||0) / 100000000) 
        }));
      } catch(e) { return null; }
    };

    // 1:외국인, 2:기관, 3:개인 순차 호출
    const fB = await fetchRank(1, 0); await sleep(800);
    const fS = await fetchRank(1, 1); await sleep(800);
    const iB = await fetchRank(2, 0); await sleep(800);
    const iS = await fetchRank(2, 1); await sleep(800);
    const pB = await fetchRank(3, 0); await sleep(800); // 개인 순매수 상위
    const pS = await fetchRank(3, 1); // 개인 순매도 상위

    if (isUpdated) times.krStock = getTimeStr();
    return {
      volume: volume.length ? volume : prevStocks.volume,
      foreignBuy: fB?.length ? fB : prevStocks.foreignBuy,
      foreignSell:fS?.length ? fS : prevStocks.foreignSell,
      instBuy: iB?.length ? iB : prevStocks.instBuy,
      instSell: iS?.length ? iS : prevStocks.instSell,
      indvBuy: pB?.length ? pB : (prevStocks.indvBuy || null), 
      indvSell: pS?.length ? pS : (prevStocks.indvSell || null),
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

  const finalData = {
    time: kstNow().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' }),
    updateTimes: updateTimes, 
    systemStatus: systemStatus,
    
    kospi: krIndex.kospi, 
    kosdaq: krIndex.kosdaq,
    
    supply: supply || prev.supply, 
    program: program || prev.program, 
    depth: depth || prev.depth, 
    sectors: sectors || prev.sectors, 
    stocks: stocks || prev.stocks,
    
    etf: etf, 
    
    crypto: crypto || prev.crypto,
    
    us: {
      sp500:  pick(us.sp500,  prevUs.sp500),  nasdaq: pick(us.nasdaq, prevUs.nasdaq), 
      dow:    pick(us.dow,    prevUs.dow),    vix:    pick(us.vix,    prevUs.vix),
      tnx:    pick(us.tnx,    prevUs.tnx),    dxy:    pick(us.dxy,    prevUs.dxy),
      spy:    pick(us.spy,    prevUs.spy),    qqq:    pick(us.qqq,    prevUs.qqq),
      tqqq:   pick(us.tqqq,   prevUs.tqqq),   sqqq:   pick(us.sqqq,   prevUs.sqqq)
    },
    
    m7: {
      aapl:  { name: 'Apple', ...pick(us.aapl,  prevM7.aapl) },
      msft:  { name: 'Microsoft', ...pick(us.msft,  prevM7.msft) },
      nvda:  { name: 'Nvidia', ...pick(us.nvda,  prevM7.nvda) },
      amzn:  { name: 'Amazon', ...pick(us.amzn,  prevM7.amzn) },
      googl: { name: 'Alphabet', ...pick(us.googl, prevM7.googl) },
      meta:  { name: 'Meta', ...pick(us.meta,  prevM7.meta) },
      tsla:  { name: 'Tesla', ...pick(us.tsla,  prevM7.tsla) }
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
    ].sort((a, b) => b.chg - a.chg)
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

log('🚀 MONEYLIVE 스케줄러 v5.6 시작');
log(`📁 저장경로: storage/data/ + storage/logs/`);
log('스케줄: 15:00 토큰 / 15:35 / 18:05 / 06:05 / 07:55');

scheduleAt(15,  0, async () => { log('[15:00] 토큰 발급'); await issueToken(); flushLog(); });
scheduleAt(15, 35, () => { log('[15:35] 1차 수집'); collect(); });
scheduleAt(18,  5, () => { log('[18:05] 2차 수집'); collect(); });
scheduleAt( 6,  5, () => { log('[06:05] 3차 수집'); collect(); });
scheduleAt( 7, 55, () => { log('[07:55] 4차 수집 (최종)'); collect(); });

log('📂 시작 시 즉시 수집 (3초 후)');
setTimeout(collect, 3000);
