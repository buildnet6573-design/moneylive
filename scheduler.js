/**
 * MONEYLIVE 스케줄러 v5
 * 수정사항:
 * - 전날 마감 데이터 강제 수집 (수급/종목/섹터 전날 기준)
 * - 날짜별 데이터 저장 /app/storage/data/YYYY-MM-DD.json
 * - 날짜별 로그 저장 /app/storage/logs/YYYY-MM-DD.log
 * - output1(현재가) + output2(히스토리) 분리
 * - 이전 데이터 유지 로직
 * - Yahoo 날짜 한국시간 변환
 */

const fs    = require('fs');
const path  = require('path');
const https = require('https');

// 저장 경로
const STORAGE_DIR = path.join(__dirname, 'storage');
const DATA_DIR    = path.join(STORAGE_DIR, 'data');
const LOG_DIR     = path.join(STORAGE_DIR, 'logs');
const DATA_FILE   = path.join(__dirname, 'data.json'); // 최신본 (프론트용)
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

function parseKisDate(s) {
  if (!s || s.length < 8) return null;
  return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
}

function pad(n) { return String(n).padStart(2, '0'); }

function loadPrevData() {
  try {
    if (!fs.existsSync(DATA_FILE)) return {};
    return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  } catch { return {}; }
}

function saveData(data) {
  // 최신본 저장
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  // 날짜별 저장
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
// Yahoo Finance
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
          const result = j?.chart?.result?.[0];
          if (!result) return reject(new Error('결과없음'));
          const closes = result.indicators.quote[0].close;
          const timestamps = result.timestamp;
          const pairs = timestamps.map((ts,i) => ({ts, c:closes[i]})).filter(p => p.c!=null && !isNaN(p.c));
          if (!pairs.length) return reject(new Error('유효데이터없음'));
          const last = pairs[pairs.length-1];
          const prev = pairs.length > 1 ? pairs[pairs.length-2] : null;
          const change = prev ? ((last.c - prev.c) / prev.c * 100) : 0;
          // 한국시간 변환
          const toKst = (ts) => {
            const d = new Date(ts*1000);
            const kst = new Date(d.toLocaleString('en-US', {timeZone:'Asia/Seoul'}));
            return `${kst.getFullYear()}-${String(kst.getMonth()+1).padStart(2,'0')}-${String(kst.getDate()).padStart(2,'0')}`;
          };
          resolve({
            value: last.c,
            change: parseFloat(change.toFixed(2)),
            history: pairs.map((p,i) => ({
              date: toKst(p.ts),
              close: p.c,
              change: i > 0 ? parseFloat(((p.c-pairs[i-1].c)/pairs[i-1].c*100).toFixed(2)) : 0
            }))
          });
        } catch(e) { reject(e); }
      });
    }).on('error', reject);
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
      const o2 = r?.output2;
      const currentPrice = o1?.bstp_nmix_prpr;
      if (!currentPrice) throw new Error('현재가 없음');
      let history = [];
      if (o2?.length) {
        const hist = [...o2].reverse();
        history = hist.slice(-5).map((h, i, arr) => ({
          date: parseKisDate(h.stck_bsop_date || h.stck_bsdt),
          close: h.bstp_nmix_prpr,
          change: i > 0
            ? parseFloat(((parseFloat(h.bstp_nmix_prpr)-parseFloat(arr[i-1].bstp_nmix_prpr))/parseFloat(arr[i-1].bstp_nmix_prpr)*100).toFixed(2))
            : parseFloat(h.bstp_nmix_ctrt||0)
        }));
      }
      return { value: currentPrice, change: parseFloat(o1?.prdy_ctrt||0), history };
    } catch(e) {
      log(`  ⚠️ ${name} 실패→이전유지: ${e.message}`);
      return prevVal || null;
    }
  };
  const [kospi, kosdaq] = await Promise.all([
    fetchOne('0001', '코스피', prev.kospi),
    fetchOne('1001', '코스닥', prev.kosdaq)
  ]);
  log(`  코스피: ${kospi?.value||'이전값'} / 코스닥: ${kosdaq?.value||'이전값'}`);
  return { kospi, kosdaq };
}

async function fetchSupply(token, prev) {
  log('💰 수급 동향 수집 중...');
  try {
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-investor',
      token, 'FHKST01010900',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: '0001' }
    );
    const rows = r?.output;
    if (!rows?.length) throw new Error('데이터없음');
    const today = rows[0];
    const f    = parseInt(today.frgn_ntby_qty||0);
    const inst = parseInt(today.orgn_ntby_qty||0);
    const ind  = parseInt(today.indv_ntby_qty||0);
    if (f===0 && inst===0 && ind===0 && prev.supply) {
      log('  ⚠️ 수급 모두 0 → 이전 데이터 유지');
      return prev.supply;
    }
    const history = rows.slice(1,5).map(h => ({
      date: parseKisDate(h.stck_bsdt),
      total: Math.round((parseInt(h.frgn_ntby_qty||0)+parseInt(h.orgn_ntby_qty||0)+parseInt(h.indv_ntby_qty||0))/1000000)
    }));
    log(`  외국인: ${f} / 기관: ${inst} / 개인: ${ind}`);
    return { foreign: today.frgn_ntby_qty, institution: today.orgn_ntby_qty, individual: today.indv_ntby_qty, history };
  } catch(e) {
    log(`  ⚠️ 수급 실패→이전유지: ${e.message}`);
    return prev.supply || null;
  }
}

async function fetchProgram(token, prev) {
  log('🤖 프로그램 매매 수집 중...');
  try {
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-program-trade-by-stock',
      token, 'FHPPG04650100',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: '0001',
        FID_INPUT_DATE_1: getDate(-1) }
    );
    const d = r?.output?.[0];
    if (!d) throw new Error('데이터없음');
    if (parseInt(d.arbt_buy_amt||0)===0 && parseInt(d.nabt_buy_amt||0)===0 && prev.program) {
      log('  ⚠️ 프로그램 매매 0 → 이전 데이터 유지');
      return prev.program;
    }
    return { buyArb: d.arbt_buy_amt, sellArb: d.arbt_sel_amt, buyNon: d.nabt_buy_amt, sellNon: d.nabt_sel_amt };
  } catch(e) {
    log(`  ⚠️ 프로그램매매 실패→이전유지: ${e.message}`);
    return prev.program || null;
  }
}

async function fetchDepth(token, prev) {
  log('📉 마켓 뎁스 수집 중...');
  try {
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice',
      token, 'FHKUP03500100',
      { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001',
        FID_INPUT_DATE_1: getDate(-1), FID_INPUT_DATE_2: getDate(0),
        FID_PERIOD_DIV_CODE: 'D' }
    );
    const rows = r?.output2;
    if (!rows?.length) throw new Error('데이터없음');
    const sorted = [...rows].reverse();
    const last = sorted[sorted.length-1];
    const up  = parseInt(last.fsts_nmix_prpr_updt_stck_cnt||0);
    const neu = parseInt(last.fsts_nmix_prpr_same_stck_cnt||0);
    const dn  = parseInt(last.fsts_nmix_prpr_down_stck_cnt||0);
    if (up+neu+dn===0 && prev.depth) { log('  ⚠️ 뎁스 0 → 이전유지'); return prev.depth; }
    log(`  상승:${up} / 보합:${neu} / 하락:${dn}`);
    return { up, neu, dn };
  } catch(e) {
    log(`  ⚠️ 마켓뎁스 실패→이전유지: ${e.message}`);
    return prev.depth || null;
  }
}

async function fetchKrEtf(token, prev) {
  log('🇰🇷 한국 ETF 수집 중...');
  const codes = {
    kodex200: '069500', kodexkosdaq: '229200', kodexlev: '122630',
    kodexinv: '114800', kodexinv2: '251340', kodexsp: '379800'
  };
  const etf = { ...(prev.etf||{}) };
  await Promise.allSettled(Object.entries(codes).map(async ([key, code]) => {
    try {
      const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-price', token, 'FHKST01010100',
        { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: code });
      const o = r?.output;
      if (!o?.stck_prpr) throw new Error('데이터없음');
      etf[key] = { value: o.stck_prpr, change: parseFloat(o.prdy_ctrt||0) };
    } catch { log(`  ⚠️ ETF ${key} 실패→이전유지`); }
  }));
  log(`  ETF ${Object.keys(etf).length}개 완료`);
  return etf;
}

async function fetchKrSectors(token, prev) {
  log('📊 KR 섹터 수집 중...');
  const sectors = [
    { code: '005930', name: '반도체',    tip: '삼성전자, SK하이닉스 등 반도체 제조·설계 기업들이에요.' },
    { code: '207940', name: '제약/바이오', tip: '셀트리온, 삼성바이오로직스 등 제약·바이오 기업들이에요.' },
    { code: '105560', name: '금융업',    tip: 'KB금융, 신한지주 등 은행·보험·증권 기업들이에요.' },
    { code: '005380', name: '자동차',    tip: '현대차, 기아 등 자동차 제조 기업들이에요.' },
    { code: '051910', name: '화학',      tip: 'LG화학, SK이노베이션 등 석유화학·소재 기업들이에요.' },
    { code: '373220', name: '2차전지',   tip: '삼성SDI, LG에너지솔루션 등 배터리 기업들이에요.' },
    { code: '005490', name: '철강/금속', tip: 'POSCO, 현대제철 등 철강·금속 기업들이에요.' },
    { code: '000720', name: '건설업',    tip: '현대건설, GS건설 등 건설 기업들이에요.' },
    { code: '066570', name: '전기전자',  tip: 'LG전자, 삼성전기 등 전자·전기 기업들이에요.' },
    { code: '097950', name: '음식료',    tip: 'CJ제일제당, 오리온 등 식품 기업들이에요.' },
    { code: '282330', name: '유통',      tip: '롯데쇼핑, 이마트 등 유통 기업들이에요.' }
  ];
  const results = await Promise.allSettled(sectors.map(async s => {
    const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-price', token, 'FHKST01010100',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: s.code });
    const o = r?.output;
    if (!o?.stck_prpr) throw new Error('데이터없음');
    return { name: s.name, tip: s.tip, chg: parseFloat(o.prdy_ctrt||0), vol: parseInt(o.acml_tr_pbmn||0) };
  }));
  const succeeded = results.filter(r => r.status==='fulfilled').map(r => r.value);
  if (succeeded.length > 0) { log(`  섹터 ${succeeded.length}개 완료`); return succeeded; }
  log('  ⚠️ 섹터 실패 → 이전유지');
  return prev.sectors || null;
}

async function fetchKrStocks(token, prev) {
  log('📋 KR 수급 종목 수집 중...');
  const prevStocks = prev.stocks || {};
  try {
    const volRes = await kisGet('/uapi/domestic-stock/v1/ranking/trading-value', token, 'FHPST01710000',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20171',
        FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0', FID_BLNG_CLS_CODE: '0',
        FID_TRGT_CLS_CODE: '111111111', FID_TRGT_EXLS_CLS_CODE: '000000',
        FID_INPUT_PRICE_1: '', FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: '' });
    const volume = (volRes?.output||[]).slice(0,10).map(s => ({
      name: s.hts_kor_isnm, code: s.mksc_shrn_iscd,
      chg: parseFloat(s.prdy_ctrt||0), amt: parseInt(s.acml_tr_pbmn||0)
    }));

    const fetchRank = async (blng) => {
      try {
        const r = await kisGet('/uapi/domestic-stock/v1/ranking/investor', token, 'FHPST01720000',
          { FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20172',
            FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0', FID_BLNG_CLS_CODE: blng,
            FID_TRGT_CLS_CODE: '111111111', FID_TRGT_EXLS_CLS_CODE: '000000',
            FID_INPUT_PRICE_1: '', FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: '' });
        return (r?.output||[]).slice(0,5).map(s => ({
          name: s.hts_kor_isnm, code: s.mksc_shrn_iscd,
          chg: parseFloat(s.prdy_ctrt||0), amt: parseInt(s.ntby_qty||0)
        }));
      } catch { return null; }
    };

    const [fB,fS,iB,iS,dB,dS] = await Promise.all([1,2,3,4,5,6].map(n => fetchRank(String(n))));
    const result = {
      volume:     volume.length ? volume  : prevStocks.volume,
      foreignBuy: fB?.length   ? fB      : prevStocks.foreignBuy,
      foreignSell:fS?.length   ? fS      : prevStocks.foreignSell,
      instBuy:    iB?.length   ? iB      : prevStocks.instBuy,
      instSell:   iS?.length   ? iS      : prevStocks.instSell,
      indvBuy:    dB?.length   ? dB      : prevStocks.indvBuy,
      indvSell:   dS?.length   ? dS      : prevStocks.indvSell,
    };
    log(`  거래대금 ${result.volume?.length||0}개 / 외국인매수 ${result.foreignBuy?.length||0}개 완료`);
    return result;
  } catch(e) {
    log(`  ⚠️ KR종목 실패→이전유지: ${e.message}`);
    return prevStocks || null;
  }
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
    try { results[key] = await fetchYahoo(sym); }
    catch(e) { log(`  ⚠️ ${sym} 실패: ${e.message}`); }
  }));
  log(`  미국 ${Object.keys(results).length}/${Object.keys(symbols).length}개 완료`);
  return results;
}

// ─────────────────────────────────────────────
// 전체 수집
// ─────────────────────────────────────────────
async function collect() {
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  log('📡 데이터 수집 시작');
  const token = await getToken();
  if (!token) { log('❌ 토큰 없음 - 수집 중단'); flushLog(); return; }

  const prev = loadPrevData();
  log(`  이전 데이터: ${Object.keys(prev).length > 0 ? '로드됨' : '없음'}`);

  const [{kospi,kosdaq}, supply, program, depth, etf, sectors, stocks, us] = await Promise.all([
    fetchIndex(token, prev), fetchSupply(token, prev), fetchProgram(token, prev),
    fetchDepth(token, prev), fetchKrEtf(token, prev), fetchKrSectors(token, prev),
    fetchKrStocks(token, prev), fetchUsData(prev)
  ]);

  const now  = kstNow();
  const DAYS = ['일','월','화','수','목','금','토'];
  const pick = (newVal, prevVal) => newVal ? { value: newVal.value, change: newVal.change } : (prevVal||null);
  const prevUs    = prev.us    || {};
  const prevM7    = prev.m7    || {};
  const prevUsSec = prev.usSectors   || [];
  const prevKrEtf = prev.koreaEtf   || {};
  const prevCom   = prev.commodities || {};
  const findSec   = (name) => prevUsSec.find(s => s.name===name)?.chg ?? 0;

  // 수집 시각 상세 (날짜+요일+시간)
  const updatedAt = `${now.getMonth()+1}월 ${now.getDate()}일 (${DAYS[now.getDay()]}) ${pad(now.getHours())}:${pad(now.getMinutes())} 기준`;

  const data = {
    updatedAt,
    dataDate: `${String(now.getFullYear()).slice(2)}.${String(now.getMonth()+1).padStart(2,'0')}.${String(now.getDate()).padStart(2,'0')} (${DAYS[now.getDay()]}) 기준`,
    collectedAt: now.toISOString(),
    kospi, kosdaq, supply, program, depth, etf, sectors, stocks,
    us: {
      sp500:  pick(us.sp500,  prevUs.sp500),  nasdaq: pick(us.nasdaq, prevUs.nasdaq),
      dow:    pick(us.dow,    prevUs.dow),    vix:    pick(us.vix,    prevUs.vix),
      tnx:    pick(us.tnx,    prevUs.tnx),    dxy:    pick(us.dxy,    prevUs.dxy),
      spy:    pick(us.spy,    prevUs.spy),    qqq:    pick(us.qqq,    prevUs.qqq),
      tqqq:   pick(us.tqqq,   prevUs.tqqq),   sqqq:   pick(us.sqqq,   prevUs.sqqq)
    },
    m7: {
      aapl:  pick(us.aapl,  prevM7.aapl),  msft:  pick(us.msft,  prevM7.msft),
      nvda:  pick(us.nvda,  prevM7.nvda),  amzn:  pick(us.amzn,  prevM7.amzn),
      googl: pick(us.googl, prevM7.googl), meta:  pick(us.meta,  prevM7.meta),
      tsla:  pick(us.tsla,  prevM7.tsla)
    },
    usSectors: [
      { name:'IT',        chg: us.xlk?.change  ?? findSec('IT') },
      { name:'커뮤니케이션', chg: us.xlc?.change  ?? findSec('커뮤니케이션') },
      { name:'임의소비재',  chg: us.xly?.change  ?? findSec('임의소비재') },
      { name:'금융',       chg: us.xlf?.change  ?? findSec('금융') },
      { name:'헬스케어',   chg: us.xlv?.change  ?? findSec('헬스케어') },
      { name:'산업재',     chg: us.xli?.change  ?? findSec('산업재') },
      { name:'필수소비재',  chg: us.xlp?.change  ?? findSec('필수소비재') },
      { name:'부동산',     chg: us.xlre?.change ?? findSec('부동산') },
      { name:'유틸리티',   chg: us.xlu?.change  ?? findSec('유틸리티') },
      { name:'소재',       chg: us.xlb?.change  ?? findSec('소재') },
      { name:'에너지',     chg: us.xle?.change  ?? findSec('에너지') }
    ],
    koreaEtf: {
      ewy:  pick(us.ewy,  prevKrEtf.ewy),
      koru: pick(us.koru, prevKrEtf.koru),
      korz: pick(us.korz, prevKrEtf.korz)
    },
    commodities: {
      wti:    pick(us.wti,    prevCom.wti),
      gold:   pick(us.gold,   prevCom.gold),
      copper: pick(us.copper, prevCom.copper)
    }
  };

  saveData(data);
  log(`✅ data.json 저장 완료 → storage/data/${getDateStr(0)}.json`);
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

log('🚀 MONEYLIVE 스케줄러 v5 시작');
log(`📁 저장경로: storage/data/ + storage/logs/`);
log('스케줄: 15:00 토큰 / 15:35 / 18:05 / 06:05 / 07:55');

scheduleAt(15,  0, async () => { log('[15:00] 토큰 발급'); await issueToken(); flushLog(); });
scheduleAt(15, 35, () => { log('[15:35] 1차 수집'); collect(); });
scheduleAt(18,  5, () => { log('[18:05] 2차 수집'); collect(); });
scheduleAt( 6,  5, () => { log('[06:05] 3차 수집'); collect(); });
scheduleAt( 7, 55, () => { log('[07:55] 4차 수집 (최종)'); collect(); });

log('📂 시작 시 즉시 수집 (3초 후)');
setTimeout(collect, 3000);
