/**
 * MONEYLIVE 스케줄러 v2
 * 수정사항:
 * - NaN/NaN 버그 수정: 한투 날짜 YYYYMMDD → YYYY-MM-DD 변환
 * - KR 종목 수집 API 경로 수정
 * - Yahoo Finance null 데이터 필터링 강화
 * - 스케줄 중복 실행 방지
 */

const fs    = require('fs');
const path  = require('path');
const https = require('https');

const TOKEN_FILE = path.join(__dirname, 'token.json');
const DATA_FILE  = path.join(__dirname, 'data.json');

const APP_KEY    = process.env.KIS_APP_KEY;
const APP_SECRET = process.env.KIS_APP_SECRET;

// ─────────────────────────────────────────────
// 유틸
// ─────────────────────────────────────────────
function log(msg) {
  const now = new Date().toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' });
  console.log(`[${now}] ${msg}`);
}

function kstNow() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
}

// ★ NaN 수정: YYYYMMDD → YYYY-MM-DD
function parseKisDate(s) {
  if (!s || s.length < 8) return null;
  return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
}

function getDate(offset = 0) {
  const d = kstNow();
  d.setDate(d.getDate() + offset);
  return `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}${String(d.getDate()).padStart(2,'0')}`;
}

function pad(n) { return String(n).padStart(2, '0'); }

// ─────────────────────────────────────────────
// HTTP 요청
// ─────────────────────────────────────────────
function httpPost(hostname, reqPath, body) {
  return new Promise((resolve, reject) => {
    const bodyStr = JSON.stringify(body);
    const req = https.request({
      hostname, port: 9443, path: reqPath, method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr)
      }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON파싱실패: ' + data.slice(0,300))); }
      });
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}

function kisGet(apiPath, token, trId, params = {}) {
  const qs = Object.entries(params)
    .map(([k,v]) => `${k}=${encodeURIComponent(v)}`)
    .join('&');
  const fullPath = qs ? `${apiPath}?${qs}` : apiPath;
  return new Promise((resolve, reject) => {
    https.get({
      hostname: 'openapi.koreainvestment.com',
      port: 9443,
      path: fullPath,
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
        catch (e) { reject(new Error('JSON파싱실패: ' + data.slice(0,300))); }
      });
    }).on('error', reject);
  });
}

// ─────────────────────────────────────────────
// 토큰 관리
// ─────────────────────────────────────────────
function loadToken() {
  try {
    if (!fs.existsSync(TOKEN_FILE)) return null;
    const t = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
    if (Date.now() < new Date(t.expires_at).getTime() - 30 * 60 * 1000) {
      return t.access_token;
    }
    return null;
  } catch { return null; }
}

async function issueToken() {
  if (!APP_KEY || !APP_SECRET) {
    log('❌ 환경변수 KIS_APP_KEY / KIS_APP_SECRET 가 없어요!');
    return null;
  }
  log('🔑 한투 API 토큰 발급 시작...');
  try {
    const res = await httpPost('openapi.koreainvestment.com', '/oauth2/tokenP', {
      grant_type: 'client_credentials',
      appkey: APP_KEY,
      appsecret: APP_SECRET
    });
    if (res.access_token) {
      fs.writeFileSync(TOKEN_FILE, JSON.stringify({
        access_token: res.access_token,
        expires_at: res.access_token_token_expired,
        issued_at: new Date().toISOString()
      }));
      log('✅ 토큰 발급 완료, token.json 저장');
      return res.access_token;
    }
    log('❌ 토큰 발급 실패: ' + JSON.stringify(res).slice(0,200));
    return null;
  } catch (e) {
    log('❌ 토큰 발급 오류: ' + e.message);
    return null;
  }
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
    const reqPath = `/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=7d`;
    https.get({
      hostname: 'query1.finance.yahoo.com',
      path: reqPath,
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
          // null 제거 후 유효한 데이터만 사용
          const pairs = timestamps
            .map((ts, i) => ({ ts, c: closes[i] }))
            .filter(p => p.c != null && !isNaN(p.c));
          if (!pairs.length) return reject(new Error('유효데이터없음'));
          const last = pairs[pairs.length - 1];
          const prev = pairs.length > 1 ? pairs[pairs.length - 2] : null;
          const change = prev ? ((last.c - prev.c) / prev.c * 100) : 0;
          resolve({
            value: last.c,
            change: parseFloat(change.toFixed(2)),
            history: pairs.map((p, i) => ({
              date: new Date(p.ts * 1000).toISOString().split('T')[0],
              close: p.c,
              change: i > 0
                ? parseFloat(((p.c - pairs[i-1].c) / pairs[i-1].c * 100).toFixed(2))
                : 0
            }))
          });
        } catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

// ─────────────────────────────────────────────
// 한국 데이터 수집
// ─────────────────────────────────────────────

async function fetchIndex(token) {
  log('📊 코스피/코스닥 수집 중...');
  const fetchOne = async (iscd, name) => {
    try {
      const r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice',
        token, 'FHKUP03500100',
        { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: iscd,
          FID_INPUT_DATE_1: getDate(-10), FID_INPUT_DATE_2: getDate(0),
          FID_PERIOD_DIV_CODE: 'D' }
      );
      const rows = r?.output2;
      if (!rows?.length) throw new Error('데이터없음');
      const hist = [...rows].reverse(); // 오름차순
      const last = hist[hist.length - 1];
      const prev = hist[hist.length - 2];
      return {
        value: last.bstp_nmix_prpr,
        change: prev
          ? parseFloat(((parseFloat(last.bstp_nmix_prpr) - parseFloat(prev.bstp_nmix_prpr))
              / parseFloat(prev.bstp_nmix_prpr) * 100).toFixed(2))
          : parseFloat(last.bstp_nmix_ctrt || 0),
        history: hist.slice(-5).map((h, i, arr) => ({
          date: parseKisDate(h.stck_bsdt),  // ★ NaN 수정
          close: h.bstp_nmix_prpr,
          change: i > 0
            ? parseFloat(((parseFloat(h.bstp_nmix_prpr) - parseFloat(arr[i-1].bstp_nmix_prpr))
                / parseFloat(arr[i-1].bstp_nmix_prpr) * 100).toFixed(2))
            : parseFloat(h.bstp_nmix_ctrt || 0)
        }))
      };
    } catch (e) {
      log(`❌ ${name} 실패: ${e.message}`);
      return null;
    }
  };

  const [kospi, kosdaq] = await Promise.all([
    fetchOne('0001', '코스피'),
    fetchOne('1001', '코스닥')
  ]);
  log(`  코스피: ${kospi?.value || 'null'} / 코스닥: ${kosdaq?.value || 'null'}`);
  return { kospi, kosdaq };
}

async function fetchSupply(token) {
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
    const history = rows.slice(1, 5).map(h => ({
      date: parseKisDate(h.stck_bsdt),  // ★ NaN 수정
      total: Math.round(
        (parseInt(h.frgn_ntby_qty||0) + parseInt(h.orgn_ntby_qty||0) + parseInt(h.indv_ntby_qty||0))
        / 1000000
      )
    }));
    log(`  외국인: ${today.frgn_ntby_qty} / 기관: ${today.orgn_ntby_qty} / 개인: ${today.indv_ntby_qty}`);
    return {
      foreign:     today.frgn_ntby_qty,
      institution: today.orgn_ntby_qty,
      individual:  today.indv_ntby_qty,
      history
    };
  } catch (e) {
    log('❌ 수급 수집 실패: ' + e.message);
    return null;
  }
}

async function fetchProgram(token) {
  log('🤖 프로그램 매매 수집 중...');
  try {
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-program-trade-by-stock',
      token, 'FHPPG04650100',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: '0001',
        FID_INPUT_DATE_1: getDate(0) }
    );
    const d = r?.output?.[0];
    if (!d) throw new Error('데이터없음');
    return { buyArb: d.arbt_buy_amt, sellArb: d.arbt_sel_amt,
             buyNon: d.nabt_buy_amt, sellNon: d.nabt_sel_amt };
  } catch (e) {
    log('❌ 프로그램 매매 실패: ' + e.message);
    return null;
  }
}

async function fetchDepth(token) {
  log('📉 마켓 뎁스 수집 중...');
  try {
    const r = await kisGet(
      '/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice',
      token, 'FHKUP03500100',
      { FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001',
        FID_INPUT_DATE_1: getDate(0), FID_INPUT_DATE_2: getDate(0),
        FID_PERIOD_DIV_CODE: 'D' }
    );
    const d = r?.output1;
    if (!d) throw new Error('데이터없음');
    return {
      up:  parseInt(d.fsts_nmix_prpr_updt_stck_cnt || 0),
      neu: parseInt(d.fsts_nmix_prpr_same_stck_cnt || 0),
      dn:  parseInt(d.fsts_nmix_prpr_down_stck_cnt || 0)
    };
  } catch (e) {
    log('❌ 마켓 뎁스 실패: ' + e.message);
    return null;
  }
}

async function fetchKrEtf(token) {
  log('🇰🇷 한국 ETF 수집 중...');
  const codes = {
    kodex200: '069500', kodexkosdaq: '229200', kodexlev: '122630',
    kodexinv: '114800', kodexinv2:   '251340', kodexsp:  '379800'
  };
  const results = await Promise.allSettled(
    Object.entries(codes).map(async ([key, code]) => {
      const r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-price',
        token, 'FHKST01010100',
        { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: code }
      );
      const o = r?.output;
      if (!o?.stck_prpr) throw new Error('데이터없음');
      return [key, { value: o.stck_prpr, change: parseFloat(o.prdy_ctrt || 0) }];
    })
  );
  const etf = {};
  results.forEach(r => { if (r.status === 'fulfilled') etf[r.value[0]] = r.value[1]; });
  log(`  ETF ${Object.keys(etf).length}개 수집 완료`);
  return etf;
}

async function fetchKrSectors(token) {
  log('📊 KR 섹터 수집 중...');
  // 대표 종목 주가로 섹터 등락률 산출
  const sectors = [
    { code: '005930', name: '반도체',   tip: '삼성전자, SK하이닉스 등 반도체 제조·설계 기업들이에요. 미국 나스닥과 연동이 강해요.' },
    { code: '207940', name: '제약/바이오', tip: '셀트리온, 삼성바이오로직스 등 제약·바이오 기업들이에요. 임상 결과에 따라 변동성이 커요.' },
    { code: '105560', name: '금융업',   tip: 'KB금융, 신한지주 등 은행·보험·증권 기업들이에요. 금리 변화에 민감해요.' },
    { code: '005380', name: '자동차',   tip: '현대차, 기아 등 자동차 제조 기업들이에요. 환율과 글로벌 수요에 영향을 받아요.' },
    { code: '051910', name: '화학',     tip: 'LG화학, SK이노베이션 등 석유화학·소재 기업들이에요.' },
    { code: '373220', name: '2차전지',  tip: '삼성SDI, LG에너지솔루션 등 배터리 기업들이에요. 전기차 시장과 연동돼요.' },
    { code: '005490', name: '철강/금속', tip: 'POSCO, 현대제철 등 철강·금속 기업들이에요. 원자재 가격과 연동돼요.' },
    { code: '000720', name: '건설업',   tip: '현대건설, GS건설 등 건설 기업들이에요. 부동산 경기에 민감해요.' },
    { code: '066570', name: '전기전자', tip: 'LG전자, 삼성전기 등 전자·전기 기업들이에요.' },
    { code: '097950', name: '음식료',   tip: 'CJ제일제당, 오리온 등 식품 기업들이에요. 경기 방어주 성격이 강해요.' },
    { code: '282330', name: '유통',     tip: '롯데쇼핑, 이마트 등 유통 기업들이에요.' }
  ];

  const results = await Promise.allSettled(
    sectors.map(async (s) => {
      const r = await kisGet(
        '/uapi/domestic-stock/v1/quotations/inquire-price',
        token, 'FHKST01010100',
        { FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: s.code }
      );
      const o = r?.output;
      if (!o?.stck_prpr) throw new Error('데이터없음');
      return {
        name: s.name, tip: s.tip,
        chg: parseFloat(o.prdy_ctrt || 0),
        vol: parseInt(o.acml_tr_pbmn || 0)
      };
    })
  );
  const list = results.filter(r => r.status === 'fulfilled').map(r => r.value);
  log(`  섹터 ${list.length}개 수집 완료`);
  return list;
}

async function fetchKrStocks(token) {
  log('📋 KR 수급 종목 수집 중...');
  try {
    // ★ 거래대금 상위 API
    const volRes = await kisGet(
      '/uapi/domestic-stock/v1/ranking/trading-value',
      token, 'FHPST01710000',
      { FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20171',
        FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0',
        FID_BLNG_CLS_CODE: '0', FID_TRGT_CLS_CODE: '111111111',
        FID_TRGT_EXLS_CLS_CODE: '000000', FID_INPUT_PRICE_1: '',
        FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: '' }
    );
    const volume = (volRes?.output || []).slice(0, 10).map(s => ({
      name: s.hts_kor_isnm, code: s.mksc_shrn_iscd,
      chg: parseFloat(s.prdy_ctrt || 0), amt: parseInt(s.acml_tr_pbmn || 0)
    }));

    // ★ 수급 랭킹 API (투자자별)
    const fetchRank = async (blng) => {
      try {
        const r = await kisGet(
          '/uapi/domestic-stock/v1/ranking/investor',
          token, 'FHPST01720000',
          { FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20172',
            FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0',
            FID_BLNG_CLS_CODE: blng, FID_TRGT_CLS_CODE: '111111111',
            FID_TRGT_EXLS_CLS_CODE: '000000', FID_INPUT_PRICE_1: '',
            FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: '' }
        );
        return (r?.output || []).slice(0, 5).map(s => ({
          name: s.hts_kor_isnm, code: s.mksc_shrn_iscd,
          chg: parseFloat(s.prdy_ctrt || 0), amt: parseInt(s.ntby_qty || 0)
        }));
      } catch (e) {
        log(`  ⚠️ 수급랭킹(${blng}) 실패: ${e.message}`);
        return [];
      }
    };

    const [foreignBuy, foreignSell, instBuy, instSell, indvBuy, indvSell] =
      await Promise.all([1,2,3,4,5,6].map(n => fetchRank(String(n))));

    log(`  거래대금 ${volume.length}개 / 외국인매수 ${foreignBuy.length}개 수집 완료`);
    return { volume, foreignBuy, foreignSell, instBuy, instSell, indvBuy, indvSell };
  } catch (e) {
    log('❌ KR 종목 수집 실패: ' + e.message);
    return null;
  }
}

async function fetchUsData() {
  log('🇺🇸 미국 데이터 수집 중...');
  const symbols = {
    sp500: '^GSPC', nasdaq: '^IXIC', dow: '^DJI',
    vix:   '^VIX',  tnx:   '^TNX',  dxy: 'DX-Y.NYB',
    spy:   'SPY',   qqq:   'QQQ',   tqqq: 'TQQQ', sqqq: 'SQQQ',
    aapl:  'AAPL',  msft:  'MSFT',  nvda: 'NVDA',
    amzn:  'AMZN',  googl: 'GOOGL', meta: 'META', tsla: 'TSLA',
    ewy:   'EWY',   koru:  'KORU',  korz: 'KORZ',
    wti:   'CL=F',  gold:  'GC=F',  copper: 'HG=F',
    xlk:   'XLK',   xlc:   'XLC',   xly:  'XLY',
    xlf:   'XLF',   xlv:   'XLV',   xli:  'XLI',
    xlp:   'XLP',   xlre:  'XLRE',  xlu:  'XLU',
    xlb:   'XLB',   xle:   'XLE'
  };
  const results = {};
  await Promise.allSettled(
    Object.entries(symbols).map(async ([key, sym]) => {
      try { results[key] = await fetchYahoo(sym); }
      catch (e) { log(`  ⚠️ ${sym} 실패: ${e.message}`); }
    })
  );
  log(`  미국 데이터 ${Object.keys(results).length}/${Object.keys(symbols).length}개 완료`);
  return results;
}

// ─────────────────────────────────────────────
// 전체 수집 & data.json 저장
// ─────────────────────────────────────────────
async function collect() {
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  log('📡 데이터 수집 시작');

  const token = await getToken();
  if (!token) { log('❌ 토큰 없음 - 수집 중단'); return; }

  const [{ kospi, kosdaq }, supply, program, depth, etf, sectors, stocks, us] =
    await Promise.all([
      fetchIndex(token), fetchSupply(token), fetchProgram(token),
      fetchDepth(token), fetchKrEtf(token), fetchKrSectors(token),
      fetchKrStocks(token), fetchUsData()
    ]);

  const now  = kstNow();
  const DAYS = ['일','월','화','수','목','금','토'];

  const pick = (obj) => obj ? { value: obj.value, change: obj.change } : null;

  const data = {
    updatedAt: now.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' }),
    dataDate:  `${String(now.getFullYear()).slice(2)}.${String(now.getMonth()+1).padStart(2,'0')}.${String(now.getDate()).padStart(2,'0')} (${DAYS[now.getDay()]}) 기준`,

    kospi, kosdaq, supply, program, depth, etf, sectors, stocks,

    us: {
      sp500: pick(us.sp500),  nasdaq: pick(us.nasdaq), dow:  pick(us.dow),
      vix:   pick(us.vix),    tnx:   pick(us.tnx),    dxy:  pick(us.dxy),
      spy:   pick(us.spy),    qqq:   pick(us.qqq),    tqqq: pick(us.tqqq),
      sqqq:  pick(us.sqqq)
    },
    m7: {
      aapl: pick(us.aapl), msft: pick(us.msft), nvda:  pick(us.nvda),
      amzn: pick(us.amzn), googl:pick(us.googl),meta:  pick(us.meta),
      tsla: pick(us.tsla)
    },
    usSectors: [
      { name: 'IT',        chg: us.xlk?.change  || 0 },
      { name: '커뮤니케이션', chg: us.xlc?.change  || 0 },
      { name: '임의소비재',  chg: us.xly?.change  || 0 },
      { name: '금융',       chg: us.xlf?.change  || 0 },
      { name: '헬스케어',   chg: us.xlv?.change  || 0 },
      { name: '산업재',     chg: us.xli?.change  || 0 },
      { name: '필수소비재',  chg: us.xlp?.change  || 0 },
      { name: '부동산',     chg: us.xlre?.change || 0 },
      { name: '유틸리티',   chg: us.xlu?.change  || 0 },
      { name: '소재',       chg: us.xlb?.change  || 0 },
      { name: '에너지',     chg: us.xle?.change  || 0 }
    ],
    koreaEtf: {
      ewy:  pick(us.ewy),  koru: pick(us.koru), korz: pick(us.korz)
    },
    commodities: {
      wti: pick(us.wti), gold: pick(us.gold), copper: pick(us.copper)
    }
  };

  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  log('✅ data.json 저장 완료');
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
}

// ─────────────────────────────────────────────
// 스케줄러 (중복 실행 방지)
// ─────────────────────────────────────────────
function scheduleAt(hour, minute, job) {
  let lastRunDay = -1;
  setInterval(() => {
    const now = kstNow();
    const today = now.getFullYear() * 10000 + now.getMonth() * 100 + now.getDate();
    if (now.getHours() === hour && now.getMinutes() === minute && lastRunDay !== today) {
      lastRunDay = today;
      job();
    }
  }, 30000); // 30초마다 체크
  log(`🕐 스케줄 등록: KST ${pad(hour)}:${pad(minute)}`);
}

log('🚀 MONEYLIVE 스케줄러 v2 시작');
log('스케줄: 15:00 토큰발급 / 15:35 / 18:05 / 06:05 / 07:55 수집');

scheduleAt(15,  0, async () => { log('[15:00] 토큰 발급'); await issueToken(); });
scheduleAt(15, 35, () => { log('[15:35] 1차 수집'); collect(); });
scheduleAt(18,  5, () => { log('[18:05] 2차 수집'); collect(); });
scheduleAt( 6,  5, () => { log('[06:05] 3차 수집'); collect(); });
scheduleAt( 7, 55, () => { log('[07:55] 4차 수집'); collect(); });

// 시작 시 data.json 없으면 즉시 수집
if (!fs.existsSync(DATA_FILE)) {
  log('📂 data.json 없음 → 3초 후 즉시 수집');
  setTimeout(collect, 3000);
} else {
  log('📂 data.json 존재 → 스케줄 대기 중');
}
