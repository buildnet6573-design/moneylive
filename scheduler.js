/**
 * MONEYLIVE 스케줄러
 *
 * 핵심 원칙:
 * - 한투 API 토큰은 매일 15:00 KST 에 딱 한 번만 발급
 * - 발급된 토큰은 token.json 에 저장해 24시간 재사용
 * - 중복 발급 시 한투에서 차단될 수 있으므로 절대 금지
 *
 * 수집 스케줄 (KST):
 * 15:00 → 토큰 발급
 * 15:35 → 1차 수집 (한국 정규장 종가)
 * 18:05 → 2차 수집 (시간외 포함 최종)
 * 06:05 → 3차 수집 (미국 정규장 종료 후)
 * 07:55 → 4차 수집 (최종 - 미국 종가 + 전체)
 */

const fs   = require('fs');
const path = require('path');
const https = require('https');

const TOKEN_FILE = path.join(__dirname, 'token.json');
const DATA_FILE  = path.join(__dirname, 'data.json');

const KIS_BASE   = 'https://openapi.koreainvestment.com:9443';
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

function httpRequest(options, body = null) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON 파싱 실패: ' + data.slice(0, 200))); }
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

function kisGet(path, headers, params = {}) {
  const qs = new URLSearchParams(params).toString();
  const fullPath = qs ? `${path}?${qs}` : path;
  return httpRequest({
    hostname: 'openapi.koreainvestment.com',
    port: 9443,
    path: fullPath,
    method: 'GET',
    headers: { 'Content-Type': 'application/json; charset=utf-8', ...headers }
  });
}

// ─────────────────────────────────────────────
// 토큰 관리 (하루 1회 발급, 파일에 저장)
// ─────────────────────────────────────────────
function loadToken() {
  try {
    if (!fs.existsSync(TOKEN_FILE)) return null;
    const t = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
    // 만료 30분 전까지만 유효
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
    const res = await httpRequest({
      hostname: 'openapi.koreainvestment.com',
      port: 9443,
      path: '/oauth2/tokenP',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    }, {
      grant_type: 'client_credentials',
      appkey: APP_KEY,
      appsecret: APP_SECRET
    });

    if (res.access_token) {
      fs.writeFileSync(TOKEN_FILE, JSON.stringify({
        access_token: res.access_token,
        expires_at: res.access_token_token_expired, // 한투 응답의 만료 일시
        issued_at: new Date().toISOString()
      }));
      log('✅ 토큰 발급 완료, token.json 저장');
      return res.access_token;
    } else {
      log('❌ 토큰 발급 실패: ' + JSON.stringify(res));
      return null;
    }
  } catch (e) {
    log('❌ 토큰 발급 오류: ' + e.message);
    return null;
  }
}

async function getToken() {
  const cached = loadToken();
  if (cached) { log('♻️ 기존 토큰 재사용'); return cached; }
  return await issueToken();
}

// ─────────────────────────────────────────────
// 한투 API 호출 헬퍼
// ─────────────────────────────────────────────
function kisHeaders(token, trId) {
  return {
    'authorization': `Bearer ${token}`,
    'appkey': APP_KEY,
    'appsecret': APP_SECRET,
    'tr_id': trId,
    'custtype': 'P'
  };
}

// ─────────────────────────────────────────────
// Yahoo Finance (서버에서 직접 호출)
// ─────────────────────────────────────────────
function fetchYahoo(symbol) {
  return new Promise((resolve, reject) => {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=5d`;
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(data);
          const meta = j.chart.result[0].meta;
          const closes = j.chart.result[0].indicators.quote[0].close;
          const timestamps = j.chart.result[0].timestamp;
          const lastClose = closes[closes.length - 1];
          const prevClose = closes[closes.length - 2] || meta.chartPreviousClose;
          const change = prevClose ? ((lastClose - prevClose) / prevClose * 100) : 0;
          resolve({
            value: lastClose,
            change: parseFloat(change.toFixed(2)),
            history: timestamps.map((ts, i) => ({
              date: new Date(ts * 1000).toISOString().split('T')[0],
              close: closes[i],
              change: i > 0 && closes[i-1] ? ((closes[i] - closes[i-1]) / closes[i-1] * 100).toFixed(2) : 0
            }))
          });
        } catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

// ─────────────────────────────────────────────
// 데이터 수집 함수들
// ─────────────────────────────────────────────

// 코스피/코스닥 지수
async function fetchIndex(token) {
  log('📊 코스피/코스닥 수집 중...');
  try {
    const [kospi, kosdaq] = await Promise.all([
      kisGet('/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice', kisHeaders(token, 'FHKUP03500100'), {
        FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '0001',
        FID_INPUT_DATE_1: getDate(-5), FID_INPUT_DATE_2: getDate(0), FID_PERIOD_DIV_CODE: 'D'
      }),
      kisGet('/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice', kisHeaders(token, 'FHKUP03500100'), {
        FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: '1001',
        FID_INPUT_DATE_1: getDate(-5), FID_INPUT_DATE_2: getDate(0), FID_PERIOD_DIV_CODE: 'D'
      })
    ]);

    const parseIdx = (r) => {
      if (!r?.output2?.length) return null;
      const hist = r.output2.slice().reverse();
      const last = hist[hist.length - 1];
      const prev = hist[hist.length - 2];
      return {
        value: last.bstp_nmix_prpr,
        change: prev ? ((last.bstp_nmix_prpr - prev.bstp_nmix_prpr) / prev.bstp_nmix_prpr * 100).toFixed(2) : 0,
        history: hist.map(h => ({ date: h.stck_bsdt, close: h.bstp_nmix_prpr, change: h.bstp_nmix_ctrt }))
      };
    };

    return { kospi: parseIdx(kospi), kosdaq: parseIdx(kosdaq) };
  } catch (e) {
    log('❌ 지수 수집 실패: ' + e.message);
    return { kospi: null, kosdaq: null };
  }
}

// 수급 동향 (외국인/기관/개인)
async function fetchSupply(token) {
  log('💰 수급 동향 수집 중...');
  try {
    const res = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-investor', kisHeaders(token, 'FHKST01010900'), {
      FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: '0001'
    });
    if (!res?.output?.length) return null;

    const today = res.output[0];
    const history = res.output.slice(1, 5).map(h => ({
      date: h.stck_bsdt,
      total: Math.round((parseInt(h.frgn_ntby_qty||0) + parseInt(h.orgn_ntby_qty||0) + parseInt(h.indv_ntby_qty||0)) / 1000000)
    }));

    return {
      foreign: today.frgn_ntby_qty,
      institution: today.orgn_ntby_qty,
      individual: today.indv_ntby_qty,
      history
    };
  } catch (e) {
    log('❌ 수급 수집 실패: ' + e.message);
    return null;
  }
}

// 프로그램 매매
async function fetchProgram(token) {
  log('🤖 프로그램 매매 수집 중...');
  try {
    const res = await kisGet('/uapi/domestic-stock/v1/quotations/program-trade-by-stock', kisHeaders(token, 'FHPPG04650100'), {
      FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: '0001', FID_INPUT_DATE_1: getDate(0)
    });
    if (!res?.output?.length) return null;
    const d = res.output[0];
    return {
      buyArb:  d.arbt_buy_amt,
      sellArb: d.arbt_sel_amt,
      buyNon:  d.nabt_buy_amt,
      sellNon: d.nabt_sel_amt
    };
  } catch (e) {
    log('❌ 프로그램 매매 수집 실패: ' + e.message);
    return null;
  }
}

// 마켓 뎁스 (상승/보합/하락 종목 수)
async function fetchDepth(token) {
  log('📉 마켓 뎁스 수집 중...');
  try {
    const res = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice', kisHeaders(token, 'FHKST03010100'), {
      FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: '0001',
      FID_INPUT_DATE_1: getDate(0), FID_INPUT_DATE_2: getDate(0), FID_PERIOD_DIV_CODE: 'D', FID_ORG_ADJ_PRC: '0'
    });
    if (!res?.output1) return null;
    return {
      up:  parseInt(res.output1.fsts_nmix_prpr_updt_stck_cnt || 0),
      neu: parseInt(res.output1.fsts_nmix_prpr_same_stck_cnt || 0),
      dn:  parseInt(res.output1.fsts_nmix_prpr_down_stck_cnt || 0)
    };
  } catch (e) {
    log('❌ 마켓 뎁스 수집 실패: ' + e.message);
    return null;
  }
}

// 한국 ETF
async function fetchKrEtf(token) {
  log('🇰🇷 한국 ETF 수집 중...');
  const codes = {
    kodex200:    '069500',
    kodexkosdaq: '229200',
    kodexlev:    '122630',
    kodexinv:    '114800',
    kodexinv2:   '251340',
    kodexsp:     '379800'
  };
  try {
    const results = await Promise.allSettled(
      Object.entries(codes).map(async ([key, code]) => {
        const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-price', kisHeaders(token, 'FHKST01010100'), {
          FID_COND_MRKT_DIV_CODE: 'J', FID_INPUT_ISCD: code
        });
        return [key, { value: r.output.stck_prpr, change: r.output.prdy_ctrt }];
      })
    );
    const etf = {};
    results.forEach(r => { if (r.status === 'fulfilled') etf[r.value[0]] = r.value[1]; });
    return etf;
  } catch (e) {
    log('❌ ETF 수집 실패: ' + e.message);
    return null;
  }
}

// KR 섹터
async function fetchKrSectors(token) {
  log('📊 KR 섹터 수집 중...');
  // 업종 코드 목록 (KRX 업종)
  const sectors = [
    { code: 'G050', name: '반도체', tip: '삼성전자, SK하이닉스 등 반도체 제조·설계 기업들이에요. 미국 나스닥과 연동이 강해요.' },
    { code: 'G140', name: '제약/바이오', tip: '셀트리온, 삼성바이오로직스 등 제약·바이오 기업들이에요. 임상 결과에 따라 변동성이 커요.' },
    { code: 'G020', name: '금융업', tip: 'KB금융, 신한지주 등 은행·보험·증권 기업들이에요. 금리 변화에 민감해요.' },
    { code: 'G070', name: '자동차', tip: '현대차, 기아 등 자동차 제조 기업들이에요. 환율과 글로벌 수요에 영향을 받아요.' },
    { code: 'G080', name: '화학', tip: 'LG화학, SK이노베이션 등 석유화학·소재 기업들이에요.' },
    { code: 'G150', name: '2차전지', tip: '삼성SDI, LG에너지솔루션 등 배터리 기업들이에요. 전기차 시장과 연동돼요.' },
    { code: 'G100', name: '철강/금속', tip: 'POSCO, 현대제철 등 철강·금속 기업들이에요. 원자재 가격과 연동돼요.' },
    { code: 'G030', name: '건설업', tip: '현대건설, GS건설 등 건설 기업들이에요. 부동산 경기에 민감해요.' },
    { code: 'G060', name: '전기전자', tip: 'LG전자, 삼성전기 등 전자·전기 기업들이에요.' },
    { code: 'G110', name: '음식료', tip: 'CJ제일제당, 오리온 등 식품 기업들이에요. 경기 방어주 성격이 강해요.' },
    { code: 'G160', name: '유통', tip: '롯데쇼핑, 이마트 등 유통 기업들이에요. 소비 트렌드에 민감해요.' }
  ];

  try {
    const results = await Promise.allSettled(
      sectors.map(async (s) => {
        const r = await kisGet('/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice', kisHeaders(token, 'FHKUP03500100'), {
          FID_COND_MRKT_DIV_CODE: 'U', FID_INPUT_ISCD: s.code,
          FID_INPUT_DATE_1: getDate(0), FID_INPUT_DATE_2: getDate(0), FID_PERIOD_DIV_CODE: 'D'
        });
        const d = r?.output2?.[0];
        return {
          name: s.name,
          tip: s.tip,
          chg: parseFloat(d?.bstp_nmix_ctrt || 0),
          vol: parseInt(d?.acml_tr_pbmn || 0)
        };
      })
    );
    return results.filter(r => r.status === 'fulfilled').map(r => r.value);
  } catch (e) {
    log('❌ KR 섹터 수집 실패: ' + e.message);
    return null;
  }
}

// KR 수급 상위 종목
async function fetchKrStocks(token) {
  log('📋 KR 수급 종목 수집 중...');
  try {
    const fetchRanking = async (trId, sortCode) => {
      const r = await kisGet('/uapi/domestic-stock/v1/ranking/trading-volume', kisHeaders(token, trId), {
        FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20171',
        FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0',
        FID_BLNG_CLS_CODE: sortCode, FID_TRGT_CLS_CODE: '111111111',
        FID_TRGT_EXLS_CLS_CODE: '000000', FID_INPUT_PRICE_1: '',
        FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: ''
      });
      return (r?.output || []).slice(0, 5).map(s => ({
        name: s.hts_kor_isnm, code: s.mksc_shrn_iscd,
        chg: parseFloat(s.prdy_ctrt || 0), amt: parseInt(s.ntby_qty || 0)
      }));
    };

    const [volume, foreignBuy, foreignSell, instBuy, instSell, indvBuy, indvSell] = await Promise.all([
      // 거래대금 상위
      (async () => {
        const r = await kisGet('/uapi/domestic-stock/v1/ranking/trading-value', kisHeaders(token, 'FHPST01710000'), {
          FID_COND_MRKT_DIV_CODE: 'J', FID_COND_SCR_DIV_CODE: '20171',
          FID_INPUT_ISCD: '0000', FID_DIV_CLS_CODE: '0',
          FID_BLNG_CLS_CODE: '0', FID_TRGT_CLS_CODE: '111111111',
          FID_TRGT_EXLS_CLS_CODE: '000000', FID_INPUT_PRICE_1: '',
          FID_INPUT_PRICE_2: '', FID_VOL_CNT: '', FID_INPUT_DATE_1: ''
        });
        return (r?.output || []).slice(0, 10).map(s => ({
          name: s.hts_kor_isnm, code: s.mksc_shrn_iscd,
          chg: parseFloat(s.prdy_ctrt || 0), amt: parseInt(s.acml_tr_pbmn || 0)
        }));
      })(),
      fetchRanking('FHPST01720000', '1'), // 외국인 순매수
      fetchRanking('FHPST01720000', '2'), // 외국인 순매도
      fetchRanking('FHPST01720000', '3'), // 기관 순매수
      fetchRanking('FHPST01720000', '4'), // 기관 순매도
      fetchRanking('FHPST01720000', '5'), // 개인 순매수
      fetchRanking('FHPST01720000', '6'), // 개인 순매도
    ]);

    return { volume, foreignBuy, foreignSell, instBuy, instSell, indvBuy, indvSell };
  } catch (e) {
    log('❌ KR 종목 수집 실패: ' + e.message);
    return null;
  }
}

// 미국 지수 + ETF + M7 + 한국 관련 ETF + 원자재 (Yahoo Finance)
async function fetchUsData() {
  log('🇺🇸 미국 데이터 수집 중...');

  const symbols = {
    sp500:  '^GSPC',  nasdaq: '^IXIC',  dow:    '^DJI',
    vix:    '^VIX',   tnx:   '^TNX',    dxy:    'DX-Y.NYB',
    spy:    'SPY',    qqq:   'QQQ',     tqqq:   'TQQQ',   sqqq: 'SQQQ',
    aapl:   'AAPL',   msft:  'MSFT',    nvda:   'NVDA',
    amzn:   'AMZN',   googl: 'GOOGL',   meta:   'META',   tsla: 'TSLA',
    ewy:    'EWY',    koru:  'KORU',    korz:   'KORZ',
    wti:    'CL=F',   gold:  'GC=F',    copper: 'HG=F',
    // US 섹터 ETF
    xlk:    'XLK',    xlc:   'XLC',     xly:    'XLY',
    xlf:    'XLF',    xlv:   'XLV',     xli:    'XLI',
    xlp:    'XLP',    xlre:  'XLRE',    xlu:    'XLU',
    xlb:    'XLB',    xle:   'XLE'
  };

  const results = {};
  await Promise.allSettled(
    Object.entries(symbols).map(async ([key, sym]) => {
      try {
        const d = await fetchYahoo(sym);
        results[key] = d;
      } catch (e) {
        log(`  ⚠️ ${sym} 실패: ${e.message}`);
      }
    })
  );

  return results;
}

// ─────────────────────────────────────────────
// 전체 데이터 수집 & data.json 저장
// ─────────────────────────────────────────────
async function collect() {
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  log('📡 데이터 수집 시작');

  const token = await getToken();
  if (!token) {
    log('❌ 토큰 없음 - 수집 중단');
    return;
  }

  // 병렬로 수집
  const [
    { kospi, kosdaq },
    supply,
    program,
    depth,
    etf,
    sectors,
    stocks,
    us
  ] = await Promise.all([
    fetchIndex(token),
    fetchSupply(token),
    fetchProgram(token),
    fetchDepth(token),
    fetchKrEtf(token),
    fetchKrSectors(token),
    fetchKrStocks(token),
    fetchUsData()
  ]);

  // data.json 구성
  const now = kstNow();
  const data = {
    updatedAt: now.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' }),
    dataDate: `${String(now.getFullYear()).slice(2)}.${String(now.getMonth()+1).padStart(2,'0')}.${String(now.getDate()).padStart(2,'0')} (${['일','월','화','수','목','금','토'][now.getDay()]}) 기준`,

    kospi, kosdaq, supply, program, depth, etf, sectors, stocks,

    us: {
      sp500:  { value: us.sp500?.value,  change: us.sp500?.change  },
      nasdaq: { value: us.nasdaq?.value, change: us.nasdaq?.change },
      dow:    { value: us.dow?.value,    change: us.dow?.change    },
      vix:    { value: us.vix?.value,    change: us.vix?.change    },
      tnx:    { value: us.tnx?.value,    change: us.tnx?.change    },
      dxy:    { value: us.dxy?.value,    change: us.dxy?.change    },
      spy:    { value: us.spy?.value,    change: us.spy?.change    },
      qqq:    { value: us.qqq?.value,    change: us.qqq?.change    },
      tqqq:   { value: us.tqqq?.value,   change: us.tqqq?.change   },
      sqqq:   { value: us.sqqq?.value,   change: us.sqqq?.change   }
    },

    m7: {
      aapl:  { value: us.aapl?.value,  change: us.aapl?.change  },
      msft:  { value: us.msft?.value,  change: us.msft?.change  },
      nvda:  { value: us.nvda?.value,  change: us.nvda?.change  },
      amzn:  { value: us.amzn?.value,  change: us.amzn?.change  },
      googl: { value: us.googl?.value, change: us.googl?.change },
      meta:  { value: us.meta?.value,  change: us.meta?.change  },
      tsla:  { value: us.tsla?.value,  change: us.tsla?.change  }
    },

    usSectors: [
      { name: 'IT',       chg: us.xlk?.change  || 0 },
      { name: '커뮤니케이션', chg: us.xlc?.change  || 0 },
      { name: '임의소비재',  chg: us.xly?.change  || 0 },
      { name: '금융',      chg: us.xlf?.change  || 0 },
      { name: '헬스케어',   chg: us.xlv?.change  || 0 },
      { name: '산업재',     chg: us.xli?.change  || 0 },
      { name: '필수소비재',  chg: us.xlp?.change  || 0 },
      { name: '부동산',     chg: us.xlre?.change || 0 },
      { name: '유틸리티',   chg: us.xlu?.change  || 0 },
      { name: '소재',      chg: us.xlb?.change  || 0 },
      { name: '에너지',     chg: us.xle?.change  || 0 }
    ],

    koreaEtf: {
      ewy:  { value: us.ewy?.value,  change: us.ewy?.change  },
      koru: { value: us.koru?.value, change: us.koru?.change },
      korz: { value: us.korz?.value, change: us.korz?.change }
    },

    commodities: {
      wti:    { value: us.wti?.value,    change: us.wti?.change    },
      gold:   { value: us.gold?.value,   change: us.gold?.change   },
      copper: { value: us.copper?.value, change: us.copper?.change }
    }
  };

  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  log('✅ data.json 저장 완료');
  log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
}

// ─────────────────────────────────────────────
// 날짜 유틸
// ─────────────────────────────────────────────
function getDate(offset = 0) {
  const d = kstNow();
  d.setDate(d.getDate() + offset);
  return `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}${String(d.getDate()).padStart(2,'0')}`;
}

// ─────────────────────────────────────────────
// 스케줄러 (cron 없이 setInterval로 구현)
// ─────────────────────────────────────────────
function pad(n) { return String(n).padStart(2, '0'); }

function scheduleAt(hour, minute, job) {
  function check() {
    const now = kstNow();
    if (now.getHours() === hour && now.getMinutes() === minute) {
      job();
    }
  }
  // 매 분마다 체크
  setInterval(check, 60 * 1000);
  log(`🕐 스케줄 등록: KST ${pad(hour)}:${pad(minute)}`);
}

log('🚀 MONEYLIVE 스케줄러 시작');
log('');
log('스케줄:');
log('  15:00 → 토큰 발급');
log('  15:35 → 1차 수집 (한국 정규장 종가)');
log('  18:05 → 2차 수집 (시간외 최종)');
log('  06:05 → 3차 수집 (미국 정규장 종료)');
log('  07:55 → 4차 수집 (최종)');
log('');

// 매일 15:00 → 토큰 발급
scheduleAt(15, 0, async () => {
  log('🔑 [15:00] 토큰 발급 시작');
  await issueToken();
});

// 15:35 → 1차 수집
scheduleAt(15, 35, () => {
  log('📡 [15:35] 1차 데이터 수집 시작');
  collect();
});

// 18:05 → 2차 수집
scheduleAt(18, 5, () => {
  log('📡 [18:05] 2차 데이터 수집 시작');
  collect();
});

// 06:05 → 3차 수집 (미국장 마감 후)
scheduleAt(6, 5, () => {
  log('📡 [06:05] 3차 데이터 수집 시작 (미국장 마감)');
  collect();
});

// 07:55 → 4차 수집 (최종)
scheduleAt(7, 55, () => {
  log('📡 [07:55] 4차 데이터 수집 시작 (최종)');
  collect();
});

// 서버 시작 직후 1회 즉시 수집 (data.json이 없을 때를 대비)
if (!fs.existsSync(DATA_FILE)) {
  log('📂 data.json 없음 → 즉시 수집 시작');
  setTimeout(collect, 3000);
} else {
  log('📂 data.json 존재 → 스케줄 대기 중');
}
