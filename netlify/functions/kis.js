const https = require('https');

// ── 토큰 캐시 (메모리에 저장, 만료 전까지 재사용) ──
let cachedToken = null;
let tokenExpiry = null;

async function getToken(appKey, appSecret) {
  const now = Date.now();

  // 토큰이 있고 만료 30분 전까지는 재사용
  if (cachedToken && tokenExpiry && now < tokenExpiry - 30 * 60 * 1000) {
    return cachedToken;
  }

  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      grant_type: 'client_credentials',
      appkey: appKey,
      appsecret: appSecret
    });
    const options = {
      hostname: 'openapi.koreainvestment.com',
      port: 9443,
      path: '/oauth2/tokenP',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          cachedToken = parsed.access_token;
          // 만료 시간 저장 (초 단위 → ms 변환)
          tokenExpiry = now + (parsed.expires_in * 1000);
          console.log('새 토큰 발급 완료, 만료:', new Date(tokenExpiry).toISOString());
          resolve(cachedToken);
        } catch(e) {
          reject(e);
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function fetchKIS(path, headers) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'openapi.koreainvestment.com',
      port: 9443,
      path,
      method: 'GET',
      headers
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

exports.handler = async (event) => {
  const appKey = process.env.KIS_APP_KEY;
  const appSecret = process.env.KIS_APP_SECRET;

  try {
    const token = await getToken(appKey, appSecret);

    const baseHeaders = {
      'authorization': `Bearer ${token}`,
      'appkey': appKey,
      'appsecret': appSecret,
      'content-type': 'application/json'
    };

    // 코스피 지수
    const kospi = await fetchKIS(
      '/uapi/domestic-stock/v1/quotations/inquire-index-price?FID_COND_MRKT_DIV_CODE=U&FID_INPUT_ISCD=0001',
      { ...baseHeaders, 'tr_id': 'FHPUP02100000' }
    );

    // 코스닥 지수
    const kosdaq = await fetchKIS(
      '/uapi/domestic-stock/v1/quotations/inquire-index-price?FID_COND_MRKT_DIV_CODE=U&FID_INPUT_ISCD=1001',
      { ...baseHeaders, 'tr_id': 'FHPUP02100000' }
    );

    // 수급 데이터 (코스피)
    const supply = await fetchKIS(
      '/uapi/domestic-stock/v1/quotations/inquire-investor?FID_COND_MRKT_DIV_CODE=J&FID_INPUT_ISCD=0001',
      { ...baseHeaders, 'tr_id': 'FHPTJ04400000' }
    );

    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({
        kospi: {
          value: kospi.output?.bstp_nmix_prpr,
          change: kospi.output?.bstp_nmix_prdy_ctrt
        },
        kosdaq: {
          value: kosdaq.output?.bstp_nmix_prpr,
          change: kosdaq.output?.bstp_nmix_prdy_ctrt
        },
        supply: {
          foreign: supply.output?.[0]?.frgn_ntby_qty,
          institution: supply.output?.[0]?.orgn_ntby_qty,
          individual: supply.output?.[0]?.indv_ntby_qty
        }
      })
    };
  } catch (e) {
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: e.message })
    };
  }
};
