const https = require('https');

async function getToken(appKey, appSecret) {
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
      res.on('end', () => resolve(JSON.parse(data)));
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
      res.on('end', () => resolve(JSON.parse(data)));
    });
    req.on('error', reject);
    req.end();
  });
}

exports.handler = async (event) => {
  const appKey = process.env.KIS_APP_KEY;
  const appSecret = process.env.KIS_APP_SECRET;

  try {
    const tokenData = await getToken(appKey, appSecret);
    const token = tokenData.access_token;

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
      body: JSON.stringify({ error: e.message })
    };
  }
};
