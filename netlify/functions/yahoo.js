const https = require('https');

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(e); }
      });
    }).on('error', reject);
  });
}

exports.handler = async (event) => {
  const symbol = event.queryStringParameters?.symbol;
  if (!symbol) {
    return { statusCode: 400, body: JSON.stringify({ error: 'symbol 파라미터 필요' }) };
  }

  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=2d`;
    const data = await fetchUrl(url);
    const result = data.chart.result[0];
    const closes = result.indicators.quote[0].close;
    const prev = closes[closes.length - 2];
    const curr = closes[closes.length - 1];
    const change = ((curr - prev) / prev) * 100;

    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ value: curr, change })
    };
  } catch(e) {
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: e.message })
    };
  }
};
