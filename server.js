const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'data.json');

// 서버 메모리 캐시 (에러 발생 시 프론트엔드 화면 깨짐 방어용)
let memoryCache = null;

// 정적 파일 서빙 (index.html)
app.use(express.static(__dirname));

// /api/data → 스케줄러가 저장한 data.json 반환
app.get('/api/data', (req, res) => {
  try {
    if (!fs.existsSync(DATA_FILE)) {
      if (memoryCache) {
        res.setHeader('Cache-Control', 'no-cache');
        return res.json(memoryCache);
      }
      return res.status(503).json({ error: '아직 데이터가 수집되지 않았어요. 잠시 후 다시 시도해주세요.' });
    }
    
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    const data = JSON.parse(raw);
    
    // 정상 로드 시 메모리 캐시 갱신
    memoryCache = data;
    
    res.setHeader('Cache-Control', 'no-cache');
    res.json(data);
  } catch (e) {
    console.error('[server] data.json 읽기 실패:', e.message);
    // 에러 발생 시 메모리 캐시가 남아있다면 서버를 멈추지 않고 안전하게 캐시 전송
    if (memoryCache) {
      res.setHeader('Cache-Control', 'no-cache');
      res.json(memoryCache);
    } else {
      res.status(500).json({ error: '데이터 읽기 실패' });
    }
  }
});

// 헬스체크
app.get('/health', (req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

app.listen(PORT, () => {
  console.log(`[server] MONEYLIVE 서버 시작 → http://localhost:${PORT}`);

  // 서버 시작 시 스케줄러도 함께 실행
  require('./scheduler');
});
