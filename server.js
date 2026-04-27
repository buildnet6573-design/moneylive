const express = require('express');
const cors = require('cors');
const path = require('path');
const scheduler = require('./scheduler'); 

const app = express();
const PORT = process.env.PORT || 3000;

// 미들웨어 설정
app.use(cors());
app.use(express.static(path.join(__dirname, 'public'))); // index.html 파일이 있는 폴더 지정

// 서버 메모리 캐시 (API 오류 시 방어용)
let fallbackCacheData = {
    updatedAt: "데이터 수집 대기중...",
    stocks: {
        volume: [], foreignBuy: [], instBuy: [], indvBuy: []
    }
};

// 프론트엔드 데이터 요청 API
app.get('/api/data', (req, res) => {
    try {
        const latestData = scheduler.getLatestData();
        
        // 스케줄러에서 정상적인 데이터를 가져왔다면 캐시 업데이트
        if (latestData && Object.keys(latestData).length > 0) {
            fallbackCacheData = latestData;
            res.json(latestData);
        } else {
            // 빈 객체이거나 에러 상태라면 마지막 정상 캐시 반환
            res.json(fallbackCacheData);
        }
    } catch (error) {
        console.error("API 라우터 에러 발생:", error);
        // 서버 내부 오류 시에도 시스템 중단 없이 캐시 데이터 전송
        res.json(fallbackCacheData);
    }
});

// 메인 페이지 라우팅
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 서버 실행
app.listen(PORT, () => {
    console.log(`🚀 MONEYLIVE 서버가 포트 ${PORT}에서 실행 중입니다.`);
    // 서버 가동 시 스케줄러 초기화 및 즉시 실행
    scheduler.init(); 
});
