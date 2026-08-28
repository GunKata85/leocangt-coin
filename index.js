// EX 급등포착 서버 - 24시간 상시 감시 + 텔레그램 푸시 알림
// -----------------------------------------------------------
// 실행 전 .env 파일에 TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID를 채워주세요.
// Node.js 18 이상 필요 (내장 fetch 사용). npm install 후 node index.js 로 실행.

import WebSocket from 'ws';
import 'dotenv/config';

// ===== 설정 =====
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const SENSITIVITY = process.env.SENSITIVITY || 'strong'; // normal | strong | extreme | insane
const DIRECTION = process.env.DIRECTION || 'all';         // all | up | down
const EXIT_THRESHOLD_PCT = parseFloat(process.env.EXIT_THRESHOLD_PCT || '5'); // 고점대비 이탈 기준(%)
const MIN_QUOTE_VOLUME = parseFloat(process.env.MIN_QUOTE_VOLUME || '50000'); // 감시 대상 최소 24h 거래대금(USDT)
const FUTURES_ONLY = (process.env.FUTURES_ONLY || 'true').toLowerCase() === 'true'; // true면 선물(무기한) 상장된 코인만 감시

const ALERT_THRESHOLDS = {
  normal:  { volMult: 3,  pricePct: 1, ratePerSec: 0.05 },
  strong:  { volMult: 5,  pricePct: 2, ratePerSec: 0.12 },
  extreme: { volMult: 10, pricePct: 3, ratePerSec: 0.25 },
  insane:  { volMult: 15, pricePct: 5, ratePerSec: 0.5 }
};
const thresholds = ALERT_THRESHOLDS[SENSITIVITY] || ALERT_THRESHOLDS.strong;

const RATE_LOOKBACK_MS = 8000;   // 초당 상승률 계산에 사용할 최근 시간 창(8초)
const RATE_MIN_SAMPLES_MS = 5000; // 최소 이만큼의 기록이 쌓여야 상승속도 판정(5초)
const UI_TICK_MS = 5000;
const CHUNK_SIZE = 180;

if(!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID){
  console.error('[오류] .env에 TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID를 설정해주세요.');
  process.exit(1);
}

// ===== 상태 =====
const symbolState = new Map(); // symbol -> {closed, curOpen, curClose, curQuoteVol, curOpenTime, curCloseTime, priceHistory}
const trackedCoins = new Map(); // symbol -> {entry, peak, peakAt, last, exitWarned, currentlyHit}
let wsConnections = [];

// ===== 텔레그램 전송 =====
async function sendTelegram(text){
  try{
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' })
    });
  }catch(e){
    console.error('텔레그램 전송 실패:', e.message);
  }
}

// ===== 후보 심볼 목록 구성 =====
// 바이낸스 선물(무기한)에 상장된 심볼 목록 (FUTURES_ONLY=true일 때 필터링용)
async function getFuturesSymbols(){
  try{
    const res = await fetch('https://fapi.binance.com/fapi/v1/exchangeInfo');
    if(!res.ok) return null;
    const data = await res.json();
    return new Set(
      data.symbols
        .filter(s => s.contractType === 'PERPETUAL' && s.status === 'TRADING')
        .map(s => s.symbol)
    );
  }catch(e){
    console.error('선물 심볼 목록 조회 실패, 필터 없이 진행:', e.message);
    return null;
  }
}

async function buildCandidatePool(){
  const res = await fetch('https://api.binance.com/api/v3/ticker/24hr');
  if(!res.ok) throw new Error('심볼 목록을 불러오지 못했습니다.');
  const data = await res.json();

  let pool = data
    .filter(d => d.symbol.endsWith('USDT') && !/(UP|DOWN|BULL|BEAR)USDT$/.test(d.symbol))
    .map(d => ({ symbol: d.symbol, quoteVolume: parseFloat(d.quoteVolume) }))
    .filter(d => d.quoteVolume >= MIN_QUOTE_VOLUME)
    .sort((a,b)=> b.quoteVolume - a.quoteVolume)
    .map(d => d.symbol);

  if(FUTURES_ONLY){
    const futuresSymbols = await getFuturesSymbols();
    if(futuresSymbols){
      const before = pool.length;
      pool = pool.filter(s => futuresSymbols.has(s));
      console.log(`선물 상장 코인만 필터링: ${before}개 → ${pool.length}개`);
    }
  }

  return pool;
}

// ===== 초기 히스토리 시딩 =====
async function seedSymbolHistory(symbols){
  const concurrency = 15;
  let idx = 0;
  let done = 0;

  async function worker(){
    while(idx < symbols.length){
      const i = idx++;
      const symbol = symbols[i];
      try{
        const res = await fetch(`https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=1m&limit=10`);
        if(res.ok){
          const kl = await res.json();
          if(Array.isArray(kl) && kl.length > 0){
            symbolState.set(symbol, {
              closed: kl.map(k => ({ open: parseFloat(k[1]), quoteVol: parseFloat(k[7]) })),
              curOpen: null, curClose: null, curQuoteVol: 0, curOpenTime: 0, curCloseTime: 0,
              priceHistory: []
            });
          }
        }
      }catch(e){ /* 개별 실패는 건너뜀 */ }
      done++;
      if(done % 50 === 0) console.log(`  시딩 진행: ${done}/${symbols.length}`);
    }
  }

  await Promise.all(Array(concurrency).fill(0).map(worker));
}

// ===== 웹소켓 연결 =====
function connectStreams(symbols){
  for(let i=0; i<symbols.length; i+=CHUNK_SIZE){
    openOneStream(symbols.slice(i, i+CHUNK_SIZE));
  }
}

function openOneStream(chunk){
  const streams = chunk.map(s => s.toLowerCase() + '@kline_1m').join('/');
  const url = `wss://stream.binance.com:9443/stream?streams=${streams}`;
  const ws = new WebSocket(url);
  wsConnections.push(ws);

  ws.on('message', (raw)=>{
    try{
      const msg = JSON.parse(raw);
      const k = msg.data && msg.data.k;
      if(!k) return;
      const symbol = k.s;
      let st = symbolState.get(symbol);
      if(!st){
        st = { closed: [], curOpen: null, curClose: null, curQuoteVol: 0, curOpenTime: 0, curCloseTime: 0, priceHistory: [] };
        symbolState.set(symbol, st);
      }
      st.curOpen = parseFloat(k.o);
      st.curClose = parseFloat(k.c);
      st.curQuoteVol = parseFloat(k.q);
      st.curOpenTime = k.t;
      st.curCloseTime = k.T;

      if(k.x){
        st.closed.push({ open: st.curOpen, quoteVol: st.curQuoteVol });
        if(st.closed.length > 10) st.closed.shift();
      }
    }catch(e){ /* 무시 */ }
  });

  ws.on('close', ()=>{
    setTimeout(()=> openOneStream(chunk), 3000);
  });
  ws.on('error', ()=>{ try{ ws.close(); }catch(e){} });
}

// ===== 초당 상승속도 계산 =====
function calcRatePerSec(priceHistory){
  const now = Date.now();
  const inWindow = priceHistory.filter(p => now - p.t <= RATE_LOOKBACK_MS);
  if(inWindow.length < 2) return null;
  const oldest = inWindow[0];
  const newest = inWindow[inWindow.length - 1];
  const elapsedMs = newest.t - oldest.t;
  if(elapsedMs < RATE_MIN_SAMPLES_MS) return null;
  if(!oldest.price) return null;
  const pctChange = (newest.price - oldest.price) / oldest.price * 100;
  return pctChange / (elapsedMs / 1000);
}

// ===== 판정 로직 =====
function computeHits(){
  const hits = [];
  const allResults = [];

  symbolState.forEach((st, symbol)=>{
    if(st.curOpen === null || st.closed.length === 0) return;

    const baseline = st.closed.reduce((s,c)=> s + c.quoteVol, 0) / st.closed.length;
    const isOpen = st.curCloseTime > Date.now();
    let curVolNormalized = st.curQuoteVol;
    if(isOpen){
      const elapsedSec = Math.max((Date.now() - st.curOpenTime) / 1000, 3);
      curVolNormalized = st.curQuoteVol * (60 / Math.min(elapsedSec, 60));
    }
    const volMult = baseline > 0 ? (curVolNormalized / baseline) : 0;

    const firstOpen = st.closed[0].open;
    const lastClose = st.curClose;
    const pricePct = firstOpen ? ((lastClose - firstOpen) / firstOpen * 100) : 0;
    const ratePerSec = calcRatePerSec(st.priceHistory);

    allResults.push({ symbol, price: lastClose });

    const meetsVolume = volMult >= thresholds.volMult;
    const meetsPrice = Math.abs(pricePct) >= thresholds.pricePct;
    const meetsDirection = DIRECTION === 'all' ? true : (DIRECTION === 'up' ? pricePct > 0 : pricePct < 0);
    const meetsRate = ratePerSec !== null && (
      DIRECTION === 'down' ? (ratePerSec <= -thresholds.ratePerSec) : (ratePerSec >= thresholds.ratePerSec)
    );

    if(meetsVolume && meetsPrice && meetsDirection && meetsRate){
      hits.push({ symbol, price: lastClose, volMult, pricePct, ratePerSec });
    }
  });

  return { hits, allResults };
}

// ===== 추적/이탈 판정 =====
function updateTracking(hits, allResults){
  const newOnes = [];
  const exitFires = [];

  hits.forEach(h=>{
    if(!trackedCoins.has(h.symbol)){
      trackedCoins.set(h.symbol, {
        entry: h.price, peak: h.price, last: h.price, exitWarned: false, currentlyHit: true
      });
      newOnes.push(h);
    }
  });

  const hitSymbols = new Set(hits.map(h=>h.symbol));
  trackedCoins.forEach((t, sym)=>{ t.currentlyHit = hitSymbols.has(sym); });

  allResults.forEach(r=>{
    if(!trackedCoins.has(r.symbol)) return;
    const t = trackedCoins.get(r.symbol);
    t.last = r.price;
    if(r.price > t.peak){
      t.peak = r.price;
      if(t.exitWarned) t.exitWarned = false;
    }
  });

  trackedCoins.forEach((t, sym)=>{
    const drawdownPct = t.peak > 0 ? ((t.peak - t.last) / t.peak * 100) : 0;
    if(drawdownPct >= EXIT_THRESHOLD_PCT && !t.exitWarned){
      t.exitWarned = true;
      exitFires.push({ symbol: sym, drawdown: drawdownPct, price: t.last });
    }
  });

  return { newOnes, exitFires };
}

// ===== 메인 틱 =====
function tick(){
  const now = Date.now();
  symbolState.forEach(st=>{
    if(st.curClose === null) return;
    st.priceHistory.push({ t: now, price: st.curClose });
    while(st.priceHistory.length > 0 && now - st.priceHistory[0].t > RATE_LOOKBACK_MS){
      st.priceHistory.shift();
    }
  });

  const { hits, allResults } = computeHits();
  const { newOnes, exitFires } = updateTracking(hits, allResults);

  newOnes.forEach(h=>{
    const isUp = h.pricePct >= 0;
    const icon = isUp ? '🚀' : '📉';
    const label = isUp ? '급등 감지' : '급락 감지';
    const rateLabel = isUp ? '초당 상승속도' : '초당 하락속도';
    const msg = `${icon} <b>${label}: ${h.symbol.replace('USDT','')}</b>\n가격: ${h.price}\n거래량 배율: ${h.volMult.toFixed(1)}배\n${rateLabel}: ${Math.abs(h.ratePerSec).toFixed(2)}%/초\n10분 변동: ${h.pricePct.toFixed(2)}%`;
    console.log(msg.replace(/<\/?b>/g,''));
    sendTelegram(msg);
  });

  exitFires.forEach(e=>{
    const msg = `🔻 <b>이탈 경고: ${e.symbol.replace('USDT','')}</b>\n현재가: ${e.price}\n고점 대비: -${e.drawdown.toFixed(1)}%`;
    console.log(msg.replace(/<\/?b>/g,''));
    sendTelegram(msg);
  });

  console.log(`[${new Date().toISOString()}] 수신 ${symbolState.size}개 · 감지 ${hits.length}건 · 추적 ${trackedCoins.size}건`);
}

// ===== 시작 =====
async function main(){
  console.log('심볼 목록 불러오는 중...');
  const symbols = await buildCandidatePool();
  console.log(`감시 대상: ${symbols.length}개`);

  console.log('초기 데이터 시딩 중...');
  await seedSymbolHistory(symbols);

  console.log('웹소켓 연결 중...');
  connectStreams(symbols);

  await sendTelegram(`✅ EX 급등포착 서버 감시 시작 (${symbols.length}개 코인, 민감도: ${SENSITIVITY})`);

  setInterval(tick, UI_TICK_MS);
}

main().catch(err=>{
  console.error('시작 실패:', err);
  process.exit(1);
});
