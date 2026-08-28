# EX 급등포착 서버

폰 화면을 안 켜놔도, 24시간 서버가 대신 감시하고 텔레그램으로 알림을 보내주는 버전입니다.

## 1단계: 텔레그램 봇 만들기 (5분)

1. 텔레그램 앱에서 `@BotFather` 검색해서 대화 시작
2. `/newbot` 입력 → 봇 이름 정하기 → 봇 아이디(예: `my_surge_bot`) 정하기
3. 완료되면 **토큰**이 나옵니다 (예: `123456789:ABCdefGhIJKlmNoPQRstuVwxyZ`) → 이걸 복사해두세요
4. 이제 만든 봇을 텔레그램에서 검색해서 **대화 시작(아무 메시지나 전송)**

## 2단계: 내 chat_id 알아내기

1. 봇에게 아무 메시지나 보낸 직후, 브라우저에서 아래 주소 접속 (토큰 부분 교체):
   ```
   https://api.telegram.org/bot<여기에_토큰>/getUpdates
   ```
2. 결과에서 `"chat":{"id":123456789, ...}` 부분의 숫자가 내 chat_id입니다

## 3단계: 설정 파일 만들기

`.env.example` 파일을 복사해서 `.env` 파일을 만들고, 위에서 받은 토큰과 chat_id를 채워넣으세요.

```bash
cp .env.example .env
# .env 파일을 열어서 TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID 채우기
```

## 4단계: 서버에 올려서 실행하기

### 가장 쉬운 방법: Railway.app (추천)

1. [railway.app](https://railway.app) 가입 (깃허브 계정으로 로그인 가능)
2. 이 폴더(`surge-server`) 전체를 깃허브 저장소에 업로드
3. Railway에서 "New Project" → "Deploy from GitHub repo" → 방금 올린 저장소 선택
4. Railway의 프로젝트 설정 → "Variables" 탭에서 `.env`에 넣었던 값들을 그대로 환경변수로 등록
5. 자동으로 배포되고 24시간 돌아갑니다 (무료 티어 있음, 사용량 넘으면 소액 과금)

### 저가 VPS로 직접 돌리기 (Vultr, Contabo 등, 월 5천원~)

```bash
# 서버에 파일 업로드 후
npm install
npm install -g pm2          # 프로세스가 죽으면 자동 재시작해주는 도구
pm2 start index.js --name surge-scanner
pm2 save
pm2 startup                 # 서버 재부팅 시에도 자동 시작되도록 설정 (안내 문구대로 명령어 한 줄 추가 실행)
```

이후 관리 명령어:
```bash
pm2 logs surge-scanner      # 실시간 로그 보기
pm2 restart surge-scanner   # 재시작
pm2 stop surge-scanner      # 중지
```

## 설정값 설명 (.env)

| 변수 | 설명 | 기본값 |
|---|---|---|
| `SENSITIVITY` | normal / strong / extreme / insane | strong |
| `DIRECTION` | all / up / down | all |
| `EXIT_THRESHOLD_PCT` | 고점 대비 몇 % 빠지면 이탈 경고 | 5 |
| `MIN_QUOTE_VOLUME` | 감시 대상 최소 24h 거래대금(USDT) | 50000 |

브라우저 버전과 동일한 로직입니다: 거래량 폭증 + 초당 상승속도 + 가격변동을 모두 만족하는 코인만 텔레그램으로 알림이 옵니다. 급등 잡힌 코인은 고점을 계속 추적하다 이탈 조건 충족 시 별도 알림이 갑니다.

## 주의사항

- Node.js 18 이상 필요
- 서버는 껐다 켜져도 되지만(재접속 시 심볼 목록/기준선을 다시 시딩합니다), 계속 켜져 있어야 감시가 유지됩니다
- 무료 호스팅(Railway 무료 티어 등)은 트래픽/시간 제한이 있을 수 있으니, 정말 상시로 쓰실 거면 저가 VPS가 더 안정적입니다
