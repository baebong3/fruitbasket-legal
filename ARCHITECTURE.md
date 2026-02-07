# 과일바구니 (Fruitbasket) 시스템 아키텍처

## 전체 시스템 개요

과일바구니는 KAMIS(농산물유통정보) 공공 API를 수집·분석하여 농산물 가격 정보를 제공하는 서비스입니다.

```
┌──────────────────────────────────────────────────────────────────┐
│                        과일바구니 시스템                            │
│                                                                  │
│  ┌─────────────┐   ┌─────────────┐   ┌────────────────────────┐ │
│  │ Presentation │   │  API Server │   │  Data Collection &     │ │
│  │   (Web)      │──▶│  (Backend)  │◀──│  Analysis Pipeline     │ │
│  └─────────────┘   └─────────────┘   └────────────────────────┘ │
│                           │                      │               │
│                           ▼                      ▼               │
│                    ┌─────────────┐      ┌──────────────┐        │
│                    │  Data Store │      │ External API │        │
│                    └─────────────┘      │ (KAMIS)      │        │
│                                         └──────────────┘        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 모듈 구성

### 모듈 1: Presentation Layer (프론트엔드)

사용자에게 가격 정보를 시각적으로 전달하는 계층입니다.

```
presentation/
├── [UI Components]
│   ├── Hero              — 서비스 브랜딩, 과일 애니메이션
│   ├── Navigation        — Sticky 내비게이션 + Scroll-spy
│   ├── PriceSnapshot     — 실시간 가격 동향 카드 (상승/하락)
│   ├── ApiDocs           — 개발자 API 문서 (Beta)
│   ├── PolicyPages       — 개인정보처리방침, 이용약관
│   └── AffiliateNotice   — 쿠팡 파트너스 수익 공지
│
├── [Theme Engine]
│   ├── CSS Variables     — Light/Dark 모드 자동 전환
│   ├── Responsive Layout — 모바일 퍼스트 반응형 그리드
│   └── Component Styles  — 카드, 배지, 배너 디자인 시스템
│
└── [Client Logic]
    └── ScrollSpy         — 스크롤 위치 기반 네비게이션 활성화
```

**현재 상태**: `index.html` 단일 파일 (정적 사이트)
**발전 방향**: SPA 프레임워크 또는 SSG 도입 시 컴포넌트 분리

---

### 모듈 2: API Server (백엔드 서비스)

수집된 데이터를 가공하여 외부에 제공하는 REST API 계층입니다.

```
api-server/
├── [Routes]
│   └── GET /v1/prices/search    — 품목별 가격 조회
│       ├── Query: item_code     — KAMIS 품목 코드 (예: 111=사과)
│       └── Query: date          — 조회 일자 (YYYY-MM-DD, 기본=오늘)
│
├── [Service Layer]
│   ├── PriceService             — 가격 데이터 조회 및 가공 로직
│   ├── TrendAnalyzer            — 가격 동향 분석 (상승/하락/급등/저렴)
│   └── ResponseFormatter        — API 응답 포맷팅 (JSON)
│
└── [Middleware]
    ├── RateLimiter              — API 호출 제한
    ├── ErrorHandler             — 통합 에러 처리
    └── RequestLogger            — 요청/응답 로깅
```

**현재 상태**: Beta (`api.fruitbasket.org/v1/prices/search`)
**엔드포인트 스펙**:

```
GET /v1/prices/search?item_code=111&date=2026-02-07

Response:
{
  "item_code": "111",
  "item_name": "사과",
  "date": "2026-02-07",
  "prices": {
    "avg_price": 52000,
    "min_price": 45000,
    "max_price": 61000,
    "unit": "10kg",
    "trend": "rising"
  }
}
```

---

### 모듈 3: Data Collection & Analysis Pipeline (수집·분석 파이프라인)

외부 공공 API에서 농산물 가격 데이터를 수집하고 분석하는 핵심 파이프라인입니다.

```
pipeline/
├── [Collector]
│   ├── KamisClient              — KAMIS API 통신 클라이언트
│   │   ├── request_with_retry() — HTTP 요청 + 지수 백오프 재시도
│   │   ├── fetch_daily_prices() — 일별 도매 가격 수집
│   │   └── fetch_retail_prices()— 소매 가격 수집
│   │
│   └── Scheduler                — 수집 주기 관리
│       ├── daily_job()          — 일일 가격 수집 (09:00 KST)
│       └── weekly_summary()     — 주간 요약 데이터 생성
│
├── [Analyzer]
│   ├── PriceTrendAnalyzer       — 가격 추세 분석
│   │   ├── calc_moving_avg()    — 이동 평균 계산
│   │   ├── detect_anomaly()     — 이상 가격 탐지
│   │   └── classify_level()     — 가격 수준 분류 (급등/높음/저렴/하락)
│   │
│   └── MarketAnalyzer           — 시장 분석
│       ├── compare_markets()    — 도매시장 간 가격 비교
│       └── seasonal_pattern()   — 계절별 가격 패턴 분석
│
├── [Transformer]
│   ├── DataCleaner              — 원시 데이터 정제 (결측치, 이상치)
│   ├── UnitNormalizer           — 단위 통일 (kg, 개, 박스 등)
│   └── PriceAggregator          — 시장별/기간별 가격 집계
│
└── [Config]
    ├── config.json              — 수집 대상 품목, 주기 설정
    └── .env                     — KAMIS API 키 관리
```

**수집 대상 API**:

| API | 제공 기관 | 데이터 |
|-----|----------|--------|
| KAMIS 농산물유통정보 | 한국농수산식품유통공사 (aT) | 도매/소매 가격 |
| 공공데이터포털 | data.go.kr | 공공 데이터 보조 |

---

### 모듈 4: Data Store (데이터 저장소)

수집·분석된 데이터를 저장하고 API Server에 제공하는 계층입니다.

```
data-store/
├── [Price DB]
│   ├── daily_prices       — 일별 품목 가격 (도매/소매)
│   ├── price_trends       — 분석된 가격 추세 데이터
│   └── market_snapshots   — 시장별 스냅샷 (오늘의 과일 물가)
│
├── [Reference DB]
│   ├── item_codes         — KAMIS 품목 코드 매핑 테이블
│   ├── market_codes       — 도매시장 코드 목록
│   └── unit_conversions   — 단위 변환 기준표
│
└── [Cache]
    └── api_cache          — API 응답 캐시 (TTL 기반)
```

---

## 데이터 흐름

### 흐름 1: 데이터 수집·분석 (배치)

```
[Scheduler] ─── 매일 09:00 KST ───▶ 수집 시작
       │
       ▼
┌──────────────────────────────────────────────┐
│  Collector: KamisClient                       │
│                                               │
│  ┌───────────────────────────────────┐       │
│  │  request_with_retry()             │       │
│  │  ┌──────────┐    ┌──────────┐    │       │
│  │  │ HTTP GET │───▶│ KAMIS    │    │ 반복  │
│  │  │          │◀───│ API      │    │       │
│  │  └──────────┘    └──────────┘    │       │
│  │  실패 시: 지수 백오프 (2s→4s→8s)   │       │
│  └───────────────────────────────────┘       │
│  결과: raw_prices[] (원시 가격 데이터)          │
└──────────────┬───────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│  Transformer                                  │
│  ├── DataCleaner     → 결측치·이상치 제거       │
│  ├── UnitNormalizer  → 단위 통일 (kg 기준)     │
│  └── PriceAggregator → 시장별·기간별 집계       │
│  결과: cleaned_prices[] (정제된 가격 데이터)     │
└──────────────┬───────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│  Analyzer                                     │
│  ├── PriceTrendAnalyzer                       │
│  │   ├── 7일 이동평균 계산                      │
│  │   ├── 이상 가격 탐지                         │
│  │   └── 수준 분류: 급등 / 높음 / 저렴 / 하락    │
│  │                                             │
│  └── MarketAnalyzer                            │
│      ├── 도매시장 간 가격 비교                    │
│      └── 계절 패턴 분석                          │
│  결과: analyzed_data (분석 결과)                 │
└──────────────┬───────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│  Data Store                                   │
│  ├── daily_prices 저장                         │
│  ├── price_trends 갱신                         │
│  └── market_snapshots 생성                     │
└──────────────────────────────────────────────┘
```

### 흐름 2: API 가격 조회 (실시간)

```
[사용자/개발자]
       │
       │  GET /v1/prices/search?item_code=111
       ▼
┌──────────────────────┐
│  API Server          │
│  ├── RateLimiter     │ → 호출 제한 확인
│  ├── RequestLogger   │ → 요청 기록
│  └── Route Handler   │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐     ┌──────────────┐
│  PriceService        │────▶│  Cache       │ → 캐시 히트 시 즉시 반환
└──────────┬───────────┘     └──────────────┘
           │ 캐시 미스
           ▼
┌──────────────────────┐
│  Data Store 조회      │
│  ├── daily_prices    │
│  └── price_trends    │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  TrendAnalyzer       │ → 최신 동향 계산
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  ResponseFormatter   │ → JSON 응답 생성
└──────────┬───────────┘
           │
           ▼
       [JSON 응답 반환]
```

### 흐름 3: 웹 화면 렌더링

```
[사용자 브라우저]
       │
       ▼
┌──────────────────────────────────────────┐
│  Presentation Layer                       │
│                                           │
│  1. index.html 로드 (정적 호스팅)          │
│  2. CSS Variables → 테마 자동 적용         │
│     └── prefers-color-scheme: dark/light  │
│  3. PriceSnapshot 렌더링                   │
│     └── API Server로부터 가격 데이터 수신   │
│  4. ScrollSpy 활성화                       │
└──────────────┬───────────────────────────┘
               │
               ▼  (사용자 인터랙션)
┌──────────────────────────────────────────┐
│  ├── 가격 조회 → API Server 호출          │
│  ├── 섹션 네비게이션 → Scroll-spy 반응     │
│  └── 쿠팡 배너 클릭 → 제휴 링크 이동       │
└──────────────────────────────────────────┘
```

---

## 에러 처리 전략

### API 수집 단계

```
KamisClient.request_with_retry()
       │
       ├── ConnectionError ──▶ 재시도 (지수 백오프: 2s → 4s → 8s)
       ├── Timeout ──────────▶ 재시도 (지수 백오프, 최대 3회)
       ├── HTTP 5xx ─────────▶ 재시도 (지수 백오프)
       ├── HTTP 4xx ─────────▶ 즉시 실패 → 로깅 + 알림
       └── JSON 파싱 실패 ───▶ 해당 건 스킵 → 로깅

재시도 정책:
  - 최대 시도: 3회
  - 대기 시간: 2^(attempt+1) 초
  - 요청 타임아웃: 30초
```

### API 서버 단계

```
API Server ErrorHandler
       │
       ├── 유효하지 않은 item_code ──▶ 400 Bad Request
       ├── 데이터 없음 ──────────────▶ 404 Not Found
       ├── Rate Limit 초과 ──────────▶ 429 Too Many Requests
       └── 내부 오류 ────────────────▶ 500 Internal Server Error
```

---

## 인프라 구성

```
┌─────────────────────────────────────────────────────────────┐
│                      GitHub Repository                       │
│                                                              │
│  ┌──────────────────┐         ┌────────────────────────┐    │
│  │  Static Hosting   │         │  GitHub Actions (CI/CD) │    │
│  │                   │         │                         │    │
│  │  index.html  ────▶│ fruitbasket  │  Scheduler (cron)    │    │
│  │                   │ -legal.org   │  ├── 수집 파이프라인   │    │
│  └──────────────────┘         │  └── 테스트 / 배포       │    │
│                                └────────────────────────┘    │
│  ┌──────────────────┐         ┌────────────────────────┐    │
│  │  Secrets          │         │  API Server             │    │
│  │  KAMIS_API_KEY    │         │  api.fruitbasket.org    │    │
│  └──────────────────┘         └────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘

외부 데이터 소스:
  ┌────────────────────────────────────────────┐
  │  KAMIS API (한국농수산식품유통공사)            │
  │  - 도매시장 가격 정보                         │
  │  - 소매 가격 정보                             │
  │  - 품목별 유통 데이터                          │
  ├────────────────────────────────────────────┤
  │  공공데이터포털 (data.go.kr)                  │
  │  - 보조 공공 데이터                           │
  └────────────────────────────────────────────┘
```

---

## 기술 스택

| 계층 | 기술 | 비고 |
|------|------|------|
| Frontend | HTML5, CSS3, Vanilla JS | 정적 사이트, 다크모드 지원 |
| API Server | Python (FastAPI/Flask) | REST API, JSON 응답 |
| Data Pipeline | Python 3.11 | 수집·정제·분석 파이프라인 |
| HTTP Client | requests (>=2.31.0) | 지수 백오프 재시도 내장 |
| Data Store | SQLite / PostgreSQL | 가격 데이터 영속 저장 |
| Cache | Redis / In-memory | API 응답 캐시 (TTL) |
| Scheduler | GitHub Actions / cron | 일일 배치 수집 |
| Config | python-dotenv, JSON | 환경변수 + 설정 파일 |
| Hosting | GitHub Pages + Cloud | 정적사이트 + API 서버 |
| Timezone | KST (UTC+9) | 한국 표준시 기준 |

---

## 품목 코드 매핑 (KAMIS 기준)

| 코드 | 품목 | 가격 단위 |
|------|------|----------|
| 111 | 사과 | 10kg |
| 112 | 배 | 10kg |
| 213 | 딸기 | 2kg |
| 225 | 바나나 | 13kg (수입) |
| 212 | 감귤 | 5kg |

> 전체 품목 코드는 KAMIS 코드 체계를 따릅니다.
