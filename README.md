# 나라장터 용역 입찰공고 자동 수집기

나라장터(G2B) 입찰공고정보서비스 API를 활용하여 용역 입찰공고를 자동 수집하고 엑셀로 저장하는 도구입니다.

## 기능

- 나라장터 용역 입찰공고 일일 자동 수집
- 검색어 기반 필터링 (config.json에서 관리)
- 검색어별 시트가 분리된 엑셀 파일 생성
- GitHub Actions를 통한 매일 자동 실행

## 설치

```bash
pip install -r requirements.txt
```

## 설정

### 1. API 키 발급

[공공데이터포털](https://www.data.go.kr/)에서 **나라장터 입찰공고정보서비스** API 키를 발급받으세요.

### 2. 환경변수 설정

```bash
cp .env.example .env
# .env 파일에 발급받은 API 키 입력
```

### 3. 검색 조건 설정

`config.json`에서 검색어와 조회 조건을 수정합니다:

```json
{
  "keywords": ["조사", "연구"],
  "inqry_div": "1",
  "num_of_rows": 999,
  "search_period": {
    "start_hour": "0000",
    "end_hour": "2359"
  }
}
```

| 필드 | 설명 |
|------|------|
| `keywords` | 공고명에서 검색할 키워드 목록 |
| `inqry_div` | 조회구분 (1: 용역) |
| `num_of_rows` | 페이지당 조회 건수 |
| `search_period` | 조회 시간 범위 (HHMM 형식) |

## 사용법

```bash
python scraper.py
```

실행 결과는 `output/` 폴더에 `g2b_용역공고_YYYYMMDD.xlsx` 형식으로 저장됩니다.

## 엑셀 출력 구조

- **요약** 시트: 검색어별 매칭 건수
- **검색어별 시트**: 각 검색어에 매칭된 공고 목록

| 컬럼 | 설명 |
|------|------|
| 공고번호 | 입찰 공고 번호 |
| 공고명 | 입찰 공고 제목 |
| 발주기관 | 공고를 낸 기관명 |
| 공고차수 | 공고 차수 |
| 입찰마감일시 | 입찰 마감 일시 |
| 추정가격 | 추정 가격 |
| 배정예산액 | 배정 예산 금액 |
| 공고일시 | 공고 등록 일시 |
| 재입찰허용여부 | 재입찰 허용 여부 |
| 공고URL | 나라장터 공고 상세 링크 |

## GitHub Actions 자동화

매일 한국시간 09:00 (UTC 00:00)에 자동으로 실행됩니다.

### 설정 방법

1. GitHub 저장소의 **Settings > Secrets and variables > Actions**로 이동
2. `G2B_API_KEY` 시크릿에 API 키 등록
3. 수동 실행: **Actions** 탭에서 workflow를 선택 후 **Run workflow** 클릭

생성된 엑셀 파일은 Actions 실행 결과의 **Artifacts**에서 30일간 다운로드 가능합니다.
