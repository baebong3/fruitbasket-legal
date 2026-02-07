"""
KAMIS 농산물 가격 분석 시스템 - 메인 실행
1. KAMIS API에서 데이터 수집 (병렬 처리)
2. SQLite DB에 누적 저장
3. 엑셀 파일에 누적 저장
4. 가격 분석 리포트 생성
"""

import argparse
from datetime import datetime

import kamis_collector as collector
import kamis_database as db
import kamis_analysis as analysis


def run_collect(args):
    """데이터 수집 + DB 저장 + 엑셀 저장."""
    end_date = datetime.now()
    start_date = datetime(end_date.year - 1, end_date.month, 1)

    data_dict = collector.collect_all_data(
        product_cls_code=args.cls_code,
        start_month=start_date.strftime("%Y%m"),
        end_month=end_date.strftime("%Y%m"),
        start_year=str(end_date.year - 5),
        end_year=str(end_date.year),
        max_workers=args.workers,
    )

    if data_dict is None:
        print("\n데이터 수집 실패.")
        return

    # DB 저장
    conn = db.get_connection()
    db.init_db(conn)
    db.save_all(data_dict, conn=conn)

    stats = db.get_db_stats(conn=conn)
    print(f"\n[DB 현황] 일별: {stats['daily_prices']}건 | "
          f"월별: {stats['monthly_prices']}건 | 연별: {stats['yearly_prices']}건")
    conn.close()

    # 엑셀 누적 저장
    filepath = collector.save_to_excel(data_dict, cumulative=True)
    if filepath:
        print(f"\n엑셀 저장 완료: {filepath}")


def run_analyze(args):
    """분석 리포트 생성."""
    report_path = analysis.generate_report()
    if report_path:
        print(f"\n분석 리포트: {report_path}")


def run_all(args):
    """수집 + 분석 전체 실행."""
    run_collect(args)
    run_analyze(args)


def run_status(args):
    """DB 현황 조회."""
    conn = db.get_connection()
    db.init_db(conn)

    stats = db.get_db_stats(conn=conn)
    products = db.get_all_products(conn=conn)

    print("\n" + "=" * 40)
    print("KAMIS DB 현황")
    print("=" * 40)
    print(f"  일별 가격 데이터: {stats['daily_prices']}건")
    print(f"  월별 가격 데이터: {stats['monthly_prices']}건")
    print(f"  연별 가격 데이터: {stats['yearly_prices']}건")
    print(f"  등록 품목 수: {len(products)}개")

    if not products.empty:
        print("\n[등록 품목]")
        for _, row in products.head(20).iterrows():
            print(f"  - {row['category_name']} > {row['product_name']} ({row['item_name']}) [{row['unit']}]")
        if len(products) > 20:
            print(f"  ... 외 {len(products) - 20}개")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="KAMIS 농산물 가격 분석 시스템",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python kamis_main.py collect          # 데이터 수집 + DB/엑셀 저장
  python kamis_main.py analyze          # 분석 리포트 생성
  python kamis_main.py all              # 수집 + 분석 전체 실행
  python kamis_main.py status           # DB 현황 조회

환경변수:
  KAMIS_CERT_KEY    KAMIS API 인증키
  KAMIS_CERT_ID     KAMIS API 인증 ID

.env 파일에 설정하거나 환경변수로 직접 지정할 수 있습니다.
""",
    )

    subparsers = parser.add_subparsers(dest="command", help="실행할 명령")

    # collect
    p_collect = subparsers.add_parser("collect", help="데이터 수집")
    p_collect.add_argument("--cls-code", default="01", choices=["01", "02"],
                           help="01=소매, 02=도매 (기본: 01)")
    p_collect.add_argument("--workers", type=int, default=10,
                           help="병렬 처리 스레드 수 (기본: 10)")
    p_collect.set_defaults(func=run_collect)

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="분석 리포트 생성")
    p_analyze.set_defaults(func=run_analyze)

    # all
    p_all = subparsers.add_parser("all", help="수집 + 분석 전체 실행")
    p_all.add_argument("--cls-code", default="01", choices=["01", "02"],
                       help="01=소매, 02=도매 (기본: 01)")
    p_all.add_argument("--workers", type=int, default=10,
                       help="병렬 처리 스레드 수 (기본: 10)")
    p_all.set_defaults(func=run_all)

    # status
    p_status = subparsers.add_parser("status", help="DB 현황 조회")
    p_status.set_defaults(func=run_status)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()
