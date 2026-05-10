"""
데이터 정합성 확인 스크립트

수집된 파일별로 PostgreSQL 테이블 수, Chroma 청크 수, manifest 상태를 교차 검증한다.

사용법:
    python check_integrity.py
    python check_integrity.py --verbose   # 테이블 목록 포함 출력
"""

from __future__ import annotations

import argparse
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from sqlalchemy import text, inspect
from database import engine, get_chroma_collection


def get_manifest_rows() -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT source, file_type, category, status, chroma_doc_count, error_message
            FROM ingestion_manifest
            ORDER BY source
        """)).fetchall()
    return [dict(r._mapping) for r in rows]


def get_postgres_tables_by_source() -> dict[str, list[str]]:
    """source 파일명(stem) → 관련 테이블 목록 매핑."""
    inspector = inspect(engine)
    all_tables = inspector.get_table_names(schema="public")
    result: dict[str, list[str]] = {}
    for tbl in all_tables:
        if tbl == "ingestion_manifest":
            continue
        # manifest_source 컬럼을 가진 테이블에서 source 값 수집
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f'SELECT DISTINCT manifest_source FROM public."{tbl}" LIMIT 1')
                ).fetchall()
            for row in rows:
                src = row[0]
                if src:
                    result.setdefault(src, []).append(tbl)
        except Exception:
            pass
    return result


def get_chroma_counts_by_source() -> dict[str, int]:
    """source 파일명 → Chroma 청크 수 매핑."""
    collection = get_chroma_collection("scholarship_rules")
    data = collection.get(include=["metadatas"])
    counts: dict[str, int] = {}
    for meta in data.get("metadatas") or []:
        src = meta.get("source", "")
        if src:
            counts[src] = counts.get(src, 0) + 1
    return counts


def check_integrity(verbose: bool = False) -> None:
    print("데이터 정합성 검사 시작...\n")

    manifest_rows = get_manifest_rows()
    if not manifest_rows:
        print("ingestion_manifest 테이블이 비어 있습니다.")
        return

    print("PostgreSQL 테이블 스캔 중...")
    pg_tables = get_postgres_tables_by_source()

    print("ChromaDB 청크 수 집계 중...\n")
    chroma_counts = get_chroma_counts_by_source()

    # 헤더
    print(f"{'파일':<40} {'타입':<6} {'상태':<12} {'PG테이블':>7} {'Chroma':>7} {'manifest':>8}  판정")
    print("-" * 100)

    issues: list[str] = []

    for row in manifest_rows:
        source = row["source"]
        ftype = row["file_type"] or "-"
        status = row["status"]
        manifest_chroma = row["chroma_doc_count"] or 0

        pg_tbl_list = pg_tables.get(source, [])
        pg_count = len(pg_tbl_list)
        chroma_actual = chroma_counts.get(source, 0)

        # 판정
        flags: list[str] = []
        if status == "FAILED":
            flags.append("FAILED")
        if status == "SUCCESS":
            if ftype in ("pdf", "hwp", "xlsx") and chroma_actual == 0:
                flags.append("Chroma=0")
            if ftype in ("pdf", "hwp") and chroma_actual != manifest_chroma:
                flags.append(f"Chroma불일치({manifest_chroma}→{chroma_actual})")
            if ftype in ("pdf", "hwp", "xlsx") and pg_count == 0 and ftype != "hwp":
                # hwp는 표가 없을 수 있어 pg_count=0도 허용
                pass

        verdict = "✅" if not flags else "⚠️  " + ", ".join(flags)

        print(
            f"{source[:39]:<40} {ftype:<6} {status:<12} {pg_count:>7} {chroma_actual:>7} {manifest_chroma:>8}  {verdict}"
        )

        if verbose and pg_tbl_list:
            for tbl in sorted(pg_tbl_list):
                print(f"    └ {tbl}")

        if flags:
            issues.append(f"[{source}] {', '.join(flags)}")

    print("\n" + "=" * 100)
    total = len(manifest_rows)
    ok_count = sum(1 for r in manifest_rows if r["status"] == "SUCCESS")
    print(f"  전체 파일: {total}개  |  SUCCESS: {ok_count}  |  문제: {len(issues)}")

    if issues:
        print(f"\n  문제 파일 ({len(issues)}개):")
        for iss in issues:
            print(f"    {iss}")
    else:
        print("  모든 파일 정상.")

    chroma_orphans = set(chroma_counts) - {r["source"] for r in manifest_rows}
    if chroma_orphans:
        print(f"\n  Chroma에만 존재(manifest 없음): {sorted(chroma_orphans)}")

    pg_orphans = set(pg_tables) - {r["source"] for r in manifest_rows}
    if pg_orphans:
        print(f"\n  PostgreSQL 테이블만 존재(manifest 없음): {sorted(pg_orphans)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="데이터 정합성 확인")
    parser.add_argument("--verbose", "-v", action="store_true", help="테이블 목록 포함 출력")
    args = parser.parse_args()
    check_integrity(verbose=args.verbose)
