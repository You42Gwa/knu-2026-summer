"""
골드셋 기반 RAG 시스템 평가 스크립트

사용법:
    python eval.py                        # 전체 테스트
    python eval.py --id TC001            # 특정 케이스만
    python eval.py --category sql_명단   # 카테고리 필터
    python eval.py --url http://...      # 서버 주소 변경
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

GOLDSET_PATH = Path(__file__).parent / "goldset.json"
DEFAULT_URL = "http://localhost:8080"


# ---------------------------------------------------------------------------
# 채점 로직
# ---------------------------------------------------------------------------

def score_keywords(answer: str, keywords: list[str]) -> tuple[float, list[str], list[str]]:
    """키워드 중 몇 개가 답변에 포함됐는지 확인 (대소문자·공백 무시)."""
    # "250만 원" == "250만원" 처럼 공백 차이를 무시
    answer_norm = re.sub(r"\s+", "", answer.lower())
    hit, miss = [], []
    for kw in keywords:
        kw_norm = re.sub(r"\s+", "", kw.lower())
        (hit if kw_norm in answer_norm else miss).append(kw)
    recall = len(hit) / len(keywords) if keywords else 1.0
    return recall, hit, miss


def evaluate_case(tc: dict[str, Any], base_url: str) -> dict[str, Any]:
    question = tc["question"]
    expected_route = tc["expected_route"]
    keywords = tc["ground_truth_keywords"]

    start = time.perf_counter()
    try:
        resp = requests.post(
            f"{base_url}/chat",
            json={"question": question},
            timeout=120,
        )
        elapsed = time.perf_counter() - start
        resp.raise_for_status()
        data = resp.json()
        answer = data.get("answer", "")
        actual_route = data.get("source", "unknown")
        error = None
    except Exception as e:
        elapsed = time.perf_counter() - start
        answer = ""
        actual_route = "error"
        error = str(e)

    route_ok = actual_route == expected_route
    keyword_recall, hit, miss = score_keywords(answer, keywords)

    # 네거티브 케이스: '없다'는 취지의 키워드 중 하나라도 있으면 통과
    if tc["category"] == "negative":
        passed = keyword_recall > 0
    else:
        # 키워드 75% 이상이면 라우팅 무관 통과 (정답을 맞혔으면 경로는 무관)
        # 라우팅이 맞고 키워드 50% 이상이면 통과
        passed = keyword_recall >= 0.75 or (route_ok and keyword_recall >= 0.5)

    return {
        "id": tc["id"],
        "question": question,
        "category": tc["category"],
        "difficulty": tc["difficulty"],
        "expected_route": expected_route,
        "actual_route": actual_route,
        "route_ok": route_ok,
        "keyword_recall": round(keyword_recall, 3),
        "hit_keywords": hit,
        "miss_keywords": miss,
        "passed": passed,
        "elapsed_sec": round(elapsed, 2),
        "answer_preview": answer[:500] if answer else "",
        "error": error,
    }


# ---------------------------------------------------------------------------
# 리포트 출력
# ---------------------------------------------------------------------------

def print_result(r: dict[str, Any], verbose: bool = False) -> None:
    status = "✅" if r["passed"] else "❌"
    route_mark = "✓" if r["route_ok"] else "✗"
    print(
        f"{status} [{r['id']}] {r['question'][:40]:<42}"
        f"  route: {r['actual_route']:6}({route_mark})"
        f"  kw: {r['keyword_recall']:.0%}"
        f"  {r['elapsed_sec']}s"
    )
    if not r["passed"] or verbose:
        if r["miss_keywords"]:
            print(f"     누락 키워드: {r['miss_keywords']}")
        if r["error"]:
            print(f"     오류: {r['error']}")
        if verbose and r["answer_preview"]:
            print(f"     답변: {r['answer_preview']}")


def print_summary(results: list[dict[str, Any]]) -> None:
    total = len(results)
    passed = sum(r["passed"] for r in results)
    route_acc = sum(r["route_ok"] for r in results) / total if total else 0
    avg_recall = sum(r["keyword_recall"] for r in results) / total if total else 0
    avg_time = sum(r["elapsed_sec"] for r in results) / total if total else 0

    print("\n" + "=" * 60)
    print(f"  전체 결과: {passed}/{total} 통과  ({passed/total:.0%})")
    print(f"  라우팅 정확도: {route_acc:.0%}")
    print(f"  평균 키워드 재현율: {avg_recall:.0%}")
    print(f"  평균 응답 시간: {avg_time:.1f}s")
    print("=" * 60)

    # 카테고리별 요약
    cats: dict[str, list] = {}
    for r in results:
        cats.setdefault(r["category"], []).append(r)
    print("\n  카테고리별:")
    for cat, rs in sorted(cats.items()):
        p = sum(x["passed"] for x in rs)
        print(f"    {cat:<20} {p}/{len(rs)} ({p/len(rs):.0%})")

    # 실패 목록
    failed = [r for r in results if not r["passed"]]
    if failed:
        print(f"\n  실패한 케이스 ({len(failed)}개):")
        for r in failed:
            print(f"    [{r['id']}] {r['question'][:50]}")


# ---------------------------------------------------------------------------
# Markdown 리포트 생성
# ---------------------------------------------------------------------------

def generate_markdown_report(results: list[dict[str, Any]], base_url: str) -> str:
    total = len(results)
    passed = sum(r["passed"] for r in results)
    route_acc = sum(r["route_ok"] for r in results) / total if total else 0
    avg_recall = sum(r["keyword_recall"] for r in results) / total if total else 0
    avg_time = sum(r["elapsed_sec"] for r in results) / total if total else 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "# RAG 평가 리포트",
        "",
        f"**생성 일시**: {now}  ",
        f"**서버**: {base_url}  ",
        f"**테스트 케이스**: {total}개",
        "",
        "---",
        "",
        "## 전체 결과",
        "",
        "| 항목 | 값 |",
        "|------|-----|",
        f"| 통과 | **{passed}/{total}** ({passed/total:.0%}) |",
        f"| 라우팅 정확도 | {route_acc:.0%} |",
        f"| 평균 키워드 재현율 | {avg_recall:.0%} |",
        f"| 평균 응답 시간 | {avg_time:.1f}s |",
        "",
        "## 카테고리별 결과",
        "",
        "| 카테고리 | 통과 | 전체 | 정확도 |",
        "|----------|:----:|:----:|:------:|",
    ]

    cats: dict[str, list] = {}
    for r in results:
        cats.setdefault(r["category"], []).append(r)
    for cat, rs in sorted(cats.items()):
        p = sum(x["passed"] for x in rs)
        bar = "🟢" if p / len(rs) >= 0.7 else ("🟡" if p / len(rs) >= 0.4 else "🔴")
        lines.append(f"| {bar} {cat} | {p} | {len(rs)} | {p/len(rs):.0%} |")

    lines += [
        "",
        "## 케이스별 결과",
        "",
        "| ID | 질문 | 카테고리 | 기대→실제 라우팅 | KW 재현율 | 시간 | 결과 |",
        "|----|------|----------|-----------------|:---------:|:----:|:----:|",
    ]

    for r in results:
        status = "✅" if r["passed"] else "❌"
        route_arrow = (
            f"{r['expected_route']}→{r['actual_route']}"
            if not r["route_ok"]
            else r["actual_route"]
        )
        route_mark = "" if r["route_ok"] else " ⚠️"
        q = r["question"][:38].replace("|", "\\|")
        lines.append(
            f"| {r['id']} | {q} | {r['category']} | {route_arrow}{route_mark}"
            f" | {r['keyword_recall']:.0%} | {r['elapsed_sec']}s | {status} |"
        )

    failed = [r for r in results if not r["passed"]]
    if failed:
        lines += [
            "",
            f"## 실패 케이스 상세 ({len(failed)}개)",
            "",
        ]
        for r in failed:
            route_ok_str = "✓" if r["route_ok"] else f"✗ (기대: {r['expected_route']})"
            lines += [
                f"### [{r['id']}] {r['question']}",
                "",
                f"- **카테고리**: {r['category']} · 난이도: {r['difficulty']}",
                f"- **라우팅**: {r['actual_route']} {route_ok_str}",
                f"- **키워드 재현율**: {r['keyword_recall']:.0%}",
                f"- **적중**: {', '.join(r['hit_keywords']) or '없음'}",
                f"- **누락**: {', '.join(r['miss_keywords']) or '없음'}",
                f"- **응답 시간**: {r['elapsed_sec']}s",
            ]
            if r["answer_preview"]:
                preview = r["answer_preview"].replace("\n", " ")[:200]
                lines.append(f"- **답변 미리보기**: {preview}")
            if r["error"]:
                lines.append(f"- **오류**: `{r['error']}`")
            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="RAG 골드셋 평가")
    parser.add_argument("--url", default=DEFAULT_URL, help="FastAPI 서버 주소")
    parser.add_argument("--id", help="특정 테스트케이스 ID만 실행 (예: TC001)")
    parser.add_argument("--category", help="카테고리 필터 (예: sql_명단)")
    parser.add_argument("--verbose", "-v", action="store_true", help="답변 미리보기 출력")
    parser.add_argument("--out", help="결과 JSON 저장 경로 (선택)")
    parser.add_argument("--report", help="Markdown 리포트 저장 경로 (예: report.md)")
    args = parser.parse_args()

    goldset = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
    test_cases = goldset["test_cases"]

    if args.id:
        test_cases = [tc for tc in test_cases if tc["id"] == args.id]
    if args.category:
        test_cases = [tc for tc in test_cases if tc["category"] == args.category]

    if not test_cases:
        print("해당 조건의 테스트케이스가 없습니다.")
        sys.exit(1)

    print(f"서버: {args.url}")
    print(f"테스트: {len(test_cases)}개\n")
    print("-" * 60)

    results = []
    for tc in test_cases:
        r = evaluate_case(tc, args.url)
        results.append(r)
        print_result(r, verbose=args.verbose)

    print_summary(results)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nJSON 저장: {out_path}")

    if args.report:
        report_path = Path(args.report)
        report_md = generate_markdown_report(results, args.url)
        report_path.write_text(report_md, encoding="utf-8")
        print(f"Markdown 리포트 저장: {report_path}")


if __name__ == "__main__":
    main()
