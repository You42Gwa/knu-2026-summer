"""
두 eval.py 결과 JSON을 비교해 Markdown 리포트를 생성합니다.

사용법:
    python compare.py results/0519_1200_experiment.json results/0519_1300_update-models.json
    python compare.py results/0519_1200_experiment.json results/0519_1300_update-models.json --out compare_result.md
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def load(path: str) -> tuple[str, list[dict]]:
    p    = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    tag  = p.stem.split("_", 2)[-1] if "_" in p.stem else p.stem
    return tag, data


def summary(results: list[dict]) -> dict:
    total      = len(results)
    passed     = sum(r["passed"] for r in results)
    route_acc  = sum(r["route_ok"] for r in results) / total if total else 0
    avg_recall = sum(r["keyword_recall"] for r in results) / total if total else 0
    avg_time   = sum(r["elapsed_sec"] for r in results) / total if total else 0
    cats: dict[str, tuple[int, int]] = {}
    for r in results:
        c = r["category"]
        prev = cats.get(c, (0, 0))
        cats[c] = (prev[0] + int(r["passed"]), prev[1] + 1)
    return {
        "total": total, "passed": passed,
        "route_acc": route_acc, "avg_recall": avg_recall, "avg_time": avg_time,
        "cats": cats,
    }


def generate_report(tag_a: str, res_a: list[dict], tag_b: str, res_b: list[dict]) -> str:
    sa = summary(res_a)
    sb = summary(res_b)

    def pct(n, d):
        return f"{n/d:.0%}" if d else "—"

    lines = [
        "# 브랜치 비교 리포트",
        "",
        f"| 항목 | `{tag_a}` | `{tag_b}` | 차이 |",
        "|------|:---------:|:---------:|:----:|",
        f"| 통과율 | **{pct(sa['passed'], sa['total'])}** ({sa['passed']}/{sa['total']}) "
        f"| **{pct(sb['passed'], sb['total'])}** ({sb['passed']}/{sb['total']}) "
        f"| {sa['passed'] - sb['passed']:+d} |",
        f"| 라우팅 정확도 | {sa['route_acc']:.0%} | {sb['route_acc']:.0%} "
        f"| {(sa['route_acc']-sb['route_acc'])*100:+.0f}pp |",
        f"| 평균 KW 재현율 | {sa['avg_recall']:.0%} | {sb['avg_recall']:.0%} "
        f"| {(sa['avg_recall']-sb['avg_recall'])*100:+.0f}pp |",
        f"| 평균 응답 시간 | {sa['avg_time']:.1f}s | {sb['avg_time']:.1f}s "
        f"| {sa['avg_time']-sb['avg_time']:+.1f}s |",
        "",
        "## 카테고리별 비교",
        "",
        f"| 카테고리 | `{tag_a}` | `{tag_b}` |",
        "|----------|:---------:|:---------:|",
    ]

    all_cats = sorted(set(sa["cats"]) | set(sb["cats"]))
    for cat in all_cats:
        pa, ta = sa["cats"].get(cat, (0, 0))
        pb, tb = sb["cats"].get(cat, (0, 0))
        cell_a = f"{pa}/{ta} ({pct(pa, ta)})" if ta else "—"
        cell_b = f"{pb}/{tb} ({pct(pb, tb)})" if tb else "—"
        lines.append(f"| {cat} | {cell_a} | {cell_b} |")

    # 승패 분석 (같은 ID 비교)
    idx_a = {r["id"]: r for r in res_a}
    idx_b = {r["id"]: r for r in res_b}
    common_ids = sorted(set(idx_a) & set(idx_b))

    a_wins = [i for i in common_ids if idx_a[i]["passed"] and not idx_b[i]["passed"]]
    b_wins = [i for i in common_ids if idx_b[i]["passed"] and not idx_a[i]["passed"]]
    both_fail = [i for i in common_ids if not idx_a[i]["passed"] and not idx_b[i]["passed"]]

    lines += [
        "",
        f"## 케이스별 승패 (공통 {len(common_ids)}개)",
        "",
        f"- `{tag_a}` 만 통과: **{len(a_wins)}건**",
        f"- `{tag_b}` 만 통과: **{len(b_wins)}건**",
        f"- 둘 다 실패: **{len(both_fail)}건**",
    ]

    if a_wins:
        lines += ["", f"### `{tag_a}` 만 통과한 케이스"]
        for i in a_wins:
            r = idx_a[i]
            lines.append(f"- [{i}] {r['question'][:50]}  (kw:{r['keyword_recall']:.0%})")

    if b_wins:
        lines += ["", f"### `{tag_b}` 만 통과한 케이스"]
        for i in b_wins:
            r = idx_b[i]
            lines.append(f"- [{i}] {r['question'][:50]}  (kw:{r['keyword_recall']:.0%})")

    if both_fail:
        lines += ["", "### 둘 다 실패한 케이스"]
        for i in both_fail:
            ra, rb = idx_a[i], idx_b[i]
            lines.append(
                f"- [{i}] {ra['question'][:45]}  "
                f"({tag_a} kw:{ra['keyword_recall']:.0%} / {tag_b} kw:{rb['keyword_recall']:.0%})"
            )

    return "\n".join(lines)


def main() -> None:
    if len(sys.argv) < 3:
        print("사용법: python compare.py <결과A.json> <결과B.json> [--out report.md]")
        sys.exit(1)

    path_a = sys.argv[1]
    path_b = sys.argv[2]
    out    = None
    if "--out" in sys.argv:
        idx = sys.argv.index("--out")
        if idx + 1 < len(sys.argv):
            out = sys.argv[idx + 1]

    tag_a, res_a = load(path_a)
    tag_b, res_b = load(path_b)

    report = generate_report(tag_a, res_a, tag_b, res_b)
    print(report)

    if out:
        Path(out).write_text(report, encoding="utf-8")
        print(f"\n저장: {out}")
    else:
        default = Path(path_a).parent / f"compare_{tag_a}_vs_{tag_b}.md"
        default.write_text(report, encoding="utf-8")
        print(f"\n저장: {default}")


if __name__ == "__main__":
    main()
