from __future__ import annotations

import re

import pandas as pd

from rag.router import _AGG_COUNT


def _format_pandas_result(result: object) -> str:
    if result is None:
        return "조회된 데이터가 없습니다."
    # numpy scalar
    if hasattr(result, "item"):
        result = result.item()
    if isinstance(result, (int, float)):
        return str(result)
    if isinstance(result, pd.Series):
        result = result.reset_index().to_dict("records")
    if isinstance(result, pd.DataFrame):
        if result.empty:
            return "조회된 데이터가 없습니다."
        return result.to_string(index=False)
    if isinstance(result, list):
        if not result:
            return "조회된 데이터가 없습니다."
        if isinstance(result[0], dict):
            cols = list(result[0].keys())
            lines = [" | ".join(cols), "-" * max(len(" | ".join(cols)), 1)]
            for row in result:
                lines.append(" | ".join(str(row.get(c, "-")) for c in cols))
            return "\n".join(lines)
        return "\n".join(str(r) for r in result)
    return str(result)


def _format_list_result(df: pd.DataFrame) -> str:
    """DataFrame 명단 결과를 LLM 우회로 직접 포맷."""
    if df is None or (hasattr(df, "empty") and df.empty):
        return "조회된 데이터가 없습니다."
    header = f"총 {len(df)}건\n"
    return header + df.to_string(index=False)


def _format_scalar_result(result: object, question: str) -> str:
    """int/float/str scalar를 LLM 없이 자연스러운 문장으로 포맷."""
    if hasattr(result, "item"):
        result = result.item()
    if isinstance(result, int):
        if _AGG_COUNT.search(question):
            return f"총 {result}명입니다."
        return str(result)
    if isinstance(result, float):
        if result == int(result):
            return _format_scalar_result(int(result), question)
        if int(result) >= 10000:
            return f"{int(result) // 10000}만원"
        return str(int(result))
    if isinstance(result, str):
        if re.search(r"\d+만원", result):
            return f"지급 금액은 {result}입니다."
        return result
    return str(result)
