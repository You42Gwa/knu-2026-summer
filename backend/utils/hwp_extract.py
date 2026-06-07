"""
HWP 표 추출 헬퍼 — subprocess로 독립 실행하여 COM 스레드 격리.
사용: python hwp_extract.py <hwp_file_path>
결과: JSON (records 형식) → stdout
"""
import json
import os
import sys

from bs4 import BeautifulSoup
from pyhwpx import Hwp


def extract(file_path: str) -> list[dict]:
    temp_html = file_path + "._tmp.html"
    hwp = None
    try:
        hwp = Hwp()
        hwp.open(os.path.abspath(file_path))
        hwp.save_as(temp_html, "HTML")
        hwp.quit()
        hwp = None

        with open(temp_html, "rb") as f:
            raw = f.read()

        import re as _re
        m = _re.search(rb'charset=["\']?([A-Za-z0-9\-]+)', raw)
        detected_enc = m.group(1).decode("ascii", errors="replace") if m else None

        soup = None
        for enc in filter(None, [detected_enc, "utf-8-sig", "cp949", "euc-kr", "utf-8"]):
            try:
                soup = BeautifulSoup(raw.decode(enc), "html.parser")
                break
            except (UnicodeDecodeError, LookupError):
                continue
        if soup is None:
            soup = BeautifulSoup(raw.decode("cp949", errors="replace"), "html.parser")

        table = soup.find("table")
        if not table:
            return []

        rows = table.find_all("tr")
        num_rows = len(rows)
        num_cols = max(
            sum(int(c.get("colspan", 1)) for c in row.find_all(["td", "th"]))
            for row in rows
        )
        grid = [[None] * num_cols for _ in range(num_rows)]

        for r_idx, tr in enumerate(rows):
            c_idx = 0
            for td in tr.find_all(["td", "th"]):
                while c_idx < num_cols and grid[r_idx][c_idx] is not None:
                    c_idx += 1
                if c_idx >= num_cols:
                    break
                rowspan = int(td.get("rowspan", 1))
                colspan = int(td.get("colspan", 1))
                paras = [p.get_text(strip=True) for p in td.find_all("p") if p.get_text(strip=True)]
                content = "\n".join(paras) if paras else td.get_text(strip=True)
                for r in range(r_idx, min(r_idx + rowspan, num_rows)):
                    for c in range(c_idx, min(c_idx + colspan, num_cols)):
                        grid[r][c] = content  # Context Padding: 병합 셀 내용을 모든 범위에 전파
                c_idx += colspan

        if not grid or len(grid) < 2:
            return []

        headers = [str(v).strip() if v else f"col_{i}" for i, v in enumerate(grid[0])]
        records = []
        for row in grid[1:]:
            rec = {}
            for h, v in zip(headers, row):
                rec[h] = str(v).strip() if v and str(v).strip() not in ("", "nan") else None
            records.append(rec)
        return records

    finally:
        if hwp:
            try:
                hwp.quit()
            except Exception:
                pass
        if os.path.exists(temp_html):
            os.remove(temp_html)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    if len(sys.argv) < 2:
        print("[]")
        sys.exit(0)
    try:
        records = extract(sys.argv[1])
        print(json.dumps(records, ensure_ascii=False))
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}\n")
        print("[]")
