import argparse
import json
from pathlib import Path

import pandas as pd


def _read_master_xlsx(path: Path, market: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    # Normalize column names (strip whitespace)
    df.columns = [str(c).strip() for c in df.columns]

    required = ["종목코드", "종목명", "업종(대분류)", "업종(중분류)", "업종(소분류)"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {path.name}: {missing}")

    out = pd.DataFrame(
        {
            "Code": df["종목코드"].astype(str).str.strip().str.zfill(6),
            "Name": df["종목명"].astype(str).str.strip(),
            "Market": market,
            "IndustryLarge": df["업종(대분류)"].astype(str).str.strip(),
            "IndustryMid": df["업종(중분류)"].astype(str).str.strip(),
            "IndustrySmall": df["업종(소분류)"].astype(str).str.strip(),
        }
    )
    out = out.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"])
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Build KRX stock master JSON from Seibro Excel files")
    p.add_argument("--kospi-xlsx", type=str, default="/workspace/data/코스피.xlsx")
    p.add_argument("--kosdaq-xlsx", type=str, default="/workspace/data/코스닥.xlsx")
    p.add_argument("--output-json", type=str, default="/workspace/data/krx_stock_master.json")
    args = p.parse_args()

    kospi = _read_master_xlsx(Path(args.kospi_xlsx), market="KOSPI")
    kosdaq = _read_master_xlsx(Path(args.kosdaq_xlsx), market="KOSDAQ")

    master = pd.concat([kospi, kosdaq], ignore_index=True)
    master = master.sort_values(["Market", "Code"]).reset_index(drop=True)

    out_path = Path(args.output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    records = master.to_dict(orient="records")
    out_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {len(master)} rows -> {out_path}")


if __name__ == "__main__":
    main()

