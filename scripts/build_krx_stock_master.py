import argparse
import json
import warnings
from pathlib import Path

import pandas as pd
import FinanceDataReader as fdr


def _read_master_xlsx(path: Path, market: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    # Normalize column names (strip whitespace)
    df.columns = [str(c).strip() for c in df.columns]

    required = ["종목코드", "종목명", "업종(대분류)", "업종(중분류)", "업종(소분류)", "발행주식수"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {path.name}: {missing}")

    shares = (
        pd.to_numeric(
            df["발행주식수"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip(),
            errors="coerce",
        )
        .round()
    )
    # Ensure JSON-serializable python ints (or None)
    shares_py = [int(x) if pd.notna(x) else None for x in shares.tolist()]

    out = pd.DataFrame(
        {
            "Code": df["종목코드"].astype(str).str.strip().str.zfill(6),
            "Name": df["종목명"].astype(str).str.strip(),
            "Market": market,
            "IndustryLarge": df["업종(대분류)"].astype(str).str.strip(),
            "IndustryMid": df["업종(중분류)"].astype(str).str.strip(),
            "IndustrySmall": df["업종(소분류)"].astype(str).str.strip(),
            "SharesOutstanding": shares_py,
        }
    )
    out = out.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"])
    return out


def _fetch_etf_data() -> pd.DataFrame:
    """Fetch ETF data from FinanceDataReader."""
    try:
        df_etf = fdr.StockListing('ETF/KR')
        if df_etf.empty:
            warnings.warn("No ETF data fetched from FDR")
            return pd.DataFrame()
        
        # Map ETF columns to master format
        # ETF data has: Symbol, Name, and other fields
        etf_master = pd.DataFrame({
            'Code': df_etf['Symbol'].astype(str).str.strip().str.zfill(6),
            'Name': df_etf['Name'].astype(str).str.strip(),
            'Market': 'ETF',
            'IndustryLarge': None,
            'IndustryMid': None,
            'IndustrySmall': None,
            'SharesOutstanding': None,
        })
        
        etf_master = etf_master.dropna(subset=["Code"]).drop_duplicates(subset=["Code", "Market"])
        print(f"Fetched {len(etf_master)} ETF entries from FinanceDataReader")
        return etf_master
        
    except Exception as e:
        warnings.warn(f"Failed to fetch ETF data: {str(e)}")
        return pd.DataFrame()


def main() -> None:
    p = argparse.ArgumentParser(description="Build KRX stock master JSON from Seibro Excel files and FDR ETF data")
    p.add_argument("--kospi-xlsx", type=str, default="/workspace/data/kospi.xlsx")
    p.add_argument("--kosdaq-xlsx", type=str, default="/workspace/data/kosdaq.xlsx")
    p.add_argument("--output-json", type=str, default="/workspace/data/krx_stock_master.json")
    p.add_argument("--include-etf", action="store_true", default=True, help="Include ETF data from FinanceDataReader (default: True)")
    p.add_argument("--no-etf", dest="include_etf", action="store_false", help="Exclude ETF data")
    args = p.parse_args()

    kospi = _read_master_xlsx(Path(args.kospi_xlsx), market="KOSPI")
    kosdaq = _read_master_xlsx(Path(args.kosdaq_xlsx), market="KOSDAQ")

    master = pd.concat([kospi, kosdaq], ignore_index=True)
    
    # Fetch and add ETF data if requested
    if args.include_etf:
        etf_data = _fetch_etf_data()
        if not etf_data.empty:
            master = pd.concat([master, etf_data], ignore_index=True)
    
    master = master.sort_values(["Market", "Code"]).reset_index(drop=True)

    out_path = Path(args.output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    records = master.to_dict(orient="records")
    out_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {len(master)} rows -> {out_path}")
    print(f"  KOSPI: {len(kospi)}, KOSDAQ: {len(kosdaq)}, ETF: {len(master) - len(kospi) - len(kosdaq)}")


if __name__ == "__main__":
    main()

