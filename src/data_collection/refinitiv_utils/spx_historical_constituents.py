import time
import pandas as pd
from pathlib import Path
from typing import Optional

try:
    import lseg.data as ld
    LSEG_AVAILABLE = True
except ImportError:
    LSEG_AVAILABLE = False
    print("WARNING: lseg.data not available. This script requires Refinitiv workspace environment.")

def _rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Tolerant column rename to standardize identifier columns"""
    cols = df.columns.tolist()
    m = {}
    
    # Find RIC column
    ric = [c for c in cols if c.lower() in {"instrument", "ric", "ric code", "instrument ric"}] \
          or [c for c in cols if "ric" in c.lower()]
    if ric: 
        m[ric[0]] = "RIC"
    
    # Find ISIN column
    isin = [c for c in cols if c.lower() in {"tr.isin", "isin", "isin code"}] \
           or [c for c in cols if "isin" in c.lower()]
    if isin: 
        m[isin[0]] = "ISIN"
    
    # Find CUSIP column
    cusip = [c for c in cols if c.lower() in {"tr.cusip", "cusip", "cusip code"}] \
            or [c for c in cols if "cusip" in c.lower()]
    if cusip: 
        m[cusip[0]] = "CUSIP"
    
    return df.rename(columns=m)

def _fallback_month(m_end: pd.Timestamp, d_iso: str, d_y4md: str, 
                   batch_size: int = 300, verbose: bool = True) -> Optional[pd.DataFrame]:
    """
    Fallback method: Get chain constituents then bulk ID call for that month.
    
    Args:
        m_end: End of month timestamp
        d_iso: Date in ISO format (YYYY-MM-DD)
        d_y4md: Date in YYYYMMDD format
        batch_size: Batch size for ID requests
        verbose: Whether to print progress messages
        
    Returns:
        DataFrame with Date, RIC, ISIN, CUSIP columns or None if failed
    """
    if not LSEG_AVAILABLE:
        return None
        
    # 1) Get the ~500 RICs from the chain
    try:
        snap = ld.get_data(
            universe=[f"0#.SPX({d_y4md})"],
            fields=["TR.PriceClose"],
            parameters={"SDATE": d_iso, "EDATE": d_iso}
        )
        if snap is None or snap.empty or "Instrument" not in snap.columns:
            return None
        
        rics = snap["Instrument"].astype(str).dropna().unique().tolist()
        if not rics:
            return None
    except Exception as e:
        if verbose:
            print(f"    [fallback] Chain request failed: {e}")
        return None

    # 2) Try a single bulk ID call first; if empty, do simple batching
    ids_all = []
    try:
        one_shot = ld.get_data(
            universe=rics,
            fields=["TR.ISIN", "TR.CUSIP"],
            parameters={"SDATE": d_iso, "EDATE": d_iso}
        )
        if one_shot is not None and not one_shot.empty:
            ids_all = [one_shot]
    except Exception:
        ids_all = []

    if not ids_all:
        # Fall back to batches
        for j in range(0, len(rics), batch_size):
            batch = rics[j:j+batch_size]
            try:
                df = ld.get_data(
                    universe=batch,
                    fields=["TR.ISIN", "TR.CUSIP"],
                    parameters={"SDATE": d_iso, "EDATE": d_iso}
                )
                if df is None or df.empty:
                    continue
                ids_all.append(df)
                if verbose:
                    print(f"    [fallback IDs] {j+1:>3}-{min(j+batch_size,len(rics)):<3} / {len(rics)}", flush=True)
            except Exception:
                continue

    if not ids_all:
        # Return rows with missing IDs rather than crashing
        return pd.DataFrame({
            "Date": [m_end] * len(rics), 
            "RIC": rics, 
            "ISIN": pd.NA, 
            "CUSIP": pd.NA
        })

    ids = pd.concat(ids_all, ignore_index=True)
    ids = _rename_cols(ids)
    
    # Ensure RIC column exists
    if "RIC" not in ids.columns and "Instrument" in ids.columns:
        ids = ids.rename(columns={"Instrument": "RIC"})
    if "RIC" not in ids.columns:
        return pd.DataFrame({
            "Date": [m_end] * len(rics), 
            "RIC": rics, 
            "ISIN": pd.NA, 
            "CUSIP": pd.NA
        })

    ids["Date"] = m_end
    keep = ["Date", "RIC"] + [c for c in ["ISIN", "CUSIP"] if c in ids.columns]
    return ids[keep].drop_duplicates(["Date", "RIC"])

def collect_spx_data(start: str = '2005-01-01', 
                    end: str = '2025-12-31',
                    batch_size: int = 300, 
                    verbose: bool = True, 
                    checkpoint_path: Optional[str] = None,
                    output_file: Optional[str] = None) -> pd.DataFrame:
    """
    Download S&P 500 historical constituent data with identifiers.
    
    Uses per-month chain-with-IDs approach. If a specific month errors or lacks IDs, 
    falls back for that month only to: chain -> one (batched) ID call.
    Continues past bad months; checkpoints every 12 months if path set.
    
    Args:
        start: Start date (YYYY-MM-DD format)
        end: End date (YYYY-MM-DD format) 
        batch_size: Batch size for API requests
        verbose: Whether to print progress messages
        checkpoint_path: Path to save checkpoint parquet files (optional)
        output_file: Path to save final Excel file (optional)
        
    Returns:
        DataFrame with columns: Date, RIC, ISIN, CUSIP, Ticker
        
    Raises:
        ImportError: If lseg.data is not available
        Exception: If unable to open LSEG session
    """
    if not LSEG_AVAILABLE:
        raise ImportError("lseg.data package not available. Must run in Refinitiv workspace environment.")
    
    # Open session
    try:
        ld.open_session()
        if verbose:
            print("Connected to LSEG. Let's do this.")
    except Exception as e:
        raise Exception(f"LSEG session failed: {e}")
    
    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)
    months = pd.date_range(start=start_date, end=end_date, freq='M')

    if verbose:
        print(f"Collecting S&P 500 data from {start} to {end}")
        print(f"That's {len(months)} months of data. Grab some coffee.")

    rows = []
    t0 = time.time()

    # Probe once so we know if chain-with-IDs usually works
    probe = months[0]
    probe_str = probe.strftime("%Y%m%d")
    probe_iso = probe.strftime("%Y-%m-%d")
    
    try:
        probe_df = ld.get_data(
            universe=[f"0#.SPX({probe_str})"],
            fields=["TR.ISIN", "TR.CUSIP"],
            parameters={"SDATE": probe_iso, "EDATE": probe_iso}
        )
        chain_has_ids = (probe_df is not None and not probe_df.empty and
                         any("isin" in c.lower() for c in probe_df.columns))
    except Exception:
        chain_has_ids = False
        
    if verbose:
        if chain_has_ids:
            print("Good news: we can get IDs directly from chains. This'll be fast.")
        else:
            print("Chains don't have IDs. We'll do this the slow way.")

    # Progress tracking (with tqdm if available)
    try:
        from tqdm.auto import tqdm
        month_iter = tqdm(months, desc="Months", leave=True)
    except ImportError:
        month_iter = months

    for idx, m_end in enumerate(month_iter, 1):
        d_y4md = m_end.strftime("%Y%m%d")
        d_iso = m_end.strftime("%Y-%m-%d")
        
        if verbose and 'tqdm' not in str(type(month_iter)):
            print(f"[{idx:03d}/{len(months)}] {d_iso} …", flush=True)

        month_df = None
        
        if chain_has_ids:
            # Try chain-with-IDs for this month; if it errors/empty/odd, fall back
            try:
                snap = ld.get_data(
                    universe=[f"0#.SPX({d_y4md})"],
                    fields=["TR.ISIN", "TR.CUSIP"],
                    parameters={"SDATE": d_iso, "EDATE": d_iso}
                )
                if snap is not None and not snap.empty:
                    snap = _rename_cols(snap)
                    if "RIC" not in snap.columns and "Instrument" in snap.columns:
                        snap = snap.rename(columns={"Instrument": "RIC"})
                    if "RIC" in snap.columns:
                        snap["Date"] = m_end
                        keep = ["Date", "RIC"] + [c for c in ["ISIN", "CUSIP"] if c in snap.columns]
                        month_df = snap[keep]
                    else:
                        if verbose: 
                            print("  -> no RIC column found, trying plan B", flush=True)
                else:
                    if verbose: 
                        print("  -> got nothing back, trying plan B", flush=True)
            except Exception as e:
                if verbose: 
                    print(f"  -> that didn't work ({e}), trying plan B", flush=True)

        if month_df is None:
            # Per-month fallback (chain → IDs)
            fb = _fallback_month(m_end, d_iso, d_y4md, batch_size, verbose)
            if fb is None or fb.empty:
                if verbose: 
                    print("  -> plan B also failed, skipping this month", flush=True)
                continue
            month_df = fb

        rows.append(month_df)

        # Checkpoint every 12 months
        if checkpoint_path and idx % 12 == 0 and rows:
            ck = pd.concat(rows, ignore_index=True).sort_values(["Date", "RIC"])
            ck.to_parquet(checkpoint_path, index=False)
            if verbose:
                elapsed = time.time() - t0
                print(f"Checkpoint saved: {len(ck):,} rows so far ({elapsed:,.1f}s elapsed)", flush=True)

    if not rows:
        print("Well, that didn't work. Got zero data.")
        return pd.DataFrame(columns=["Date", "RIC", "ISIN", "CUSIP", "Ticker"])

    # Combine all data
    out = pd.concat(rows, ignore_index=True).drop_duplicates(["Date", "RIC"])
    out["Ticker"] = out["RIC"].str.extract(r"^([^\.]+)")[0].str.upper()

    # Summary statistics
    by_m = out.groupby(out["Date"].dt.to_period("M"))["RIC"].nunique()
    elapsed = time.time() - t0
    
    print(f"\nDone! Here's what we got:")
    print(f"=" * 40)
    print(f"Monthly companies: {int(by_m.min())} to {int(by_m.max())} (avg {by_m.mean():.1f})")
    print(f"Unique companies: {out['RIC'].nunique()}")
    print(f"Total rows: {len(out):,}")
    print(f"Time span: {out['Date'].min().date()} to {out['Date'].max().date()}")
    print(f"Time taken: {elapsed:,.1f} seconds")
    
    if "ISIN" in out.columns:
        isin_coverage = (1 - out['ISIN'].isna().mean()) * 100
        print(f"ISIN coverage: {isin_coverage:.1f}% (not bad)")
    if "CUSIP" in out.columns:
        cusip_coverage = (1 - out['CUSIP'].isna().mean()) * 100
        print(f"CUSIP coverage: {cusip_coverage:.1f}% (decent)")

    # Save to file if requested
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if output_path.suffix.lower() == '.xlsx':
            out.to_excel(output_path, index=False)
        elif output_path.suffix.lower() == '.parquet':
            out.to_parquet(output_path, index=False)
        else:
            out.to_csv(output_path, index=False)
            
        print(f"Saved to: {output_path}")

    return out

def main():
    """Main function for standalone execution"""
    # Configuration
    START_DATE = '2005-01-01'
    END_DATE = '2025-12-31'
    BATCH_SIZE = 5000
    CHECKPOINT_PATH = 'spx_checkpoint.parquet'
    OUTPUT_FILE = 'spx_historical_constituents_with_identifiers.xlsx'
    
    print("S&P 500 Historical Constituents Collection")
    print("=" * 50)
    print("This script grabs S&P 500 constituent data with identifiers")
    print("Only works in Refinitiv Workspace (obviously)\n")
    
    try:
        data = collect_spx_data(
            start=START_DATE,
            end=END_DATE,
            batch_size=BATCH_SIZE,
            verbose=True,
            checkpoint_path=CHECKPOINT_PATH,
            output_file=OUTPUT_FILE
        )
        
        print(f"\nAll done! Got {len(data):,} rows of data.")
        
    except Exception as e:
        print(f"Something went wrong: {e}")
        raise

if __name__ == "__main__":
    main()
