#!/usr/bin/env python3
"""
Build a CRSP/Compustat security master from raw CRSP CSV files.
This creates the security_master_segments.csv file required by the ESG mappers.

Based on PyAnomaly-style security master building with optimizations.
"""

import pandas as pd
import numpy as np
import time
from datetime import datetime
from pathlib import Path
import sys

# -------------------------
# Configuration
# -------------------------
class SecurityMasterConfig:
    """Configuration for security master building"""
    
    @classmethod
    def get_project_root(cls):
        """Get the project root directory"""
        # Go up from src/data_preparation/security_master to project root
        current_dir = Path(__file__).parent
        return current_dir.parent.parent.parent
    
    @classmethod
    def get_names_csv(cls):
        return cls.get_project_root() / "data/raw/crsp/CRSP_STOCK_EVENTS_HIST_DESC_INF_FULL_DB.csv"
    
    @classmethod 
    def get_link_csv(cls):
        return cls.get_project_root() / "data/raw/crsp/CRSP_COMPUSTAT_LINKING_TABLE_FULL_DATABASE.csv"
    
    @classmethod
    def get_output_dir(cls):
        return cls.get_project_root() / "data/processed/security_master"
    
    # Output file names
    SEGMENTS_OUT = "security_master_segments.csv"
    PRIMARY_OUT = "gvkey_to_primary_permno.csv"
    
    @classmethod
    def get_output_paths(cls):
        """Get full output file paths"""
        output_dir = cls.get_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        
        return {
            'segments': output_dir / cls.SEGMENTS_OUT,
            'primary': output_dir / cls.PRIMARY_OUT,
            'dir': output_dir
        }

# -------------------------
# Progress tracking
# -------------------------
def print_progress(step_name, start_time=None):
    """Print progress with optional timing"""
    current_time = datetime.now().strftime("%H:%M:%S")
    if start_time:
        elapsed = time.time() - start_time
        print(f"[{current_time}] ✓ {step_name} - Completed in {elapsed:.2f}s")
    else:
        print(f"[{current_time}] → {step_name}...")
    return time.time()

# -------------------------
# Preferences / constants
# -------------------------
far_future = pd.Timestamp("2262-04-11")

# Rank link types: LU > LC > everything else
LINKTYPE_PRIORITY = {
    "LU": 2,
    "LC": 1,
    # others default to 0
}

# -------------------------
# Helpers
# -------------------------
def norm_gvkey(series: pd.Series) -> pd.Series:
    """Normalize gvkey as zero-padded 6-char string of digits."""
    s = pd.Series(series, dtype="string").str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)  # e.g. '12345.0'
    s = s.str.replace(r"\D", "", regex=True)    # keep digits only
    s = s.where(s.ne(""), pd.NA)
    return s.str.zfill(6)

def check_types(df: pd.DataFrame, name: str):
    """Check data types for debugging"""
    want = {
        "permno": "Int64",
        "permco": "Int64",
        "gvkey": "string",
        "namedt": "datetime64[ns]",
        "nameendt": "datetime64[ns]",
        "linkdt": "datetime64[ns]",
        "linkenddt": "datetime64[ns]",
        "shrcd": "Int64",
        "exchcd": "Int64",
        "is_common": "boolean",
        "comnam": "string",
        "ticker": "string",
        "tsymbol": "string",
        "primexch": "string",
        "shrcls": "string",
        "siccd": "Int64",
        "naics": "Int64",
        "ncusip": "string",
        "ncusip6": "string",
        "cusip": "string",
        "cusip6": "string",
        "linkprim": "string",
        "linktype": "string",
        "linkprim_score": "int64",
        "linktype_rank": "int64",
        "overlap_days": "int64",
        "cusip6_match": "int64",
        "exch": "string",
    }
    print(f"\n[{name}] dtype check:")
    for c, t in want.items():
        if c in df.columns:
            ok = str(df[c].dtype) == t
            print(f"  {c:14s} -> {df[c].dtype!s:14s} {'OK' if ok else 'EXPECTED ' + t}")

def check_input_files():
    """Check if required input files exist"""
    names_path = SecurityMasterConfig.get_names_csv()
    link_path = SecurityMasterConfig.get_link_csv()
    
    missing = []
    if not names_path.exists():
        missing.append(f"NAMES file: {names_path}")
    if not link_path.exists():
        missing.append(f"LINK file: {link_path}")
    
    if missing:
        print("❌ MISSING INPUT FILES:")
        for f in missing:
            print(f"   {f}")
        print("\nPlease ensure you have the raw CRSP files in data/raw/crsp/:")
        print(f"   {names_path}")
        print(f"   {link_path}")
        return False
    
    print("✅ Input files found")
    return True

# -------------------------
# MAIN PROCESSING FUNCTIONS
# -------------------------
def load_and_process_names():
    """Load and process CRSP NAMES file"""
    step_time = print_progress("Step 1: Loading NAMES file")
    names = pd.read_csv(SecurityMasterConfig.get_names_csv(), dtype=str, low_memory=False)
    names.columns = names.columns.str.lower()
    print(f"   Loaded {len(names):,} rows from NAMES")

    step_time = print_progress("Processing NAMES data", step_time)

    # Some exports call NAMEDT just "date"
    if "namedt" not in names.columns and "date" in names.columns:
        names = names.rename(columns={"date": "namedt"})

    # Parse dates (open-ended NAMEENDT -> far future)
    names["namedt"] = pd.to_datetime(names["namedt"], errors="coerce")
    if "nameendt" in names.columns:
        # sometimes 'E' appears; treat as open-ended
        names["nameendt"] = pd.to_datetime(
            names["nameendt"].replace({"E": None}), errors="coerce"
        ).fillna(far_future)
    else:
        names["nameendt"] = far_future

    # Numeric ids
    if "permno" in names.columns:
        names["permno"] = pd.to_numeric(names["permno"], errors="coerce").astype("Int64")
    if "permco" in names.columns:
        names["permco"] = pd.to_numeric(names["permco"], errors="coerce").astype("Int64")

    # Uppercase standard string cols - VECTORIZED
    uppercase_cols = ["comnam", "ticker", "tsymbol", "primexch", "shrcls"]
    for c in uppercase_cols:
        if c in names.columns:
            names[c] = names[c].str.upper()

    # CUSIP handling
    if "ncusip" in names.columns:
        names["ncusip"] = names["ncusip"].str.upper()
        names["ncusip6"] = names["ncusip"].str.slice(0, 6)

    # Optional industry codes as nullable ints
    for c in ["siccd", "naics"]:
        if c in names.columns:
            names[c] = pd.to_numeric(names[c], errors="coerce").astype("Int64")

    # Keep only needed columns (those that exist)
    keep_names = [c for c in [
        "permno", "permco", "namedt", "nameendt",
        "comnam", "ticker", "tsymbol", "shrcls",
        "ncusip", "ncusip6", "shrcd", "exchcd", "primexch", "siccd", "naics"
    ] if c in names.columns]
    names = names[keep_names].dropna(subset=["permno", "namedt"]).copy()
    print(f"   After cleaning: {len(names):,} rows")

    print_progress("NAMES processing complete", step_time)
    return names

def load_and_process_links():
    """Load and process CRSP-Compustat LINK file"""
    step_time = print_progress("Step 2: Loading LINK file")
    links = pd.read_csv(SecurityMasterConfig.get_link_csv(), dtype=str, low_memory=False)
    links.columns = links.columns.str.lower()
    print(f"   Loaded {len(links):,} rows from LINK")

    step_time = print_progress("Processing LINK data", step_time)

    # Standardize PERMNO/PERMCO
    if "lpermno" in links.columns:
        links = links.rename(columns={"lpermno": "permno"})
    if "lpermco" in links.columns and "permco" not in links.columns:
        links = links.rename(columns={"lpermco": "permco"})
    for col in ["permno", "permco"]:
        if col in links.columns:
            links[col] = pd.to_numeric(links[col], errors="coerce").astype("Int64")

    # GVKEY normalization
    if "gvkey" in links.columns:
        links["gvkey"] = norm_gvkey(links["gvkey"])

    # Normalize flags/ids - VECTORIZED
    for c in ["linkprim", "linktype", "cusip"]:
        if c in links.columns:
            links[c] = links[c].astype("string").str.upper()

    # CUSIP6 if present
    if "cusip" in links.columns:
        links["cusip6"] = links["cusip"].str.slice(0, 6)

    # Link dates (fill missing; treat 'E' as open-ended)
    links["linkdt"] = pd.to_datetime(
        links.get("linkdt", pd.Series(index=links.index)), errors="coerce"
    ).fillna(pd.Timestamp("1900-01-01"))
    links["linkenddt"] = pd.to_datetime(
        links.get("linkenddt", pd.Series(index=links.index)).replace({"E": None}),
        errors="coerce"
    ).fillna(far_future)

    # Scores for tie-breaking
    lp = links.get("linkprim", pd.Series(index=links.index, dtype="string"))
    links["linkprim_score"] = lp.isin(["P", "C"]).astype("Int64").fillna(0).astype("int64")

    lt = links.get("linktype", pd.Series(index=links.index, dtype="string"))
    links["linktype_rank"] = lt.map(LINKTYPE_PRIORITY).fillna(0).astype("int64")

    # Retain minimal link columns
    keep_links = [c for c in [
        "permno", "permco", "gvkey",
        "linkdt", "linkenddt",
        "cusip", "cusip6",
        "linkprim", "linktype", "linkprim_score", "linktype_rank"
    ] if c in links.columns or c in ["linkprim_score", "linktype_rank"]]
    links = links[keep_links].dropna(subset=["permno", "linkdt", "linkenddt"]).copy()
    print(f"   After cleaning: {len(links):,} rows")

    print_progress("LINK processing complete", step_time)
    return links

def merge_and_rank(names, links):
    """Merge NAMES and LINKS data with overlap calculation and ranking"""
    step_time = print_progress("Step 3: Merging NAMES and LINK data")
    merged = names.merge(links, on="permno", how="left", suffixes=("", "_link"))
    print(f"   Merged result: {len(merged):,} rows")

    step_time = print_progress("Computing overlaps (VECTORIZED)", step_time)

    # VECTORIZED overlap check
    valid = (merged["namedt"] <= merged["linkenddt"]) & (merged["nameendt"] >= merged["linkdt"])
    merged["overlap_ok"] = valid.fillna(False)

    # VECTORIZED overlap_days calculation - THIS IS THE BIG OPTIMIZATION
    print("   Computing overlap days using vectorized operations...")
    overlap_start = pd.DataFrame({
        'namedt': merged['namedt'].values,
        'linkdt': merged['linkdt'].values
    }).max(axis=1)

    overlap_end = pd.DataFrame({
        'nameendt': merged['nameendt'].values,
        'linkenddt': merged['linkenddt'].values
    }).min(axis=1)

    days_diff = (overlap_end - overlap_start).dt.days
    merged["overlap_days"] = np.where(
        merged["overlap_ok"],
        days_diff.clip(lower=0).fillna(0),
        0
    ).astype("int64")

    # CUSIP6 match
    if "ncusip6" in merged.columns and "cusip6" in merged.columns:
        m = merged["ncusip6"].notna() & merged["cusip6"].notna() & (merged["ncusip6"] == merged["cusip6"])
        merged["cusip6_match"] = m.astype("int64")
    else:
        merged["cusip6_match"] = 0

    # Convert all scoring columns to proper numeric types
    for c in ["linkprim_score", "linktype_rank", "overlap_days", "cusip6_match"]:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0).astype("int64")

    print_progress("Overlap computation complete", step_time)

    # -------------------------
    # Ranking and deduplication
    # -------------------------
    step_time = print_progress("Ranking matches")

    # Sort - simplified column list for performance
    merged = merged.sort_values(
        ["permno", "namedt", "nameendt",
         "linkprim_score", "linktype_rank", "cusip6_match", "overlap_days", "linkdt"],
        ascending=[True, True, True,
                   False, False, False, False, True]
    )

    seg_keys = ["permno", "namedt", "nameendt"]

    matched_best = (
        merged[merged["overlap_ok"]]
        .drop_duplicates(subset=seg_keys, keep="first")
    )
    print(f"   Best matches found: {len(matched_best):,} rows")

    all_segments = names.drop_duplicates(subset=seg_keys).copy()
    matched_keys = matched_best[seg_keys].drop_duplicates()

    unmatched = (
        all_segments
          .merge(matched_keys.assign(_hit=1), on=seg_keys, how="left")
          .query("_hit.isnull()", engine="python")
          .drop(columns="_hit")
    )
    print(f"   Unmatched segments: {len(unmatched):,} rows")

    # Fill required columns on unmatched
    for c, val in [
        ("gvkey", pd.NA),
        ("linkdt", pd.NaT),
        ("linkenddt", pd.NaT),
        ("cusip", pd.NA),
        ("cusip6", pd.NA),
        ("linkprim", pd.NA),
        ("linktype", pd.NA),
    ]:
        unmatched[c] = val
    unmatched["overlap_days"]   = 0
    unmatched["cusip6_match"]   = 0
    unmatched["linkprim_score"] = 0
    unmatched["linktype_rank"]  = 0

    # Build time-segmented master
    cols_common = [c for c in matched_best.columns if c in all_segments.columns]
    cols_add    = ["gvkey", "linkdt", "linkenddt", "cusip", "cusip6",
                   "linkprim", "linktype", "linkprim_score", "linktype_rank",
                   "overlap_days", "cusip6_match"]

    time_master = pd.concat(
        [
            matched_best[cols_common + cols_add],
            unmatched[cols_common + cols_add],
        ],
        ignore_index=True
    )

    print_progress("Ranking complete", step_time)
    return time_master

def add_flags_and_labels(time_master):
    """Add flags and exchange labels"""
    step_time = print_progress("Step 4: Adding flags and labels")

    if "shrcd" in time_master.columns:
        time_master["shrcd"] = pd.to_numeric(time_master["shrcd"], errors="coerce").astype("Int64")
        time_master["is_common"] = time_master["shrcd"].isin([10, 11]).astype("boolean")
    else:
        time_master["is_common"] = pd.Series(pd.NA, index=time_master.index, dtype="boolean")

    if "exchcd" in time_master.columns:
        time_master["exchcd"] = pd.to_numeric(time_master["exchcd"], errors="coerce").astype("Int64")
        exch_map = {1: "NYSE", 2: "AMEX", 3: "NASDAQ"}
        time_master["exch"] = time_master["exchcd"].map(exch_map).astype("string")

    # Cast string columns for consistency
    for c in ["comnam", "ticker", "tsymbol", "primexch", "shrcls",
              "ncusip", "ncusip6", "cusip", "cusip6", "linkprim", "linktype"]:
        if c in time_master.columns:
            time_master[c] = time_master[c].astype("string")

    # Order columns
    col_order = [c for c in [
        "permno", "permco", "gvkey",
        "namedt", "nameendt", "linkdt", "linkenddt",
        "comnam", "ticker", "tsymbol", "shrcls",
        "shrcd", "is_common", "exchcd", "exch", "primexch",
        "siccd", "naics",
        "ncusip", "ncusip6", "cusip", "cusip6",
        "linkprim", "linktype", "linkprim_score", "linktype_rank",
        "overlap_days", "cusip6_match"
    ] if c in time_master.columns]
    time_master = time_master[col_order].sort_values(["permno", "namedt"]).reset_index(drop=True)

    print_progress("Flags and labels complete", step_time)
    return time_master

def compute_primary_permno(time_master):
    """Compute primary PERMNO per GVKEY using optimized selection"""
    step_time = print_progress("Step 5: Computing primary PERMNO per GVKEY (VECTORIZED)")

    tm_for_primary = time_master.dropna(subset=["gvkey"]).copy()
    tm_for_primary["seg_days"] = (tm_for_primary["nameendt"] - tm_for_primary["namedt"]).dt.days.clip(lower=0)

    # Compute permno<->gvkey one-to-one pairs
    pairs = tm_for_primary[["permno", "gvkey"]].drop_duplicates()
    perm_deg  = pairs.groupby("permno")["gvkey"].nunique()
    gvkey_deg = pairs.groupby("gvkey")["permno"].nunique()
    pairs["one2one"] = pairs["permno"].map(perm_deg).eq(1) & pairs["gvkey"].map(gvkey_deg).eq(1)

    tm_for_primary = tm_for_primary.merge(pairs, on=["permno", "gvkey"], how="left")
    tm_for_primary["one2one"] = tm_for_primary["one2one"].fillna(False)

    # VECTORIZED PRIMARY SELECTION - THIS IS THE SECOND BIG OPTIMIZATION
    print("   Aggregating statistics per permno-gvkey pair...")
    agg_stats = tm_for_primary.groupby(['gvkey', 'permno'], dropna=False).agg({
        'one2one': 'max',
        'linkprim_score': 'max',
        'linktype_rank': 'max',
        'seg_days': 'sum',
        'is_common': lambda x: x.eq(True).any() if x.dtype == 'boolean' else False
    }).reset_index()

    # Rename is_common to avoid confusion
    agg_stats = agg_stats.rename(columns={'is_common': 'has_common'})

    print("   Selecting primary permno for each gvkey...")
    # Sort by priority criteria
    agg_stats = agg_stats.sort_values(
        ['gvkey', 'has_common', 'one2one', 'linkprim_score', 'linktype_rank', 'seg_days'],
        ascending=[True, False, False, False, False, False]
    )

    # Get first (best) permno per gvkey
    primary_map = (
        agg_stats.groupby('gvkey')
        .first()[['permno']]
        .rename(columns={'permno': 'primary_permno'})
        .reset_index()
    )

    print(f"   Found primary mappings for {len(primary_map):,} gvkeys")

    time_master = time_master.merge(primary_map, on="gvkey", how="left")
    time_master["is_primary_permno"] = (time_master["permno"] == time_master["primary_permno"])

    print_progress("Primary PERMNO selection complete", step_time)
    return time_master, primary_map

def build_security_master():
    """Main function to build security xmaster"""
    print("\n" + "="*60)
    print("CRSP/COMPUSTAT SECURITY MASTER BUILD - OPTIMIZED")
    print("="*60)
    script_start = time.time()
    
    # Check input files
    if not check_input_files():
        return False
    
    # Get output paths
    paths = SecurityMasterConfig.get_output_paths()
    
    # Step 1: Load NAMES
    names = load_and_process_names()
    
    # Step 2: Load LINKS  
    links = load_and_process_links()
    
    # Step 3: Merge and rank
    time_master = merge_and_rank(names, links)
    
    # Step 4: Add flags
    time_master = add_flags_and_labels(time_master)
    
    # Step 5: Primary PERMNO
    time_master, primary_map = compute_primary_permno(time_master)
    
    # Step 6: Save outputs
    step_time = print_progress("Step 6: Saving output files")

    # Normalize dtypes before save
    if "gvkey" in time_master.columns:
        time_master["gvkey"] = time_master["gvkey"].astype("string")
    if "primary_permno" in time_master.columns:
        time_master["primary_permno"] = time_master["primary_permno"].astype("Int64")

    time_master.to_csv(paths['segments'], index=False)
    print(f"   Saved {len(time_master):,} rows to {paths['segments']}")

    primary_table = (
        time_master
        .dropna(subset=["gvkey", "primary_permno"])
        .drop_duplicates(subset=["gvkey"])
        [["gvkey", "primary_permno"]]
        .sort_values("gvkey")
        .copy()
    )
    primary_table["gvkey"] = primary_table["gvkey"].astype("string")
    primary_table["primary_permno"] = primary_table["primary_permno"].astype("Int64")
    primary_table.to_csv(paths['primary'], index=False)
    print(f"   Saved {len(primary_table):,} rows to {paths['primary']}")

    print_progress("File saving complete", step_time)

    # Summary Statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    print(f"Total rows in security_master_segments: {len(time_master):,}")
    print(f"Total rows in gvkey_to_primary_permno: {len(primary_table):,}")
    print(f"\nTotal processing time: {time.time() - script_start:.2f} seconds")

    print("\nHead of time_master:")
    print(time_master.head(10))

    # Sanity checks
    print("\n" + "="*60)
    print("SANITY CHECKS")
    print("="*60)

    check_types(time_master, "time_master")
    check_types(primary_table, "primary_table")

    print("\n[Logical checks]")
    neg_spans = (time_master["nameendt"] < time_master["namedt"]).sum()
    print(f"  Segments with nameendt < namedt: {int(neg_spans)}")

    if {"linkdt", "linkenddt", "gvkey"}.issubset(time_master.columns):
        bad_links = (
            time_master.dropna(subset=["gvkey"])
            .assign(bad=lambda d: d["linkenddt"] < d["linkdt"])
            ["bad"].sum()
        )
        print(f"  Rows with linkenddt < linkdt (where gvkey present): {int(bad_links)}")

    # Exactly one distinct primary PERMNO per GVKEY
    prim_counts = (
        time_master[time_master["is_primary_permno"] == True]
        .groupby("gvkey")["permno"].nunique()
    )
    print(f"  GVKEYs with exactly one primary permno: {int((prim_counts == 1).sum())}")
    print(f"  GVKEYs with zero primaries: {int(time_master['gvkey'].nunique() - prim_counts.index.nunique())}")
    print(f"  GVKEYs with >1 primary permno (should be 0): {int((prim_counts > 1).sum())}")

    print("\n" + "="*60)
    print("PROCESSING COMPLETE!")
    print("="*60)
    
    return True

if __name__ == "__main__":
    success = build_security_master()
    sys.exit(0 if success else 1)
