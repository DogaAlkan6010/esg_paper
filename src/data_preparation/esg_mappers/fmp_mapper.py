"""
Financial Modeling Prep (FMP) ESG data mapper implementation.
Maps FMP Symbol to GVKEY using ISIN/ticker matching.
"""

import pandas as pd
import numpy as np
import gc
from typing import Optional
from .base_mapper import BaseESGMapper
import time

class FMPMapper(BaseESGMapper):
    """Mapper for Financial Modeling Prep ESG data"""
    
    def __init__(self, security_master_path: str = "security_master_segments.csv",
                 output_dir: str = "./processed/id_mappings"):
        super().__init__(security_master_path, output_dir)
        
        # Provider-specific attributes
        self.provider_name = "fmp"
        self.entity_id_col = "symbol"
        self.entity_name_col = "symbol"  # FMP uses symbol as identifier
        
    def load_provider_data(self, data_path: str) -> pd.DataFrame:
        """Load and standardize FMP data from Parquet file"""
        step_time = self.print_progress("Loading FMP ESG data")
        
        try:
            fmp_data = pd.read_parquet(data_path)
            print(f"   Loaded {len(fmp_data):,} FMP ESG records")
        except FileNotFoundError:
            raise FileNotFoundError(f"FMP data file not found: {data_path}")
        
        print(f"   Columns: {list(fmp_data.columns)}")
        print(f"   Sample data:")
        print(fmp_data.head())
        
        # Convert timestamps to datetime
        if "periodEndDate" in fmp_data.columns:
            # Convert from milliseconds timestamp to datetime
            fmp_data["period_end"] = pd.to_datetime(fmp_data["periodEndDate"], unit="ms", errors="coerce")
            fmp_data["year"] = fmp_data["period_end"].dt.year.astype("Int16")
        elif "year" not in fmp_data.columns:
            print("   WARNING: No date information found!")
            fmp_data["year"] = pd.NA

        if "acceptedDate" in fmp_data.columns:
            fmp_data["accepted_date"] = pd.to_datetime(fmp_data["acceptedDate"], unit="ms", errors="coerce")

        # Normalize string columns
        for col in ["symbol", "isin"]:
            if col in fmp_data.columns:
                fmp_data[col] = self.normalize_string(fmp_data[col])

        # Create standardized symbol (remove exchange suffixes like .L, .F, etc.)
        if "symbol" in fmp_data.columns:
            fmp_data["clean_symbol"] = fmp_data["symbol"].str.replace(r'\.[A-Z]+$', '', regex=True)

        # Clean and deduplicate
        keep_cols = ["symbol", "clean_symbol", "year", "isin", 
                    "Environmental", "Social", "Governance", "ESG", "period_end"]
        keep_cols = [c for c in keep_cols if c in fmp_data.columns]
        
        fmp_clean = (
            fmp_data[keep_cols]
            .dropna(subset=["symbol", "year"])
            .drop_duplicates(subset=["symbol", "year"])
            .reset_index(drop=True)
        )

        print(f"   Cleaned FMP data: {len(fmp_clean):,} symbol-year pairs")
        print(f"   Year range: {fmp_clean['year'].min()} - {fmp_clean['year'].max()}")
        print(f"   Unique symbols: {fmp_clean['symbol'].nunique():,}")
        
        self.print_progress("FMP data loaded", step_time)
        return fmp_clean
    
    def extract_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract CUSIP6 and other identifiers from FMP data"""
        step_time = self.print_progress("Extracting identifiers")
        
        # Extract CUSIP6 from ISIN (for US/CA ISINs)
        if "isin" in df.columns:
            df["cusip6_from_isin"] = self.extract_cusip6_from_isin(df["isin"])
        else:
            df["cusip6_from_isin"] = pd.NA
        
        # Create date windows for matching
        df["valid_from"] = pd.to_datetime(
            df["year"].astype("string") + "-01-01", errors="coerce"
        )
        df["valid_to"] = pd.to_datetime(
            (df["year"] + 1).astype("string") + "-01-01", errors="coerce"
        )
        
        # Check identifier availability
        for col in ["isin", "cusip6_from_isin", "clean_symbol"]:
            if col in df.columns:
                available = df[col].notna().sum()
                print(f"   {col.upper():>15}: {available:,} records ({100*available/len(df):.1f}%)")
        
        self.print_progress("Identifier extraction complete", step_time)
        return df
    
    def perform_matching(self, provider_data: pd.DataFrame, security_master: pd.DataFrame) -> pd.DataFrame:
        """Perform matching for FMP data"""
        all_matches = []
        
        # Step 1: Match via ISIN (CUSIP6)
        step_time = self.print_progress("Matching via ISIN (CUSIP6)")
        
        isin_matches = self.match_by_cusip6(
            provider_data,
            security_master,
            "cusip6_from_isin"
        )
        
        if len(isin_matches):
            isin_matches["match_src"] = "ISIN"
            isin_matches["match_score"] = self.calculate_match_score(isin_matches, "ISIN")
            best_isin = self.select_best_match(isin_matches)
            all_matches.append(best_isin)
            print(f"   Found {len(best_isin):,} matches via ISIN")
        
        self.print_progress("ISIN matching complete", step_time)
        
        # Step 2: Match via Ticker (for unmatched records)
        step_time = self.print_progress("Matching via Ticker")
        
        already_matched = best_isin if len(isin_matches) else None
        
        ticker_matches = self.match_by_ticker(
            provider_data,
            security_master,
            "clean_symbol",
            already_matched
        )
        
        if len(ticker_matches):
            ticker_matches["match_src"] = "TICKER"
            ticker_matches["match_score"] = self.calculate_match_score(ticker_matches, "TICKER")
            best_ticker = self.select_best_match(ticker_matches)
            all_matches.append(best_ticker)
            print(f"   Found {len(best_ticker):,} additional matches via ticker")
        
        self.print_progress("Ticker matching complete", step_time)
        
        # Clean up memory
        del isin_matches, ticker_matches
        gc.collect()
        
        # Combine all matches
        if all_matches:
            combined = pd.concat(all_matches, ignore_index=True)
            
            # Select output columns
            output_cols = [
                "symbol", "clean_symbol", "year", "match_src", "match_score", "overlap_days",
                "permno", "gvkey", "primary_permno", "is_primary_permno", "is_common", "exchcd",
                "isin", "Environmental", "Social", "Governance", "ESG",
                "namedt", "nameendt", "ticker", "ncusip6"
            ]
            combined = combined[[c for c in output_cols if c in combined.columns]]
            combined = combined.sort_values(["symbol", "year"])
            
            return combined
        
        return pd.DataFrame()
    
    def _print_summary(self, start_time: float):
        """Print mapping summary with FMP-specific details"""
        print(f"\n{'='*60}")
        print("MAPPING SUMMARY")
        print(f"{'='*60}")
        
        if self.provider_data is not None and self.matches is not None:
            print(f"Input: {len(self.provider_data):,} {self.entity_id_col}-year pairs")
            print(f"Matched: {len(self.matches):,} pairs ({100*len(self.matches)/len(self.provider_data):.1f}%)")
            print(f"Unique {self.entity_id_col}s: {self.matches[self.entity_id_col].nunique():,}")
            print(f"Unique GVKEYs: {self.matches['gvkey'].nunique():,}")
            
            if "match_src" in self.matches.columns:
                print(f"\nMatch sources:")
                print(self.matches['match_src'].value_counts())
            
            # FMP-specific: ESG score availability
            print(f"\nESG score availability:")
            for esg_col in ["Environmental", "Social", "Governance", "ESG"]:
                if esg_col in self.matches.columns:
                    available = self.matches[esg_col].notna().sum()
                    print(f"  {esg_col}: {available:,} ({100*available/len(self.matches):.1f}%)")
            
            # Year distribution
            if "year" in self.matches.columns:
                print(f"\nMatches by year:")
                year_summary = self.matches.groupby('year').size().sort_index()
                for year, count in year_summary.items():
                    if pd.notna(year):
                        print(f"  {int(year)}: {count:,} matches")
        
        print(f"\nTotal processing time: {time.time() - start_time:.2f} seconds")
        print(f"{'='*60}")
