"""
Abstract base class for all ESG data provider mappers.
Handles common functionality like CUSIP matching, overlap calculation, and scoring.
"""

import pandas as pd
import numpy as np
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple, Dict

class BaseESGMapper(ABC):
    """Base class for mapping ESG data providers to GVKEY"""
    
    # Constants
    FAR_FUTURE = pd.Timestamp("2262-04-11")
    PREFERRED_EXCHANGES = {1, 3}  # NYSE, NASDAQ
    
    def __init__(self, 
                 security_master_path: str = "security_master_segments.csv",
                 output_dir: str = "./processed/id_mappings"):
        """
        Initialize base mapper
        
        Args:
            security_master_path: Path to security master CSV
            output_dir: Directory for output files
        """
        self.security_master_path = security_master_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.security_master = None
        self.provider_data = None
        self.matches = None
        
        # Provider-specific attributes (to be set by subclasses)
        self.provider_name = None
        self.entity_id_col = None  # e.g., 'orgpermid', 'issuer_id', 'symbol'
        self.entity_name_col = None  # e.g., 'comname', 'company_name'
        
    # -------------------------
    # Abstract methods (must be implemented by subclasses)
    # -------------------------
    
    @abstractmethod
    def load_provider_data(self, data_path: str) -> pd.DataFrame:
        """Load and standardize provider-specific data"""
        pass
    
    @abstractmethod
    def extract_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract CUSIP6, ISIN, tickers etc. from provider data"""
        pass
    
    # -------------------------
    # Utility methods (shared across all providers)
    # -------------------------
    
    @staticmethod
    def print_progress(message: str, start_time: Optional[float] = None) -> float:
        """Print timestamped progress message"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if start_time:
            elapsed = time.time() - start_time
            print(f"[{timestamp}] ✓ {message} ({elapsed:.2f}s)")
        else:
            print(f"[{timestamp}] → {message}")
        return time.time()
    
    @staticmethod
    def normalize_string(s):
        """Normalize string columns to uppercase"""
        if isinstance(s, pd.Series):
            return s.astype("string").str.strip().str.upper()
        return str(s).strip().upper() if s else None
    
    @staticmethod
    def calculate_overlap_days(start1, end1, start2, end2) -> pd.Series:
        """Calculate overlap days between two date ranges"""
        overlap_start = np.maximum(start1.values, start2.values)
        overlap_end = np.minimum(end1.values, end2.values)
        days = ((overlap_end - overlap_start) / np.timedelta64(1, "D")).astype("int32")
        return pd.Series(days).clip(lower=0)
    
    @staticmethod
    def coerce_boolean(series) -> pd.Series:
        """Convert various representations to boolean"""
        if pd.api.types.is_bool_dtype(series):
            return series.fillna(False)
        if pd.api.types.is_numeric_dtype(series):
            return series.fillna(0).astype(bool)
        
        # String conversion
        s_lower = series.astype(str).str.strip().str.lower()
        truthy = {"1", "true", "t", "y", "yes"}
        return s_lower.isin(truthy).fillna(False)
    
    def extract_cusip6_from_isin(self, isin_series: pd.Series) -> pd.Series:
        """Extract CUSIP6 from ISIN for North American securities"""
        is_north_american = isin_series.str[:2].isin(["US", "CA"])
        is_valid_length = isin_series.str.len() >= 12
        return isin_series.where(
            is_north_american & is_valid_length
        ).str.slice(2, 8)
    
    # -------------------------
    # Security master methods
    # -------------------------
    
    def load_security_master(self) -> pd.DataFrame:
        """Load and prepare security master data"""
        step_time = self.print_progress("Loading security master")
        
        # Load relevant columns
        master_cols = [
            "permno", "gvkey", "namedt", "nameendt",
            "primary_permno", "is_primary_permno", "is_common", 
            "shrcd", "exchcd", "ncusip", "ncusip6", "ticker",
            "linkprim_score", "linktype_rank"
        ]
        
        security_master = pd.read_csv(
            self.security_master_path,
            usecols=lambda c: c.lower() in [col.lower() for col in master_cols],
            dtype=str,
            low_memory=False
        )
        security_master.columns = security_master.columns.str.lower()
        
        # Parse dates
        security_master["namedt"] = pd.to_datetime(security_master["namedt"], errors="coerce")
        security_master["nameendt"] = pd.to_datetime(
            security_master["nameendt"], errors="coerce"
        ).fillna(self.FAR_FUTURE)
        
        # Convert numeric columns
        for col in ["permno", "exchcd", "primary_permno", "linkprim_score", "linktype_rank", "gvkey"]:
            if col in security_master.columns:
                security_master[col] = pd.to_numeric(security_master[col], errors="coerce").astype("Int32")
        
        # Create/ensure boolean columns
        if "is_common" not in security_master.columns and "shrcd" in security_master.columns:
            shrcd = pd.to_numeric(security_master["shrcd"], errors="coerce")
            security_master["is_common"] = shrcd.isin([10, 11, 12])
        else:
            security_master["is_common"] = self.coerce_boolean(security_master.get("is_common", False))
        
        if "is_primary_permno" not in security_master.columns:
            security_master["is_primary_permno"] = (
                security_master["permno"] == security_master["primary_permno"]
            )
        else:
            security_master["is_primary_permno"] = self.coerce_boolean(security_master["is_primary_permno"])
        
        # Normalize identifiers
        for col in ["ncusip6", "ncusip", "ticker"]:
            if col in security_master.columns:
                if col == "ncusip6" and col not in security_master.columns and "ncusip" in security_master.columns:
                    security_master["ncusip6"] = self.normalize_string(security_master["ncusip"]).str.slice(0, 6)
                else:
                    security_master[col] = self.normalize_string(security_master[col])
        
        # Filter to valid records
        security_master = security_master.dropna(subset=["permno", "namedt", "nameendt"])
        
        print(f"   Loaded {len(security_master):,} security segments")
        self.print_progress("Security master loaded", step_time)
        
        self.security_master = security_master
        return security_master
    
    def filter_security_master(self, identifiers: Dict[str, set], year_range: Tuple[int, int]) -> pd.DataFrame:
        """Filter security master to relevant records"""
        step_time = self.print_progress("Filtering security master")
        
        # Filter by identifiers
        mask = pd.Series(False, index=self.security_master.index)
        
        if "cusip6" in identifiers and identifiers["cusip6"]:
            mask |= self.security_master["ncusip6"].isin(identifiers["cusip6"])
        
        if "ticker" in identifiers and identifiers["ticker"]:
            if "ticker" in self.security_master.columns:
                mask |= self.security_master["ticker"].isin(identifiers["ticker"])
        
        filtered = self.security_master[mask].copy()
        
        # Filter by date range
        if year_range:
            year_min, year_max = year_range
            date_min = pd.Timestamp(year_min, 1, 1)
            date_max = pd.Timestamp(year_max + 1, 1, 1)
            
            filtered = filtered[
                (filtered["namedt"] < date_max) & 
                (filtered["nameendt"] >= date_min)
            ].copy()
        
        print(f"   Filtered to {len(filtered):,} relevant security segments")
        self.print_progress("Filtering complete", step_time)
        
        return filtered
    
    # -------------------------
    # Matching methods
    # -------------------------
    
    def calculate_match_score(self, df: pd.DataFrame, match_source: str) -> np.ndarray:
        """Calculate quality score for matches"""
        n = len(df)
        score = np.zeros(n, dtype=np.int16)
        
        # Points for match source type
        source_scores = {
            "CUSIP6": 5,
            "ISIN_CUSIP6": 4,
            "ISIN": 6,
            "TICKER": 3
        }
        score += source_scores.get(match_source, 0)
        
        # Points for security characteristics
        if "is_common" in df:
            score += self.coerce_boolean(df["is_common"]).astype(np.int16) * 3
        
        if "is_primary_permno" in df:
            score += self.coerce_boolean(df["is_primary_permno"]).astype(np.int16) * 2
        
        if "exchcd" in df:
            is_preferred = pd.to_numeric(df["exchcd"], errors="coerce").isin(list(self.PREFERRED_EXCHANGES))
            score += is_preferred.fillna(False).astype(np.int16)
        
        # Points for overlap duration (max 3 points)
        if "overlap_days" in df:
            years = pd.to_numeric(df["overlap_days"], errors="coerce").fillna(0) // 365
            score += np.minimum(3, years).astype(np.int16)
        
        # Points for link quality
        for col, weight in [("linkprim_score", 1), ("linktype_rank", 1)]:
            if col in df:
                score += pd.to_numeric(df[col], errors="coerce").fillna(0).astype(np.int16) * weight
        
        return score
    
    def select_best_match(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select best match per entity-year based on match score"""
        if df.empty:
            return df
        
        key_cols = [self.entity_id_col, "year"]
        
        # Ranking columns in priority order
        rank_cols = ["match_score", "overlap_days", "is_primary_permno", "is_common", "exchcd", "namedt"]
        rank_cols = [c for c in rank_cols if c in df.columns]
        
        # Sort ascending for keys, descending for scores/booleans
        ascending = [True] * len(key_cols) + [
            False if c in ["match_score", "overlap_days", "is_primary_permno", "is_common"] else True
            for c in rank_cols
        ]
        
        # Sort and take first per group
        df_sorted = df.sort_values(key_cols + rank_cols, ascending=ascending)
        best_idx = df_sorted.drop_duplicates(subset=key_cols, keep="first").index
        
        return df.loc[best_idx].copy()
    
    def match_by_cusip6(self, provider_data: pd.DataFrame, security_master: pd.DataFrame, 
                        cusip6_col: str, already_matched: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Match provider data to security master using CUSIP6"""
        
        # Filter to records with CUSIP6
        candidates = provider_data[provider_data[cusip6_col].notna()].copy()
        
        # Exclude already matched if provided
        if already_matched is not None and len(already_matched) > 0:
            candidates = candidates.merge(
                already_matched[[self.entity_id_col, "year"]].assign(_matched=1),
                on=[self.entity_id_col, "year"],
                how="left"
            )
            candidates = candidates[candidates["_matched"].isna()].drop(columns="_matched")
        
        if candidates.empty:
            return pd.DataFrame()
        
        # Merge on CUSIP6
        matches = candidates.merge(
            security_master,
            left_on=cusip6_col,
            right_on="ncusip6",
            how="inner"
        )
        
        # Calculate overlap
        matches["overlap_days"] = self.calculate_overlap_days(
            matches["valid_from"], matches["valid_to"],
            matches["namedt"], matches["nameendt"]
        )
        
        # Filter to valid overlaps
        matches = matches[matches["overlap_days"] > 0].copy()
        
        return matches
    
    def match_by_ticker(self, provider_data: pd.DataFrame, security_master: pd.DataFrame,
                       ticker_col: str, already_matched: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Match provider data to security master using ticker"""
        
        if "ticker" not in security_master.columns:
            return pd.DataFrame()
        
        # Filter to records with ticker
        candidates = provider_data[provider_data[ticker_col].notna()].copy()
        
        # Exclude already matched if provided
        if already_matched is not None and len(already_matched) > 0:
            candidates = candidates.merge(
                already_matched[[self.entity_id_col, "year"]].assign(_matched=1),
                on=[self.entity_id_col, "year"],
                how="left"
            )
            candidates = candidates[candidates["_matched"].isna()].drop(columns="_matched")
        
        if candidates.empty:
            return pd.DataFrame()
        
        # Merge on ticker
        matches = candidates.merge(
            security_master,
            left_on=ticker_col,
            right_on="ticker",
            how="inner"
        )
        
        # Calculate overlap
        matches["overlap_days"] = self.calculate_overlap_days(
            matches["valid_from"], matches["valid_to"],
            matches["namedt"], matches["nameendt"]
        )
        
        # Filter to valid overlaps
        matches = matches[matches["overlap_days"] > 0].copy()
        
        return matches
    
    # -------------------------
    # Output methods
    # -------------------------
    
    def create_crosswalk(self, matches: pd.DataFrame) -> pd.DataFrame:
        """Create entity-level crosswalk from yearly matches"""
        if matches.empty or "gvkey" not in matches.columns:
            return pd.DataFrame()
        
        matches_with_gvkey = matches.dropna(subset=["gvkey"]).copy()
        
        # Aggregate by entity -> gvkey
        crosswalk_agg = matches_with_gvkey.groupby([self.entity_id_col, "gvkey"]).agg({
            "match_score": "sum",
            "year": "nunique",
            "overlap_days": "max",
            "permno": lambda x: x.value_counts().index[0] if len(x) > 0 else pd.NA,
            "namedt": "min",
            "nameendt": "max"
        }).reset_index()
        
        crosswalk_agg.columns = [self.entity_id_col, "gvkey", "total_score", "years_covered", 
                                 "max_overlap_days", "primary_permno", "first_seen", "last_seen"]
        
        # Select best GVKEY per entity
        crosswalk = (
            crosswalk_agg
            .sort_values([self.entity_id_col, "total_score", "years_covered", "max_overlap_days"],
                        ascending=[True, False, False, False])
            .drop_duplicates(subset=[self.entity_id_col], keep="first")
        )
        
        return crosswalk
    
    def save_outputs(self, matches: pd.DataFrame, crosswalk: pd.DataFrame):
        """Save all output files"""
        # Yearly matches
        match_file = self.output_dir / f"{self.provider_name}_{self.entity_id_col}_year_match.csv"
        matches.to_csv(match_file, index=False)
        print(f"   Saved {len(matches):,} matches to {match_file.name}")
        
        # Entity crosswalk
        crosswalk_file = self.output_dir / f"{self.provider_name}_{self.entity_id_col}_to_gvkey.csv"
        crosswalk.to_csv(crosswalk_file, index=False)
        print(f"   Saved {len(crosswalk):,} entity mappings to {crosswalk_file.name}")
    
    # -------------------------
    # Main execution method
    # -------------------------
    
    def run(self, data_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main execution method - runs the complete mapping process
        
        Args:
            data_path: Path to provider data file(s)
            
        Returns:
            Tuple of (yearly_matches, entity_crosswalk) DataFrames
        """
        print(f"\n{'='*60}")
        print(f"{self.provider_name.upper()} TO GVKEY MAPPING")
        print(f"{'='*60}")
        
        script_start = time.time()
        
        # Load security master
        self.load_security_master()
        
        # Load provider data
        self.provider_data = self.load_provider_data(data_path)
        
        # Extract identifiers
        self.provider_data = self.extract_identifiers(self.provider_data)
        
        # Get unique identifiers and date range for filtering
        identifiers = self._get_unique_identifiers()
        year_range = self._get_year_range()
        
        # Filter security master
        security_master_filtered = self.filter_security_master(identifiers, year_range)
        
        # Perform matching (implemented by subclasses using base methods)
        self.matches = self.perform_matching(self.provider_data, security_master_filtered)
        
        # Create crosswalk
        crosswalk = self.create_crosswalk(self.matches)
        
        # Save outputs
        self.save_outputs(self.matches, crosswalk)
        
        # Print summary
        self._print_summary(script_start)
        
        return self.matches, crosswalk
    
    @abstractmethod
    def perform_matching(self, provider_data: pd.DataFrame, security_master: pd.DataFrame) -> pd.DataFrame:
        """Perform the actual matching - implemented by subclasses"""
        pass
    
    def _get_unique_identifiers(self) -> Dict[str, set]:
        """Get unique identifiers from provider data"""
        identifiers = {}
        
        for col in ["cusip6_from_cusip", "cusip6_from_isin", "ticker", "clean_symbol"]:
            if col in self.provider_data.columns:
                identifiers[col] = set(self.provider_data[col].dropna())
        
        # Combine CUSIP6 sources
        cusip6_cols = [k for k in identifiers.keys() if "cusip6" in k]
        if cusip6_cols:
            all_cusip6 = set()
            for col in cusip6_cols:
                all_cusip6.update(identifiers[col])
            identifiers["cusip6"] = all_cusip6
        
        return identifiers
    
    def _get_year_range(self) -> Optional[Tuple[int, int]]:
        """Get year range from provider data"""
        if "year" in self.provider_data.columns:
            years = self.provider_data["year"].dropna()
            if len(years) > 0:
                return (years.min(), years.max())
        return None
    
    def _print_summary(self, start_time: float):
        """Print mapping summary"""
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
        
        print(f"\nTotal processing time: {time.time() - start_time:.2f} seconds")
        print(f"{'='*60}")
