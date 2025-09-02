"""
Refinitiv ESG data mapper implementation.
Maps OrgPermID to GVKEY using CUSIP/ISIN matching.
"""

import pandas as pd
from typing import Optional
from .base_mapper import BaseESGMapper

class RefinitivMapper(BaseESGMapper):
    """Mapper for Refinitiv ESG data"""
    
    def __init__(self, security_master_path: str = "security_master_segments.csv",
                 output_dir: str = "./processed/id_mappings"):
        super().__init__(security_master_path, output_dir)
        
        # Provider-specific attributes
        self.provider_name = "refinitiv"
        self.entity_id_col = "orgpermid"
        self.entity_name_col = "comname"
        
    def load_provider_data(self, data_path: str) -> pd.DataFrame:
        """Load and standardize Refinitiv data"""
        step_time = self.print_progress("Loading Refinitiv data")
        
        # Try different encodings
        for encoding in ["cp1252", "latin1", "utf-8"]:
            try:
                refinitiv = pd.read_csv(
                    data_path,
                    usecols=lambda c: c.lower() in ["orgpermid", "year", "cusip", "isin", "sedol", "ticker", "comname"],
                    dtype=str,
                    encoding=encoding,
                    low_memory=False
                )
                break
            except UnicodeDecodeError:
                continue
        
        refinitiv.columns = refinitiv.columns.str.lower().str.strip()
        
        # Normalize string columns
        for col in ["orgpermid", "cusip", "isin", "sedol", "ticker", "comname"]:
            if col in refinitiv.columns:
                refinitiv[col] = self.normalize_string(refinitiv[col])
        
        # Convert year to numeric
        refinitiv["year"] = pd.to_numeric(refinitiv["year"], errors="coerce").astype("Int16")
        
        # Clean and deduplicate
        refinitiv_clean = (
            refinitiv
            .dropna(subset=["orgpermid", "year"])
            .drop_duplicates(subset=["orgpermid", "year"])
            .reset_index(drop=True)
        )
        
        print(f"   Loaded {len(refinitiv_clean):,} orgpermid-year pairs")
        self.print_progress("Refinitiv data loaded", step_time)
        
        return refinitiv_clean
    
    def extract_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract CUSIP6 and other identifiers from Refinitiv data"""
        step_time = self.print_progress("Extracting identifiers")
        
        # Extract CUSIP6 from full CUSIP
        df["cusip6_from_cusip"] = df["cusip"].str.slice(0, 6)
        
        # Extract CUSIP6 from ISIN (US/CA ISINs contain CUSIP)
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
        for col in ["cusip", "isin", "sedol", "ticker"]:
            if col in df.columns:
                available = df[col].notna().sum()
                print(f"   {col.upper():>6}: {available:,} records ({100*available/len(df):.1f}%)")
        
        self.print_progress("Identifier extraction complete", step_time)
        
        return df
    
    def perform_matching(self, provider_data: pd.DataFrame, security_master: pd.DataFrame) -> pd.DataFrame:
        """Perform matching for Refinitiv data"""
        all_matches = []
        
        # Step 1: Match via CUSIP
        step_time = self.print_progress("Matching via CUSIP")
        
        cusip_matches = self.match_by_cusip6(
            provider_data, 
            security_master, 
            "cusip6_from_cusip"
        )
        
        if len(cusip_matches):
            cusip_matches["match_src"] = "CUSIP6"
            cusip_matches["match_score"] = self.calculate_match_score(cusip_matches, "CUSIP6")
            best_cusip = self.select_best_match(cusip_matches)
            all_matches.append(best_cusip)
            print(f"   Found {len(best_cusip):,} matches via CUSIP")
        
        self.print_progress("CUSIP matching complete", step_time)
        
        # Step 2: Match via ISIN (for unmatched records)
        step_time = self.print_progress("Matching via ISIN")
        
        already_matched = best_cusip if len(cusip_matches) else None
        
        isin_matches = self.match_by_cusip6(
            provider_data,
            security_master,
            "cusip6_from_isin",
            already_matched
        )
        
        if len(isin_matches):
            isin_matches["match_src"] = "ISIN_CUSIP6"
            isin_matches["match_score"] = self.calculate_match_score(isin_matches, "ISIN_CUSIP6")
            best_isin = self.select_best_match(isin_matches)
            all_matches.append(best_isin)
            print(f"   Found {len(best_isin):,} additional matches via ISIN")
        
        self.print_progress("ISIN matching complete", step_time)
        
        # Combine all matches
        if all_matches:
            combined = pd.concat(all_matches, ignore_index=True)
            
            # Select output columns
            output_cols = [
                "orgpermid", "year", "match_src", "match_score", "overlap_days",
                "permno", "gvkey", "primary_permno", "is_primary_permno", "is_common", "exchcd",
                "comname", "ticker", "cusip", "isin", "sedol",
                "namedt", "nameendt", "ncusip6"
            ]
            combined = combined[[c for c in output_cols if c in combined.columns]]
            combined = combined.sort_values(["orgpermid", "year"])
            
            return combined
        
        return pd.DataFrame()
