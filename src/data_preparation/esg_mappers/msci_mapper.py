"""
MSCI ESG data mapper implementation.
Maps IssuerID to GVKEY using CUSIP/ISIN matching.
"""

import pandas as pd
import re
from pathlib import Path
from typing import List, Optional
from .base_mapper import BaseESGMapper

class MSCIMapper(BaseESGMapper):
    """Mapper for MSCI ESG data"""
    
    def __init__(self, security_master_path: str = "security_master_segments.csv",
                 output_dir: str = "./processed/id_mappings"):
        super().__init__(security_master_path, output_dir)
        
        # Provider-specific attributes
        self.provider_name = "msci"
        self.entity_id_col = "issuer_id"
        self.entity_name_col = "company_name"
        
    def load_provider_data(self, data_path: str) -> pd.DataFrame:
        """Load and standardize MSCI data from Excel files"""
        step_time = self.print_progress("Loading MSCI data")
        
        # Handle directory of files or single file
        data_path = Path(data_path)
        if data_path.is_dir():
            files = sorted(data_path.glob("ESG Ratings Timeseries*.xlsx"))
        else:
            files = [data_path]
        
        if not files:
            raise FileNotFoundError(f"No MSCI files found in {data_path}")
        
        print(f"   Found {len(files)} MSCI files")
        
        all_msci_data = []
        
        for file_path in files:
            try:
                df = self._process_single_file(file_path)
                if df is not None and len(df) > 0:
                    all_msci_data.append(df)
            except Exception as e:
                print(f"     ERROR reading {file_path.name}: {e}")
                continue
        
        # Combine all MSCI data
        if all_msci_data:
            msci_data = pd.concat(all_msci_data, ignore_index=True)
            
            # Remove duplicates at issuer-year level
            before_dedup = len(msci_data)
            msci_data = msci_data.drop_duplicates(subset=["issuer_id", "year"]).reset_index(drop=True)
            after_dedup = len(msci_data)
            
            print(f"   Combined MSCI data: {before_dedup:,} total records")
            print(f"   After deduplication: {after_dedup:,} issuer-year pairs")
            print(f"   Unique issuers: {msci_data['issuer_id'].nunique():,}")
            print(f"   Year range: {msci_data['year'].min()} - {msci_data['year'].max()}")
        else:
            raise ValueError("No valid MSCI data could be loaded")
        
        self.print_progress("MSCI data loaded", step_time)
        
        return msci_data
    
    def _process_single_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Process a single MSCI Excel file"""
        print(f"     Processing: {file_path.name}")
        
        # Read Excel file
        df = pd.read_excel(file_path, dtype="string", engine="calamine")
        
        # Normalize column names
        df.columns = df.columns.str.strip().str.upper()
        
        # Expected columns
        identifier_cols = {"ISSUER_ISIN", "ISSUER_CUSIP", "ISSUER_SEDOL", "ISSUER_TICKER", 
                          "ISSUER_NAME", "ISSUERID"}
        date_cols = {"AS_OF_DATE", "AS OF DATE", "ASOF_DATE", "DATE", "PERIOD", "YEAR"}
        
        available_cols = [col for col in identifier_cols | date_cols if col in df.columns]
        
        if not available_cols:
            print(f"       Skipping - no relevant columns found")
            return None
        
        df = df[available_cols].copy()
        
        # Parse year from various date columns
        year = self._extract_year(df, file_path.name)
        
        # Build standardized dataframe
        msci_record = pd.DataFrame({
            "issuer_id": df.get("ISSUERID", pd.Series(index=df.index, dtype="string")),
            "year": year,
            "company_name": df.get("ISSUER_NAME", pd.Series(index=df.index, dtype="string")),
            "ticker": df.get("ISSUER_TICKER", pd.Series(index=df.index, dtype="string")),
            "cusip": df.get("ISSUER_CUSIP", pd.Series(index=df.index, dtype="string")),
            "isin": df.get("ISSUER_ISIN", pd.Series(index=df.index, dtype="string")),
            "sedol": df.get("ISSUER_SEDOL", pd.Series(index=df.index, dtype="string")),
        })
        
        # Normalize strings
        for col in ["issuer_id", "company_name", "ticker", "cusip", "isin", "sedol"]:
            if col in msci_record.columns:
                msci_record[col] = self.normalize_string(msci_record[col])
        
        # Drop invalid records
        msci_record = msci_record.dropna(subset=["issuer_id", "year"])
        
        print(f"       Valid records: {len(msci_record):,}")
        
        return msci_record
    
    def _extract_year(self, df: pd.DataFrame, filename: str) -> pd.Series:
        """Extract year from various date columns or filename"""
        
        # Try YEAR column first
        if "YEAR" in df.columns:
            return pd.to_numeric(df["YEAR"], errors="coerce").astype("Int16")
        
        # Try date columns
        date_columns = ["AS_OF_DATE", "AS OF DATE", "ASOF_DATE", "DATE", "PERIOD"]
        for date_col in date_columns:
            if date_col in df.columns:
                # Try various date formats
                for date_format in ["%Y%m%d", "%Y-%m-%d", "%m/%d/%Y", None]:
                    try:
                        if date_format:
                            date_series = pd.to_datetime(df[date_col], format=date_format, errors="coerce")
                        else:
                            date_series = pd.to_datetime(df[date_col], errors="coerce")
                        
                        if date_series.notna().any():
                            return date_series.dt.year.astype("Int16")
                    except:
                        continue
        
        # Fallback to filename
        year_match = re.search(r"(19|20)\d{2}", filename)
        if year_match:
            detected_year = int(year_match.group(0))
            return pd.Series([detected_year] * len(df), dtype="Int16")
        
        return pd.Series([pd.NA] * len(df), dtype="Int16")
    
    def extract_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract CUSIP6 and other identifiers from MSCI data"""
        step_time = self.print_progress("Extracting identifiers")
        
        # Extract CUSIP6 from full CUSIP
        df["cusip6_from_cusip"] = df["cusip"].str.slice(0, 6)
        
        # Extract CUSIP6 from ISIN
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
        """Perform matching for MSCI data"""
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
        
        # Step 2: Match via ISIN
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
                "issuer_id", "year", "match_src", "match_score", "overlap_days",
                "permno", "gvkey", "primary_permno", "is_primary_permno", "is_common", "exchcd",
                "company_name", "ticker", "cusip", "isin", "sedol",
                "namedt", "nameendt", "ncusip6"
            ]
            combined = combined[[c for c in output_cols if c in combined.columns]]
            combined = combined.sort_values(["issuer_id", "year"])
            
            return combined
        
        return pd.DataFrame()
