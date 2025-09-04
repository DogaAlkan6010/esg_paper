"""
Build Monthly S&P 500 Panel - IMPROVED VERSION
Fixes issues identified: GVKEY as string, proper ranking, deduplication
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime
import time

# Add src to path to reuse utilities
sys.path.append(str(Path(__file__).parent.parent))

# Import reusable utilities from base mapper
from data_preparation.esg_mappers.base_mapper import BaseESGMapper

class SP500MonthlyPanel:
    """Build monthly panel of S&P 500 constituents with proper GVKEY/PERMNO mapping"""
    
    def __init__(self, sp500_path, security_master_path):
        self.sp500_path = Path(sp500_path)
        self.security_master_path = Path(security_master_path)
        self.security_master = None
        self.monthly_panel = None
        
        # Reuse constants from BaseESGMapper
        self.FAR_FUTURE = BaseESGMapper.FAR_FUTURE
        self.PREFERRED_EXCHANGES = BaseESGMapper.PREFERRED_EXCHANGES
        
    def load_security_master(self):
        """Load and prepare security master with proper handling"""
        step_time = BaseESGMapper.print_progress("Loading security master")
        
        # Load with GVKEY as STRING (critical fix)
        self.security_master = pd.read_csv(
            self.security_master_path,
            dtype={'gvkey': str, 'permno': str, 'permco': str},
            low_memory=False
        )
        
        # GVKEY should be 6-char zero-padded string
        self.security_master['gvkey'] = self.security_master['gvkey'].str.zfill(6)
        
        # PERMNO as nullable integer
        self.security_master['permno'] = pd.to_numeric(self.security_master['permno'], errors='coerce').astype('Int64')
        self.security_master['permco'] = pd.to_numeric(self.security_master['permco'], errors='coerce').astype('Int64')
        
        # Parse date columns
        date_cols = ['namedt', 'nameendt', 'linkdt', 'linkenddt']
        for col in date_cols:
            self.security_master[col] = pd.to_datetime(self.security_master[col], errors='coerce')
        
        # Fill open-ended dates using BaseESGMapper constant
        self.security_master['nameendt'] = self.security_master['nameendt'].fillna(self.FAR_FUTURE)
        self.security_master['linkenddt'] = self.security_master['linkenddt'].fillna(self.FAR_FUTURE)
        
        # Standardize boolean columns using BaseESGMapper utility
        bool_cols = ['is_primary_permno', 'is_common']
        for col in bool_cols:
            if col in self.security_master.columns:
                self.security_master[col] = BaseESGMapper.coerce_boolean(self.security_master[col])
        
        # Convert ranking columns to numeric
        rank_cols = ['linkprim_score', 'linktype_rank', 'overlap_days', 'cusip6_match']
        for col in rank_cols:
            if col in self.security_master.columns:
                self.security_master[col] = pd.to_numeric(self.security_master[col], errors='coerce').fillna(0)
        
        # Normalize string columns using BaseESGMapper utility
        for col in ['ticker', 'ncusip', 'cusip']:
            if col in self.security_master.columns:
                self.security_master[col] = BaseESGMapper.normalize_string(self.security_master[col])
        
        # Create CUSIP6 columns
        if 'ncusip' in self.security_master.columns:
            self.security_master['ncusip6'] = self.security_master['ncusip'].str[:6]
        if 'cusip' in self.security_master.columns:
            self.security_master['cusip6_link'] = self.security_master['cusip'].str[:6]
        
        BaseESGMapper.print_progress(f"Security master loaded: {len(self.security_master):,} rows", step_time)
        
    def get_sp500_monthly_snapshots(self):
        """Get month-end snapshots from S&P 500 file"""
        step_time = BaseESGMapper.print_progress("Processing S&P 500 constituents")
        
        sp500 = pd.read_excel(self.sp500_path)
        sp500['Date'] = pd.to_datetime(sp500['Date'])
        
        # Normalize identifiers using BaseESGMapper utility
        sp500['cusip6'] = BaseESGMapper.normalize_string(sp500['CUSIP']).str[:6]
        sp500['ticker'] = BaseESGMapper.normalize_string(sp500['Ticker'])
        
        # Get month-end for each date
        sp500['month_end'] = sp500['Date'] + pd.offsets.MonthEnd(0)
        
        # For each month, take the latest available snapshot
        monthly_snapshots = []
        
        for month_end in sorted(sp500['month_end'].unique()):
            month_data = sp500[sp500['month_end'] == month_end]
            last_date = month_data['Date'].max()
            snapshot = month_data[month_data['Date'] == last_date].copy()
            snapshot['snapshot_date'] = month_end
            monthly_snapshots.append(snapshot)
        
        result = pd.concat(monthly_snapshots, ignore_index=True)
        
        BaseESGMapper.print_progress(f"S&P 500 snapshots: {len(result):,} constituent-months", step_time)
        print(f"   Date range: {result['snapshot_date'].min()} to {result['snapshot_date'].max()}")
        
        return result
    
    def rank_and_select_best(self, candidates, selection_type='permno'):
        """
        Rank multiple candidates and select the best one
        Used for both GVKEY selection and PERMNO selection
        """
        if len(candidates) == 0:
            return None
        
        if len(candidates) == 1:
            return candidates.iloc[0]
        
        # Create ranking scores
        candidates = candidates.copy()
        
        if selection_type == 'permno':
            # For PERMNO selection within a GVKEY
            # Priority: primary > common > cusip6_match > link scores > overlap
            candidates['rank_score'] = (
                candidates['is_primary_permno'].astype(int) * 10000 +
                candidates['is_common'].astype(int) * 1000 +
                candidates.get('cusip6_match', 0) * 100 +
                candidates.get('linkprim_score', 0) * 10 +
                candidates.get('linktype_rank', 0) * 5 +
                candidates.get('overlap_days', 0) / 365  # Normalized to years
            )
        else:
            # For GVKEY selection when multiple match
            # Use link quality indicators
            candidates['rank_score'] = (
                candidates.get('linkprim_score', 0) * 100 +
                candidates.get('linktype_rank', 0) * 10 +
                candidates.get('overlap_days', 0) / 365
            )
        
        # Sort by rank score and take the best
        return candidates.nlargest(1, 'rank_score').iloc[0]
    
    def map_to_gvkey_permno(self, sp500_snapshot):
        """
        Map S&P 500 constituents to GVKEY and PERMNO with proper ranking
        """
        step_time = BaseESGMapper.print_progress("Mapping to GVKEY/PERMNO with improved logic")
        
        mapped_data = []
        
        for snapshot_date, group in sp500_snapshot.groupby('snapshot_date'):
            
            for _, constituent in group.iterrows():
                
                # Step 1: Find GVKEY matches
                cusip_matches = self.security_master[
                    ((self.security_master['ncusip6'] == constituent['cusip6']) |
                     (self.security_master.get('cusip6_link', pd.Series()) == constituent['cusip6'])) &
                    (self.security_master['namedt'] <= snapshot_date) &
                    (self.security_master['nameendt'] > snapshot_date) &
                    (self.security_master['linkdt'] <= snapshot_date) &
                    (self.security_master['linkenddt'] > snapshot_date)
                ]
                
                if len(cusip_matches) > 0:
                    # If multiple GVKEYs match, rank and select best
                    unique_gvkeys = cusip_matches['gvkey'].unique()
                    if len(unique_gvkeys) > 1:
                        # Multiple GVKEYs - need to pick best
                        best_gvkey_matches = []
                        for gvkey in unique_gvkeys:
                            gvkey_subset = cusip_matches[cusip_matches['gvkey'] == gvkey]
                            best_for_gvkey = self.rank_and_select_best(gvkey_subset, 'gvkey')
                            best_gvkey_matches.append(best_for_gvkey)
                        
                        # Now pick best GVKEY
                        best_gvkey_df = pd.DataFrame(best_gvkey_matches)
                        match = self.rank_and_select_best(best_gvkey_df, 'gvkey')
                    else:
                        # Single GVKEY - pick best PERMNO for it
                        match = self.rank_and_select_best(cusip_matches, 'permno')
                    
                    match_type = 'CUSIP6'
                    permno_type = 'PRIMARY' if match.get('is_primary_permno', False) else 'SECONDARY'
                
                # Fallback to ticker
                elif pd.notna(constituent['ticker']):
                    ticker_matches = self.security_master[
                        (self.security_master['ticker'] == constituent['ticker']) &
                        (self.security_master['namedt'] <= snapshot_date) &
                        (self.security_master['nameendt'] > snapshot_date) &
                        (self.security_master['linkdt'] <= snapshot_date) &
                        (self.security_master['linkenddt'] > snapshot_date)
                    ]
                    
                    if len(ticker_matches) > 0:
                        # Same ranking logic for ticker matches
                        unique_gvkeys = ticker_matches['gvkey'].unique()
                        if len(unique_gvkeys) > 1:
                            best_gvkey_matches = []
                            for gvkey in unique_gvkeys:
                                gvkey_subset = ticker_matches[ticker_matches['gvkey'] == gvkey]
                                best_for_gvkey = self.rank_and_select_best(gvkey_subset, 'gvkey')
                                best_gvkey_matches.append(best_for_gvkey)
                            
                            best_gvkey_df = pd.DataFrame(best_gvkey_matches)
                            match = self.rank_and_select_best(best_gvkey_df, 'gvkey')
                        else:
                            match = self.rank_and_select_best(ticker_matches, 'permno')
                        
                        match_type = 'TICKER'
                        permno_type = 'PRIMARY' if match.get('is_primary_permno', False) else 'SECONDARY'
                    else:
                        match = None
                        match_type = 'UNMATCHED'
                        permno_type = 'NO_MATCH'
                else:
                    match = None
                    match_type = 'UNMATCHED'
                    permno_type = 'NO_MATCH'
                
                # Build the row
                if match is not None:
                    mapped_data.append({
                        'date': snapshot_date,
                        'ticker_sp500': constituent['Ticker'],
                        'cusip_sp500': constituent['CUSIP'],
                        'gvkey': match['gvkey'],  # Already a string
                        'permno': match['permno'],  # Already Int64
                        'ticker_crsp': match.get('ticker', ''),
                        'company_name': match.get('comnam', ''),
                        'match_type': match_type,
                        'permno_type': permno_type,
                        'is_primary': match.get('is_primary_permno', False),
                        'is_common': match.get('is_common', False),
                        'exchange': match.get('exch', ''),
                        'linkprim': match.get('linkprim', ''),
                        'linktype': match.get('linktype', ''),
                        'rank_score': match.get('rank_score', 0)
                    })
                else:
                    mapped_data.append({
                        'date': snapshot_date,
                        'ticker_sp500': constituent['Ticker'],
                        'cusip_sp500': constituent['CUSIP'],
                        'gvkey': None,
                        'permno': None,
                        'ticker_crsp': '',
                        'company_name': '',
                        'match_type': match_type,
                        'permno_type': permno_type,
                        'is_primary': False,
                        'is_common': False,
                        'exchange': '',
                        'linkprim': '',
                        'linktype': '',
                        'rank_score': 0
                    })
        
        result = pd.DataFrame(mapped_data)
        
        # CRITICAL: Deduplicate by (date, gvkey) keeping best match
        result = result.sort_values(['date', 'gvkey', 'rank_score'], ascending=[True, True, False])
        result = result.drop_duplicates(subset=['date', 'gvkey'], keep='first')
        
        # Add time features
        result['year'] = result['date'].dt.year
        result['month'] = result['date'].dt.month
        result['year_month'] = result['date'].dt.to_period('M')
        
        # ADD ESG KNOWLEDGE DATE (critical for no look-ahead)
        result['esg_knowledge_year'] = result['year'] - 1  # Conservative: use prior year ESG
        
        BaseESGMapper.print_progress(f"Mapping complete: {len(result):,} rows", step_time)
        print(f"   Matched to GVKEY: {result['gvkey'].notna().mean():.1%}")
        print(f"   Matched to PERMNO: {result['permno'].notna().mean():.1%}")
        
        return result
    
    def build_panel(self):
        """Main function to build the monthly panel"""
        print("\n" + "="*60)
        print("BUILDING MONTHLY S&P 500 PANEL (IMPROVED)")
        print("="*60)
        
        # Load security master
        self.load_security_master()
        
        # Get S&P 500 monthly snapshots
        sp500_snapshots = self.get_sp500_monthly_snapshots()
        
        # Map to GVKEY/PERMNO with improved logic
        self.monthly_panel = self.map_to_gvkey_permno(sp500_snapshots)
        
        # Sort for clarity
        self.monthly_panel = self.monthly_panel.sort_values(
            ['date', 'gvkey']
        ).reset_index(drop=True)
        
        return self.monthly_panel
    
    def extract_annual_ml_grid(self):
        """Extract annual ML grid from monthly panel (December snapshots)"""
        if self.monthly_panel is None:
            raise ValueError("Build monthly panel first")
        
        print("\nExtracting annual ML grid...")
        
        # Take December snapshots
        december = self.monthly_panel[self.monthly_panel['month'] == 12].copy()
        
        # Already deduplicated by (date, gvkey) so this should be clean
        ml_grid = december.copy()
        
        # Add ML-specific columns with clear naming
        ml_grid['constituency_year'] = ml_grid['year']
        ml_grid['formation_year'] = ml_grid['year']  # When we form portfolios
        ml_grid['prediction_year'] = ml_grid['year'] + 1
        ml_grid['formation_date'] = ml_grid['date']  # Actual date, not hardcoded
        
        # ESG timing - explicit and clear
        ml_grid['esg_baseline_year'] = ml_grid['year'] - 1  # For baseline disagreement
        ml_grid['esg_outcome_year'] = ml_grid['year']  # For outcome disagreement
        # The change is from baseline to outcome
        
        # Select relevant columns
        ml_grid = ml_grid[[
            'gvkey', 'permno', 'ticker_crsp', 'company_name',
            'constituency_year', 'formation_year', 'prediction_year',
            'formation_date', 'esg_baseline_year', 'esg_outcome_year',
            'esg_knowledge_year', 'match_type', 'permno_type',
            'is_primary', 'is_common', 'exchange', 'linkprim', 'linktype'
        ]]
        
        print(f"   ML grid: {len(ml_grid):,} firm-years")
        print(f"   Years: {ml_grid['constituency_year'].min()}-{ml_grid['constituency_year'].max()}")
        print(f"   Unique GVKEYs: {ml_grid['gvkey'].nunique()}")
        
        return ml_grid
    
    def print_summary(self):
        """Print summary statistics"""
        if self.monthly_panel is None:
            print("No panel built yet")
            return
        
        print("\n" + "="*60)
        print("MONTHLY PANEL SUMMARY")
        print("="*60)
        
        print(f"Total rows: {len(self.monthly_panel):,}")
        print(f"Date range: {self.monthly_panel['date'].min()} to {self.monthly_panel['date'].max()}")
        print(f"Unique GVKEYs (as string): {self.monthly_panel['gvkey'].nunique()}")
        print(f"Unique PERMNOs: {self.monthly_panel['permno'].nunique()}")
        
        # Match quality
        print("\nMatch Quality:")
        print(f"  Match types:")
        print(self.monthly_panel['match_type'].value_counts())
        print(f"\n  PERMNO types:")
        print(self.monthly_panel['permno_type'].value_counts())
        
        # Check for duplicates (should be none after deduplication)
        dupes = self.monthly_panel.duplicated(subset=['date', 'gvkey'], keep=False).sum()
        print(f"\nDuplicate (date, gvkey) pairs: {dupes} (should be 0)")
        
        # Coverage by year
        print("\nAnnual Coverage:")
        yearly = self.monthly_panel.groupby('year').agg({
            'gvkey': 'nunique',
            'permno': 'nunique',
            'date': 'count'
        }).rename(columns={'date': 'total_rows'})
        print(yearly.tail(10))
    
    def save(self, output_dir):
        """Save the panel and ML grid"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save monthly panel
        monthly_file = output_path / 'monthly_sp500_panel.parquet'
        self.monthly_panel.to_parquet(monthly_file, index=False)
        print(f"\nSaved monthly panel to {monthly_file}")
        
        # Save ML grid
        ml_grid = self.extract_annual_ml_grid()
        ml_file = output_path / 'annual_ml_grid.csv'
        ml_grid.to_csv(ml_file, index=False)
        print(f"Saved ML grid to {ml_file}")
        
        # Save unmatched for investigation
        unmatched = self.monthly_panel[self.monthly_panel['match_type'] == 'UNMATCHED']
        if len(unmatched) > 0:
            unmatched_file = output_path / 'unmatched_constituents.csv'
            unmatched[['date', 'ticker_sp500', 'cusip_sp500']].drop_duplicates().to_csv(
                unmatched_file, index=False
            )
            print(f"WARNING: {len(unmatched)} unmatched records saved to {unmatched_file}")
        
        # Audit file - show ranking details for a sample
        audit_sample = self.monthly_panel.head(100)[
            ['date', 'gvkey', 'permno', 'match_type', 'permno_type', 'is_primary', 'linkprim', 'linktype']
        ]
        audit_file = output_path / 'audit_sample.csv'
        audit_sample.to_csv(audit_file, index=False)
        print(f"Audit sample saved to {audit_file}")

def main():
    """Main execution"""
    # Configuration - use proper relative paths
    project_root = Path(__file__).parent.parent.parent
    SP500_PATH = project_root / "data/raw/reference_data/spx_historical_constituents_with_identifiers.xlsx"
    SECURITY_MASTER_PATH = project_root / "data/processed/security_master/security_master_segments.csv"
    OUTPUT_DIR = project_root / "data/processed/sp500_panel"
    
    # Build the panel
    builder = SP500MonthlyPanel(SP500_PATH, SECURITY_MASTER_PATH)
    monthly_panel = builder.build_panel()
    
    # Print summary
    builder.print_summary()
    
    # Save outputs
    builder.save(OUTPUT_DIR)
    
    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print("\nKey improvements implemented:")
    print("- GVKEY kept as 6-char zero-padded string")
    print("- Proper ranking when multiple matches exist")
    print("- Deduplication by (date, gvkey)")
    print("- ESG knowledge year added for no look-ahead")
    print("- Audit trail for match quality")
    print("- Reused utilities from base mapper")

if __name__ == "__main__":
    main()