"""
Example usage of the ESG mappers.
Demonstrates how to use individual mappers and process results.
"""

from pathlib import Path
import pandas as pd

# Import mappers
from esg_mappers.refinitiv_mapper import RefinitivMapper
from esg_mappers.msci_mapper import MSCIMapper

def example_refinitiv():
    """Example: Processing Refinitiv data"""
    print("=" * 50)
    print("REFINITIV MAPPING EXAMPLE")
    print("=" * 50)
    
    # Initialize mapper
    mapper = RefinitivMapper(
        security_master_path="./data/processed/security_master/security_master_segments.csv",
        output_dir="./data/processed/id_mappings"
    )
    
    # Process data
    data_path = "./data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv"
    
    if Path(data_path).exists():
        matches, crosswalk = mapper.run(data_path)
        
        # Display results
        print(f"\nResults:")
        print(f"  Matches: {len(matches):,} orgpermid-year pairs")
        print(f"  Entities: {len(crosswalk):,} unique orgpermids")
        print(f"  Coverage: {100*len(matches)/len(mapper.provider_data):.1f}%")
        
        return matches, crosswalk
    else:
        print(f"Data file not found: {data_path}")
        return None, None

def example_msci():
    """Example: Processing MSCI data"""
    print("\n" + "=" * 50)
    print("MSCI MAPPING EXAMPLE")
    print("=" * 50)
    
    # Initialize mapper
    mapper = MSCIMapper(
        security_master_path="./data/processed/security_master/security_master_segments.csv",
        output_dir="./data/processed/id_mappings"
    )
    
    # Process data (directory of Excel files)
    data_path = "./data/raw/esg_ratings/msci/"
    
    if Path(data_path).exists():
        matches, crosswalk = mapper.run(data_path)
        
        # Display results
        print(f"\nResults:")
        print(f"  Matches: {len(matches):,} issuer-year pairs")
        print(f"  Entities: {len(crosswalk):,} unique issuers")
        print(f"  Coverage: {100*len(matches)/len(mapper.provider_data):.1f}%")
        
        return matches, crosswalk
    else:
        print(f"Data directory not found: {data_path}")
        return None, None

def analyze_results(refinitiv_matches=None, msci_matches=None):
    """Analyze and compare results across providers"""
    print("\n" + "=" * 50)
    print("CROSS-PROVIDER ANALYSIS")
    print("=" * 50)
    
    if refinitiv_matches is not None:
        print(f"Refinitiv:")
        print(f"  Years: {refinitiv_matches['year'].min()} - {refinitiv_matches['year'].max()}")
        print(f"  Unique GVKEYs: {refinitiv_matches['gvkey'].nunique():,}")
        print(f"  Match sources: {refinitiv_matches['match_src'].value_counts().to_dict()}")
    
    if msci_matches is not None:
        print(f"\nMSCI:")
        print(f"  Years: {msci_matches['year'].min()} - {msci_matches['year'].max()}")
        print(f"  Unique GVKEYs: {msci_matches['gvkey'].nunique():,}")
        print(f"  Match sources: {msci_matches['match_src'].value_counts().to_dict()}")
    
    # Find overlap in GVKEYs
    if refinitiv_matches is not None and msci_matches is not None:
        ref_gvkeys = set(refinitiv_matches['gvkey'].dropna())
        msci_gvkeys = set(msci_matches['gvkey'].dropna())
        overlap = ref_gvkeys & msci_gvkeys
        
        print(f"\nGVKEY Overlap:")
        print(f"  Common GVKEYs: {len(overlap):,}")
        print(f"  Refinitiv only: {len(ref_gvkeys - msci_gvkeys):,}")
        print(f"  MSCI only: {len(msci_gvkeys - ref_gvkeys):,}")

def load_consolidated_results():
    """Load and display consolidated results"""
    consolidated_path = "./data/processed/id_mappings/consolidated_esg_to_gvkey.csv"
    
    if Path(consolidated_path).exists():
        print("\n" + "=" * 50)
        print("CONSOLIDATED RESULTS")
        print("=" * 50)
        
        df = pd.read_csv(consolidated_path)
        
        print(f"Total mappings: {len(df):,}")
        print(f"Providers: {df['provider'].value_counts().to_dict()}")
        print(f"Unique GVKEYs: {df['gvkey'].nunique():,}")
        print(f"\nTop 10 GVKEYs by coverage:")
        print(df['gvkey'].value_counts().head(10))
        
        return df
    else:
        print(f"Consolidated file not found: {consolidated_path}")
        return None

if __name__ == "__main__":
    # Run examples
    ref_matches, ref_crosswalk = example_refinitiv()
    msci_matches, msci_crosswalk = example_msci()
    
    # Analyze results
    analyze_results(ref_matches, msci_matches)
    
    # Load consolidated results
    consolidated = load_consolidated_results()
    
    print("\nExample complete!")
