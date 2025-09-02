"""
Example usage of the ESG mappers.
Demonstrates how to use individual mappers and process results.
"""

from pathlib import Path
import pandas as pd

# Import mappers
from esg_mappers.refinitiv_mapper import RefinitivMapper
from esg_mappers.msci_mapper import MSCIMapper
from esg_mappers.fmp_mapper import FMPMapper

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

def example_fmp():
    """Example: Processing FMP data"""
    print("\n" + "=" * 50)
    print("FMP MAPPING EXAMPLE")
    print("=" * 50)
    
    # Initialize mapper
    mapper = FMPMapper(
        security_master_path="./data/processed/security_master/security_master_segments.csv",
        output_dir="./data/processed/id_mappings"
    )
    
    # Process data (Parquet file)
    data_path = "./data/raw/esg_ratings/fmp/fmp_esg_panel.parquet"
    
    if Path(data_path).exists():
        matches, crosswalk = mapper.run(data_path)
        
        # Display results
        print(f"\nResults:")
        print(f"  Matches: {len(matches):,} symbol-year pairs")
        print(f"  Entities: {len(crosswalk):,} unique symbols")
        print(f"  Coverage: {100*len(matches)/len(mapper.provider_data):.1f}%")
        
        return matches, crosswalk
    else:
        print(f"Data file not found: {data_path}")
        return None, None

def analyze_results(refinitiv_matches=None, msci_matches=None, fmp_matches=None):
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
    
    if fmp_matches is not None:
        print(f"\nFMP:")
        print(f"  Years: {fmp_matches['year'].min()} - {fmp_matches['year'].max()}")
        print(f"  Unique GVKEYs: {fmp_matches['gvkey'].nunique():,}")
        print(f"  Match sources: {fmp_matches['match_src'].value_counts().to_dict()}")
    
    # Find overlap in GVKEYs
    all_providers = [refinitiv_matches, msci_matches, fmp_matches]
    valid_providers = [p for p in all_providers if p is not None]
    
    if len(valid_providers) >= 2:
        all_gvkeys = [set(p['gvkey'].dropna()) for p in valid_providers]
        provider_names = []
        if refinitiv_matches is not None:
            provider_names.append("Refinitiv")
        if msci_matches is not None:
            provider_names.append("MSCI")
        if fmp_matches is not None:
            provider_names.append("FMP")
        
        print(f"\nGVKEY Overlap Analysis:")
        if len(all_gvkeys) >= 2:
            # Calculate intersections
            common_all = set.intersection(*all_gvkeys)
            print(f"  Common across all providers: {len(common_all):,}")
            
            # Pairwise overlaps
            for i in range(len(all_gvkeys)):
                for j in range(i+1, len(all_gvkeys)):
                    overlap = all_gvkeys[i] & all_gvkeys[j]
                    print(f"  {provider_names[i]} ∩ {provider_names[j]}: {len(overlap):,}")
            
            # Provider-specific
            for i, (gvkeys, name) in enumerate(zip(all_gvkeys, provider_names)):
                others = set.union(*[all_gvkeys[j] for j in range(len(all_gvkeys)) if j != i])
                unique = gvkeys - others
                print(f"  {name} only: {len(unique):,}")

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
    fmp_matches, fmp_crosswalk = example_fmp()
    
    # Analyze results
    analyze_results(ref_matches, msci_matches, fmp_matches)
    
    # Load consolidated results
    consolidated = load_consolidated_results()
    
    print("\nExample complete!")
