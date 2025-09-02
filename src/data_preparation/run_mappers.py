"""
Main script to run all ESG data provider mappings.
Easy to extend with new providers - just add to MAPPER_REGISTRY.
"""

import sys
from pathlib import Path
from typing import Dict, Type
import pandas as pd

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

# Import all mapper classes
from esg_mappers.base_mapper import BaseESGMapper
from esg_mappers.refinitiv_mapper import RefinitivMapper
from esg_mappers.msci_mapper import MSCIMapper
# from esg_mappers.sustainalytics_mapper import SustAnalyticsMapper  # Future
# from esg_mappers.sp_mapper import SPMapper  # Future
# from esg_mappers.fmp_mapper import FMPMapper  # Future

# -------------------------
# MAPPER REGISTRY
# -------------------------
# Add new providers here - no other code changes needed!
MAPPER_REGISTRY: Dict[str, Type[BaseESGMapper]] = {
    "refinitiv": RefinitivMapper,
    "msci": MSCIMapper,
    # "sustainalytics": SustAnalyticsMapper,  # Uncomment when implemented
    # "sp_global": SPMapper,  # Uncomment when implemented
}

# -------------------------
# DATA SOURCE CONFIGURATION
# -------------------------
# Configure your data sources here  
def get_data_sources():
    """Get data source paths relative to project root"""
    # Get project root (src/data_preparation -> project root)
    project_root = Path(__file__).parent.parent.parent
    
    return {
        "refinitiv": {
            "path": str(project_root / "data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv"),
            "enabled": True
        },
        "msci": {
            "path": str(project_root / "data/raw/esg_ratings/msci/"),  # Directory of Excel files
            "enabled": True
        },
        # "sustainalytics": {
        #     "path": str(project_root / "data/raw/esg_ratings/sustainalytics/"),
        #     "enabled": False
        # },
    }

def run_single_mapper(provider: str, config: dict, security_master_path: str, output_dir: str):
    """Run a single mapper"""
    
    print(f"\n{'='*70}")
    print(f"Processing {provider.upper()} data")
    print(f"{'='*70}")
    
    # Check if mapper exists
    if provider not in MAPPER_REGISTRY:
        print(f"ERROR: No mapper found for {provider}")
        return None, None
    
    # Check if data exists
    data_path = Path(config["path"])
    if not data_path.exists():
        print(f"ERROR: Data path not found: {data_path}")
        return None, None
    
    try:
        # Initialize mapper
        mapper_class = MAPPER_REGISTRY[provider]
        mapper = mapper_class(
            security_master_path=security_master_path,
            output_dir=output_dir
        )
        
        # Run mapping
        matches, crosswalk = mapper.run(str(data_path))
        
        return matches, crosswalk
        
    except Exception as e:
        print(f"ERROR processing {provider}: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def create_consolidated_mapping(output_dir: str):
    """Create a consolidated mapping file combining all providers"""
    
    output_path = Path(output_dir)
    
    print(f"\n{'='*70}")
    print("Creating consolidated mapping")
    print(f"{'='*70}")
    
    all_crosswalks = []
    
    # Load all individual crosswalks
    for provider in MAPPER_REGISTRY.keys():
        # Look for crosswalk files
        pattern = f"{provider}_*_to_gvkey.csv"
        crosswalk_files = list(output_path.glob(pattern))
        
        for file in crosswalk_files:
            try:
                df = pd.read_csv(file)
                df["provider"] = provider
                
                # Standardize column names
                # Find the entity ID column (first column that's not 'gvkey')
                entity_col = [col for col in df.columns if col != "gvkey" and col != "provider"][0]
                df = df.rename(columns={entity_col: "entity_id"})
                
                # Keep only essential columns
                keep_cols = ["provider", "entity_id", "gvkey", "primary_permno", 
                            "years_covered", "first_seen", "last_seen"]
                df = df[[col for col in keep_cols if col in df.columns]]
                
                all_crosswalks.append(df)
                print(f"   Loaded {len(df):,} mappings from {file.name}")
                
            except Exception as e:
                print(f"   ERROR loading {file}: {e}")
    
    if all_crosswalks:
        # Combine all crosswalks
        consolidated = pd.concat(all_crosswalks, ignore_index=True)
        
        # Save consolidated file
        output_file = output_path / "consolidated_esg_to_gvkey.csv"
        consolidated.to_csv(output_file, index=False)
        
        print(f"\nSaved consolidated mapping with {len(consolidated):,} total mappings")
        print(f"   Providers: {consolidated['provider'].value_counts().to_dict()}")
        print(f"   Unique GVKEYs: {consolidated['gvkey'].nunique():,}")
        
        return consolidated
    
    return None

def build_security_master_if_needed():
    """Build security master if it doesn't exist and raw CRSP files are available"""
    # Get project root (go up from src/data_preparation to project root)
    project_root = Path(__file__).parent.parent.parent
    security_master_path = project_root / "data/processed/security_master/security_master_segments.csv"
    
    if security_master_path.exists():
        print("✅ Security master already exists")
        return True
    
    print("❌ Security master not found. Checking for raw CRSP files...")
    
    # Check if raw CRSP files exist
    names_file = project_root / "data/raw/crsp/CRSP_STOCK_EVENTS_HIST_DESC_INF_FULL_DB.csv"
    link_file = project_root / "data/raw/crsp/CRSP_COMPUSTAT_LINKING_TABLE_FULL_DATABASE.csv"
    
    if names_file.exists() and link_file.exists():
        print("✅ Raw CRSP files found. Building security master...")
        
        try:
            # Import and run security master builder
            from security_master.build_security_master import build_security_master
            return build_security_master()
        except Exception as e:
            print(f"❌ Error building security master: {e}")
            return False
    else:
        print("❌ Raw CRSP files not found. Please either:")
        print(f"   1. Add CRSP files to data/raw/crsp/:")
        print(f"      - {names_file}")
        print(f"      - {link_file}")
        print(f"   2. Or manually place security_master_segments.csv at:")
        print(f"      - {security_master_path}")
        return False

def main():
    """Main execution function"""
    
    # Configuration - use absolute paths from project root
    project_root = Path(__file__).parent.parent.parent
    SECURITY_MASTER = str(project_root / "data/processed/security_master/security_master_segments.csv")
    OUTPUT_DIR = str(project_root / "data/processed/id_mappings")
    
    # Create output directory
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    print("\n" + "="*70)
    print("ESG DATA PROVIDER MAPPING PIPELINE")
    print("="*70)
    
    # Check/build security master
    if not build_security_master_if_needed():
        return
    
    # Track results
    results = {}
    
    # Process each enabled data source  
    for provider, config in get_data_sources().items():
        if config.get("enabled", False):
            matches, crosswalk = run_single_mapper(
                provider, config, SECURITY_MASTER, OUTPUT_DIR
            )
            
            results[provider] = {
                "matches": len(matches) if matches is not None else 0,
                "entities": len(crosswalk) if crosswalk is not None else 0
            }
    
    # Create consolidated mapping
    consolidated = create_consolidated_mapping(OUTPUT_DIR)
    
    # Print final summary
    print("\n" + "="*70)
    print("PIPELINE SUMMARY")
    print("="*70)
    
    for provider, stats in results.items():
        print(f"{provider:15s}: {stats['matches']:,} matches, {stats['entities']:,} entities")
    
    if consolidated is not None:
        print(f"\nTotal consolidated mappings: {len(consolidated):,}")
    
    print("\nPipeline complete!")

if __name__ == "__main__":
    main()
