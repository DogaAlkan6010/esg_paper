#!/usr/bin/env python3
"""
Quick script to check if required data files are in the correct locations.
Run this before executing the main pipeline.
"""

from pathlib import Path
import sys

def check_file_exists(path, description, required=True):
    """Check if a file exists and print status"""
    path_obj = Path(path)
    exists = path_obj.exists()
    
    if required:
        status = "✅ FOUND" if exists else "❌ MISSING"
    else:
        status = "✅ FOUND" if exists else "⚪ OPTIONAL (missing)"
    
    print(f"{status:20} {description}")
    print(f"                     Expected at: {path}")
    
    if exists and path_obj.is_file():
        size_mb = path_obj.stat().st_size / (1024 * 1024)
        print(f"                     File size: {size_mb:.1f} MB")
    elif exists and path_obj.is_dir():
        files = list(path_obj.glob("*.xlsx"))
        print(f"                     Found {len(files)} Excel files")
        for f in files[:3]:  # Show first 3 files
            print(f"                       - {f.name}")
        if len(files) > 3:
            print(f"                       ... and {len(files) - 3} more")
    
    print()
    return exists

def main():
    """Check all required data files"""
    print("🔍 CHECKING REQUIRED DATA FILES FOR ESG PIPELINE")
    print("=" * 70)
    
    all_required_good = True
    
    # Option 1: Raw CRSP files (will auto-build security master)
    print("📁 OPTION 1: Raw CRSP Files (will auto-build security master)")
    print("-" * 50)
    
    crsp_names = check_file_exists(
        "data/raw/crsp/CRSP_STOCK_EVENTS_HIST_DESC_INF_FULL_DB.csv",
        "CRSP Stock Events (Names)",
        required=False
    )
    
    crsp_link = check_file_exists(
        "data/raw/crsp/CRSP_COMPUSTAT_LINKING_TABLE_FULL_DATABASE.csv", 
        "CRSP-Compustat Link Table",
        required=False
    )
    
    has_crsp_files = crsp_names and crsp_link
    
    print("📁 OPTION 2: Pre-built Security Master")
    print("-" * 50)
    
    # Option 2: Pre-built security master
    security_master = check_file_exists(
        "data/processed/security_master/security_master_segments.csv",
        "Security Master (pre-built)",
        required=False
    )
    
    # At least one option should be available
    if not (has_crsp_files or security_master):
        print("❌ PROBLEM: Need either raw CRSP files OR pre-built security master")
        all_required_good = False
    elif has_crsp_files:
        print("✅ Will auto-build security master from raw CRSP files")
    elif security_master:
        print("✅ Using pre-built security master")
    
    print("📁 ESG DATA FILES")
    print("-" * 50)
    
    # Refinitiv Data
    refinitiv_exists = check_file_exists(
        "data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv",
        "Refinitiv ESG Data"
    )
    all_required_good &= refinitiv_exists
    
    # MSCI Data Directory
    msci_exists = check_file_exists(
        "data/raw/esg_ratings/msci/",
        "MSCI ESG Data Directory"
    )
    all_required_good &= msci_exists
    
    # Check for MSCI files specifically
    if msci_exists:
        msci_path = Path("data/raw/esg_ratings/msci/")
        excel_files = list(msci_path.glob("*.xlsx"))
        if not excel_files:
            print("⚠️  WARNING: No Excel files found in MSCI directory")
            print("             Expected files like: ESG Ratings Timeseries*.xlsx")
            all_required_good = False
    
    # FMP Data
    fmp_exists = check_file_exists(
        "data/raw/esg_ratings/fmp/fmp_esg_panel.parquet",
        "FMP ESG Data (Parquet)",
        required=False
    )
    
    # Reference Data
    print("📁 REFERENCE DATA")
    print("-" * 50)
    
    spx_exists = check_file_exists(
        "data/raw/reference_data/spx_historical_constituents_with_identifiers.xlsx",
        "S&P 500 Historical Constituents",
        required=False
    )
    
    # Summary
    print("=" * 70)
    if all_required_good:
        print("🎉 ALL REQUIRED FILES READY!")
        print()
        if has_crsp_files and not security_master:
            print("   The pipeline will automatically build the security master first.")
        print("   You can run the pipeline:")
        print("   cd src/data_preparation")
        print("   python3 run_mappers.py")
    else:
        print("❌ MISSING REQUIRED FILES!")
        print()
        print("   Please add the missing files. You have two options for the security master:")
        print() 
        print("   OPTION 1 (Recommended): Add raw CRSP files")
        print("     data/raw/crsp/CRSP_STOCK_EVENTS_HIST_DESC_INF_FULL_DB.csv")
        print("     data/raw/crsp/CRSP_COMPUSTAT_LINKING_TABLE_FULL_DATABASE.csv")
        print("     → Pipeline will auto-build security master")
        print()
        print("   OPTION 2: Add pre-built security master")  
        print("     data/processed/security_master/security_master_segments.csv")
        print()
        print("   Plus ESG data files:")
        print("     data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv")
        print("     data/raw/esg_ratings/msci/*.xlsx files")
        print("     data/raw/esg_ratings/fmp/fmp_esg_panel.parquet (optional)")
    
    return 0 if all_required_good else 1

if __name__ == "__main__":
    sys.exit(main())
