# ESG Data Processing Pipeline

Automatically links ESG ratings from different providers to financial databases. Because every ESG provider decided to invent their own company identifiers, and someone has to deal with the mess.

## The Problem

ESG providers use their own identifiers, but your financial data uses different ones:
- Refinitiv: "OrgPermID 4295905573"  
- MSCI: "IssuerID APPLE_INC" 
- Your CRSP data: "GVKEY 001690"

## The Solution

This pipeline creates mappings between provider identifiers and standard financial database keys (GVKEY, PERMNO) using CUSIP codes, ISINs, and ticker symbols as bridges. It handles the matching logic, quality scoring, and deduplication so you don't have to.

## Project Structure

```
esg_paper/
├── data/
│   ├── raw/
│   │   ├── esg_ratings/        # ESG provider files
│   │   ├── crsp/              # Raw CRSP files (optional)
│   │   └── reference_data/    # S&P 500 constituents, etc.
│   └── processed/
│       ├── security_master/   # CRSP-Compustat linking table
│       └── id_mappings/       # Output mapping files
└── src/
    ├── data_collection/       # Scripts for downloading data
    └── data_preparation/      # Main ESG mapping pipeline
        ├── run_mappers.py     # Orchestrates all providers
        ├── esg_mappers/       # Provider-specific logic
        └── security_master/   # Builds linking table from CRSP
```

## How It Works

The pipeline uses a base class that handles common matching logic, with provider-specific implementations for data loading and identifier extraction. Each provider mapper:

1. **Loads and cleans** provider data (handles different file formats, encodings)
2. **Extracts identifiers** (CUSIP, ISIN, ticker) using provider-specific logic
3. **Matches to security master** using hierarchical matching (CUSIP > ISIN > ticker)
4. **Scores matches** based on overlap duration, security type, exchange quality
5. **Selects best match** per company using aggregated scores

The security master can be auto-built from raw CRSP files or provided pre-built.

## Supported Providers

- **Refinitiv** (CSV) - OrgPermID → GVKEY via CUSIP/ISIN matching
- **MSCI** (Excel) - IssuerID → GVKEY via CUSIP/ISIN matching  
- **FMP** (Parquet) - Symbol → GVKEY via ticker/ISIN matching

## Output Files

For each provider:
- **`*_year_match.csv`** - Detailed yearly matches with quality scores
- **`*_to_gvkey.csv`** - Entity-level crosswalk (best GVKEY per company)
- **`consolidated_esg_to_gvkey.csv`** - Combined mapping across all providers

## Matching Algorithm

Uses hierarchical matching with quality scoring:
1. **CUSIP6 matching** (most reliable)
2. **ISIN matching** (extracts CUSIP6 for North American securities)  
3. **Ticker matching** (fallback, least reliable)

Match scores consider identifier type, security characteristics (common vs preferred), exchange quality (NYSE/NASDAQ preferred), and temporal overlap duration.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Check data setup
python check_data_files.py

# Run pipeline
cd src/data_preparation && python run_mappers.py
```

## Data Collection

The `src/data_collection/` directory contains scripts for downloading reference data:

- **S&P 500 Historical Constituents** (`refinitiv_utils/spx_historical_constituents.py`)
  - Downloads monthly S&P 500 membership with identifiers
  - Must be copied and run inside Refinitiv Workspace CodeBook
  - Output stored in `data/raw/reference_data/spx_historical_constituents_with_identifiers.xlsx`
  - Useful for analyzing ESG coverage of large-cap universe

## Adding Providers

Extend `BaseESGMapper` and implement:
- `load_provider_data()` - Handle provider file format
- `extract_identifiers()` - Extract CUSIP/ISIN/ticker
- `perform_matching()` - Usually just calls base class methods

Register in `run_mappers.py` MAPPER_REGISTRY.

## Requirements

- Python 3.8+, pandas, numpy, openpyxl, python-calamine
- 1-4GB RAM depending on data size
- See [SETUP_GUIDE.md](SETUP_GUIDE.md) for file locations and formats

---

*Academic research tool. Makes ESG identifier matching less painful.*
