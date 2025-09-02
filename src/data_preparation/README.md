# ESG Data Processing Pipeline

This module provides a modular, extensible pipeline for mapping ESG data provider identifiers (like OrgPermID, IssuerID) to GVKEY for linkage with Compustat data.

## Architecture

The pipeline is built around a base class `BaseESGMapper` that handles common functionality, with provider-specific implementations that extend it:

```
src/data_preparation/
├── esg_mappers/
│   ├── __init__.py
│   ├── base_mapper.py          # Abstract base class with common logic
│   ├── refinitiv_mapper.py     # Refinitiv-specific implementation
│   ├── msci_mapper.py         # MSCI-specific implementation
│   └── [future mappers...]    # Easy to add new providers
├── run_mappers.py             # Main orchestration script
└── README.md                  # This file
```

## Features

- **DRY Principle**: Common logic is shared in the base class
- **Modular**: Easy to add new ESG data providers
- **Robust Matching**: Multi-step matching using CUSIP6, ISIN, and ticker symbols
- **Quality Scoring**: Prioritizes matches based on data quality and security characteristics
- **Progress Tracking**: Timestamped progress messages throughout the process
- **Flexible Input**: Handles various file formats (CSV for Refinitiv, Excel for MSCI)

## Usage

### Basic Usage

```python
from esg_mappers.refinitiv_mapper import RefinitivMapper

# Initialize mapper
mapper = RefinitivMapper(
    security_master_path="path/to/security_master_segments.csv",
    output_dir="./processed/id_mappings"
)

# Run mapping
matches, crosswalk = mapper.run("path/to/refinitiv_data.csv")
```

### Run All Mappers

Use the main script to process all configured data sources:

```bash
cd src/data_preparation
python run_mappers.py
```

### Configuration

Edit the `DATA_SOURCES` dictionary in `run_mappers.py` to configure your data paths:

```python
DATA_SOURCES = {
    "refinitiv": {
        "path": "./data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv",
        "enabled": True
    },
    "msci": {
        "path": "./data/raw/esg_ratings/msci/",  # Directory of Excel files
        "enabled": True
    },
}
```

## Adding New Providers

1. Create a new mapper class inheriting from `BaseESGMapper`
2. Implement the required abstract methods:
   - `load_provider_data()`: Load and standardize raw data
   - `extract_identifiers()`: Extract CUSIP6, ISIN, ticker etc.
   - `perform_matching()`: Execute the matching logic
3. Add to `MAPPER_REGISTRY` in `run_mappers.py`
4. Configure data source in `DATA_SOURCES`

Example:

```python
class NewProviderMapper(BaseESGMapper):
    def __init__(self, ...):
        super().__init__(...)
        self.provider_name = "new_provider"
        self.entity_id_col = "entity_id"
        self.entity_name_col = "company_name"
    
    def load_provider_data(self, data_path: str) -> pd.DataFrame:
        # Provider-specific loading logic
        pass
    
    def extract_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        # Extract CUSIP6, ISIN, etc.
        pass
    
    def perform_matching(self, provider_data: pd.DataFrame, 
                        security_master: pd.DataFrame) -> pd.DataFrame:
        # Use base class methods like match_by_cusip6()
        pass
```

## Output Files

The pipeline generates several output files:

1. **Yearly Matches**: `{provider}_{entity_id}_year_match.csv`
   - Detailed year-by-year matching results
   - Includes match scores, overlap days, and data quality indicators

2. **Entity Crosswalk**: `{provider}_{entity_id}_to_gvkey.csv`
   - One record per entity with best GVKEY mapping
   - Aggregated statistics across years

3. **Consolidated Mapping**: `consolidated_esg_to_gvkey.csv`
   - Combined mapping from all providers
   - Useful for analysis across data sources

## Matching Logic

The matching process follows this hierarchy:

1. **CUSIP6 Matching**: Direct match using 6-digit CUSIP codes
2. **ISIN Matching**: Extract CUSIP6 from North American ISINs
3. **Ticker Matching**: Fallback using ticker symbols (lower quality)

Each match is scored based on:
- Match source type (CUSIP6 > ISIN > Ticker)
- Security characteristics (common stock, primary PERMNO)
- Exchange preference (NYSE, NASDAQ)
- Overlap duration
- Link quality scores

## Requirements

- Python 3.8+
- pandas >= 1.5.0
- numpy >= 1.21.0
- python-calamine >= 0.4.0 (for Excel files)
- openpyxl >= 3.0.0

## Data Requirements

- **Security Master**: A CSV file with CRSP-Compustat linking data
- **ESG Data**: Provider-specific files in the configured formats
  - Refinitiv: CSV with columns [orgpermid, year, cusip, isin, ticker, comname]
  - MSCI: Excel files with columns [ISSUERID, YEAR, ISSUER_CUSIP, ISSUER_ISIN, ISSUER_TICKER, ISSUER_NAME]
