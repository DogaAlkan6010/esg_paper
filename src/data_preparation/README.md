# ESG Data Processing Module

This is where the actual work happens. It takes ESG data from various providers and figures out how to link it to your financial databases using GVKEY identifiers. Because apparently every ESG provider thinks their way of identifying companies is the best.

## How It's Built

The pipeline is built around a base class `BaseESGMapper` that handles all the boring common stuff, with provider-specific implementations for the weird quirks each provider has:

```
src/data_preparation/
├── README.md                    # This file
├── run_mappers.py              # Main orchestration script  
├── example_usage.py            # Usage examples
├── spx_analysis.py             # S&P 500 coverage analysis
└── esg_mappers/                # Mapper implementations
    ├── __init__.py
    ├── base_mapper.py          # Abstract base class
    ├── refinitiv_mapper.py     # Refinitiv implementation
    ├── msci_mapper.py         # MSCI implementation
    └── fmp_mapper.py          # FMP implementation
```

## What It Actually Does

- **Doesn't repeat itself**: Common logic is shared in the base class so we don't write the same matching code 3 times
- **Easy to extend**: Adding new ESG providers is straightforward (just inherit from the base class)
- **Smart matching**: Tries CUSIP codes first, then ISINs, then ticker symbols as a last resort
- **Quality scoring**: Picks the best matches based on data quality, not just the first one it finds
- **Progress tracking**: Shows you what's happening so you know it's not frozen
- **Handles different formats**: CSV, Excel, Parquet - whatever the providers decided to use

## Usage

### Basic Usage

```python
from esg_mappers.refinitiv_mapper import RefinitivMapper

# Initialize mapper
mapper = RefinitivMapper(
    security_master_path="./data/processed/security_master/security_master_segments.csv",
    output_dir="./data/processed/id_mappings"
)

# Run mapping
matches, crosswalk = mapper.run("../../data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv")
```

### Process Everything at Once

Use the main script to process all configured data sources:

```bash
cd src/data_preparation
python run_mappers.py
```

This is probably what you want to do. It processes all enabled providers and creates all the mapping files.

### Configuration

Edit the `get_data_sources()` function in `run_mappers.py` if your files are in different locations:

```python
def get_data_sources():
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
        "fmp": {
            "path": str(project_root / "data/raw/esg_ratings/fmp/fmp_esg_panel.parquet"),  # Parquet file
            "enabled": True
        },
    }
```

## Adding New Providers

If you have ESG data from other providers, here's how to add support:

1. Create a new mapper class that inherits from `BaseESGMapper`
2. Implement the required abstract methods:
   - `load_provider_data()`: Load and clean your raw data files
   - `extract_identifiers()`: Extract CUSIP6, ISIN, ticker symbols, etc.
   - `perform_matching()`: Execute the matching logic (usually just calls base class methods)
3. Add your mapper to `MAPPER_REGISTRY` in `run_mappers.py`
4. Configure the data source path in `get_data_sources()`

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

## What You Get

The pipeline generates several output files for each provider:

1. **Yearly Matches**: `{provider}_{entity_id}_year_match.csv`
   - Every company-year combination that was successfully matched
   - Includes match scores, overlap days, and quality indicators
   - Good for detailed analysis and debugging

2. **Entity Crosswalk**: `{provider}_{entity_id}_to_gvkey.csv`
   - One row per company with its best GVKEY mapping
   - This is probably what you want for most research
   - Aggregated statistics across all years

3. **Consolidated Mapping**: `consolidated_esg_to_gvkey.csv`
   - Combined mapping from all providers
   - Shows which companies appear in multiple ESG datasets

## Bonus Analysis Tools

### S&P 500 Coverage Analysis

If you have S&P 500 historical constituents data, you can analyze ESG coverage for large-cap companies:

```bash
cd src/data_preparation
python spx_analysis.py
```

This requires the S&P 500 historical constituents file at:
`data/raw/reference_data/spx_historical_constituents_with_identifiers.xlsx`

(You can get this data using the script in `src/data_collection/refinitiv_utils/`)

## How the Matching Actually Works

The matching process follows this hierarchy (from most reliable to least):

1. **CUSIP6 Matching**: Direct match using 6-digit CUSIP codes (most reliable)
2. **ISIN Matching**: Extracts CUSIP6 from North American ISINs (pretty good)
3. **Ticker Matching**: Fallback using ticker symbols (least reliable, but better than nothing)

Each match gets a score based on:
- Match source type (CUSIP6 beats ISIN beats Ticker)
- Security characteristics (common stock is preferred over preferred stock)
- Exchange preference (NYSE and NASDAQ are preferred)
- Overlap duration (longer overlaps get higher scores)
- CRSP-Compustat link quality scores

## Requirements

- Python 3.8+ (probably works on older versions but untested)
- pandas >= 1.5.0 (for the data manipulation)
- numpy >= 1.21.0 (for the math stuff)
- python-calamine >= 0.4.0 (for faster Excel reading)
- openpyxl >= 3.0.0 (backup Excel reader)

## What Data You Need

- **Security Master**: A CSV file with CRSP-Compustat linking data (the pipeline can build this automatically from raw CRSP files)
- **ESG Data**: Provider-specific files in whatever format they decided to use:
  - Refinitiv: CSV with columns like [orgpermid, year, cusip, isin, ticker, comname]
  - MSCI: Excel files with columns like [ISSUERID, YEAR, ISSUER_CUSIP, ISSUER_ISIN, ISSUER_TICKER, ISSUER_NAME]
  - FMP: Parquet file with columns like [symbol, periodEndDate, isin, Environmental, Social, Governance, ESG]
- **Reference Data** (Optional): S&P 500 historical constituents for coverage analysis

See the main project README and setup guide for detailed file locations and formats.