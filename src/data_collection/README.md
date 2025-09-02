# Data Collection Utilities

Scripts for downloading reference data from financial data providers. Most require special API access.

```
src/data_collection/
├── README.md
├── __init__.py                 
└── refinitiv_utils/            
    ├── __init__.py
    ├── spx_historical_constituents.py    # S&P 500 historical constituents
    └── [future scripts...]
```

## S&P 500 Historical Constituents

Downloads complete S&P 500 membership history with identifiers (CUSIP, ISIN, ticker) for use as a reference universe in ESG research.

**Key features:**
- Monthly snapshots of index membership from configurable date range
- Comprehensive identifier collection (RIC, ISIN, CUSIP, ticker)
- Robust error handling with checkpointing for long-running downloads
- Automatic fallback strategies when primary API calls fail
- Progress tracking and data quality validation

**Output:** Excel file saved to `data/raw/reference_data/spx_historical_constituents_with_identifiers.xlsx`

**Environment requirement:** Must be copied and executed inside Refinitiv Workspace CodeBook (requires `lseg.data` package).

### Implementation Details

The script uses a dual-approach strategy to handle Refinitiv API reliability issues:

1. **Primary method:** Attempts to get identifiers directly from the S&P 500 chain (`0#.SPX(YYYYMMDD)`) with fields `TR.ISIN` and `TR.CUSIP`
2. **Fallback method:** If primary fails, gets the chain constituents first, then makes batched identifier requests

**API handling:**
- Automatic session management with `lseg.data.open_session()`
- Request batching to respect rate limits
- Checkpoint saving every 12 months for resumability
- Multiple encoding attempts for data cleaning

**Data processing:**
- Column name normalization for consistent output
- Ticker extraction from RIC codes
- Duplicate removal based on Date+RIC combinations
- Coverage statistics reporting (ISIN/CUSIP completeness)

## Integration with ESG Pipeline

The downloaded S&P 500 data serves multiple purposes in ESG research:

- **Reference universe filtering:** Focus ESG analysis on large-cap companies during specific time periods
- **Improved identifier matching:** CUSIP/ISIN codes enhance matching reliability over ticker-only approaches  
- **Coverage analysis:** Quantify ESG data availability across the large-cap universe over time
- **Historical context:** Avoid survivorship bias by using period-appropriate index membership

The data integrates with the main pipeline's `spx_analysis.py` script for S&P 500-focused ESG coverage analysis.

## Future Extensions

Planned additions:
- Direct ESG score downloads via Refinitiv API
- Fundamental data collection for the same universe
- Price/return data for performance analysis
- Other index families (Russell, FTSE, MSCI)

## Technical Requirements

- **Environment:** Refinitiv Workspace CodeBook only
- **Dependencies:** `lseg.data` package (Refinitiv environment exclusive)
- **Permissions:** Historical index data access required
- **Performance:** 10-30 minutes for typical date ranges, <1GB memory
- **Output size:** 5-20MB depending on historical span

---

*Reference data collection utilities for ESG research infrastructure.*
