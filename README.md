# ESG Paper Data Processing Pipeline

This repository contains a modular, extensible pipeline for processing ESG (Environmental, Social, Governance) data from multiple providers and linking it to financial databases via GVKEY identifiers.

## 🏗️ Project Structure

```
esg_paper/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── data/                       # Data directories (created automatically)
│   ├── raw/
│   │   └── esg_ratings/
│   │       ├── refinitiv/
│   │       └── msci/
│   └── processed/
│       ├── security_master/
│       └── id_mappings/
└── src/                        # Source code
    └── data_preparation/
        ├── README.md           # Detailed module documentation
        ├── run_mappers.py      # Main orchestration script
        ├── example_usage.py    # Usage examples
        └── esg_mappers/        # Mapper implementations
            ├── __init__.py
            ├── base_mapper.py          # Abstract base class
            ├── refinitiv_mapper.py     # Refinitiv implementation
            └── msci_mapper.py          # MSCI implementation
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Prepare Your Data

- Place your security master file at: `data/processed/security_master/security_master_segments.csv`
- Place ESG data files:
  - Refinitiv: `data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv`
  - MSCI: Excel files in `data/raw/esg_ratings/msci/`

### 3. Run the Pipeline

```bash
cd src/data_preparation
python run_mappers.py
```

## 🎯 Key Features

### ✅ **Modular & Extensible**
- Abstract base class handles common functionality
- Easy to add new ESG data providers
- DRY (Don't Repeat Yourself) architecture

### ✅ **Robust Matching Logic**
- Multi-step identifier matching (CUSIP6, ISIN, Ticker)
- Quality scoring system for match prioritization
- Handles various data formats and encodings

### ✅ **Production Ready**
- Comprehensive error handling
- Progress tracking with timestamps
- Detailed logging and output files

### ✅ **Multiple Output Formats**
- Yearly match details
- Entity-level crosswalks
- Consolidated cross-provider mapping

## 📊 Supported Providers

| Provider | Identifier | Input Format | Status |
|----------|------------|--------------|--------|
| **Refinitiv** | OrgPermID | CSV | ✅ Implemented |
| **MSCI** | IssuerID | Excel | ✅ Implemented |
| Sustainalytics | Entity ID | CSV | 🔄 Future |
| S&P Global | Entity ID | CSV | 🔄 Future |

## 🛠️ Usage Examples

### Individual Mapper Usage

```python
from esg_mappers.refinitiv_mapper import RefinitivMapper

# Initialize mapper
mapper = RefinitivMapper(
    security_master_path="path/to/security_master.csv",
    output_dir="./processed/id_mappings"
)

# Run mapping
matches, crosswalk = mapper.run("path/to/refinitiv_data.csv")

print(f"Matched {len(matches):,} entity-year pairs")
print(f"Mapped {len(crosswalk):,} unique entities")
```

### Batch Processing

```python
cd src/data_preparation
python run_mappers.py  # Processes all enabled providers
```

### Analysis & Results

```python
python example_usage.py  # Runs examples and shows analysis
```

## 📈 Output Files

The pipeline generates three types of output files:

### 1. **Yearly Matches** (`{provider}_{entity_id}_year_match.csv`)
Detailed year-by-year matching results with:
- Match source (CUSIP6, ISIN, Ticker)
- Quality scores and overlap calculations
- Security characteristics and exchange information

### 2. **Entity Crosswalk** (`{provider}_{entity_id}_to_gvkey.csv`)
One record per entity with:
- Best GVKEY mapping based on aggregated scores
- Coverage statistics (years covered, first/last seen)
- Primary PERMNO assignments

### 3. **Consolidated Mapping** (`consolidated_esg_to_gvkey.csv`)
Combined mapping across all providers for cross-provider analysis.

## 🔧 Adding New Providers

Adding a new ESG data provider is straightforward:

1. **Create a new mapper class**:
```python
from esg_mappers.base_mapper import BaseESGMapper

class NewProviderMapper(BaseESGMapper):
    def __init__(self, ...):
        super().__init__(...)
        self.provider_name = "new_provider"
        self.entity_id_col = "entity_id"
    
    def load_provider_data(self, data_path: str) -> pd.DataFrame:
        # Load and clean your data
        pass
    
    def extract_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        # Extract CUSIP6, ISIN, ticker symbols
        pass
    
    def perform_matching(self, provider_data, security_master) -> pd.DataFrame:
        # Use inherited methods like match_by_cusip6()
        pass
```

2. **Register the mapper**:
```python
# In run_mappers.py
MAPPER_REGISTRY["new_provider"] = NewProviderMapper

DATA_SOURCES["new_provider"] = {
    "path": "./data/raw/esg_ratings/new_provider/",
    "enabled": True
}
```

## 🧪 Testing

The pipeline includes example usage scripts that demonstrate functionality:

```bash
cd src/data_preparation
python example_usage.py
```

## 📋 Requirements

- **Python**: 3.8+
- **Core Libraries**: pandas, numpy
- **Excel Support**: python-calamine, openpyxl
- **Memory**: Varies by dataset size (typically 1-4GB RAM)

## 🗂️ Data Setup

The pipeline requires several data files. You have **two options** for the security master:

### **Option 1: Auto-build from Raw CRSP (Recommended)**
Place your raw CRSP files here - the pipeline will build the security master automatically:
```
data/raw/crsp/CRSP_STOCK_EVENTS_HIST_DESC_INF_FULL_DB.csv
data/raw/crsp/CRSP_COMPUSTAT_LINKING_TABLE_FULL_DATABASE.csv
```

### **Option 2: Use Pre-built Security Master**
If you already have a security master file:
```
data/processed/security_master/security_master_segments.csv
```

### **ESG Data Files (Required)**
```
data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv
data/raw/esg_ratings/msci/*.xlsx
```

**📖 For detailed setup instructions, see [SETUP_GUIDE.md](SETUP_GUIDE.md)**

**✅ Check your setup:**
```bash
python3 check_data_files.py
```

## 🤝 Contributing

The modular design makes it easy to contribute:

1. **New Providers**: Follow the template in the "Adding New Providers" section
2. **Base Functionality**: Enhance the `BaseESGMapper` class for shared improvements
3. **Output Formats**: Add new analysis functions to the main runner script

## 📄 Data Requirements

### Security Master File
- Columns: `permno`, `gvkey`, `namedt`, `nameendt`, `ncusip6`, `ticker`, etc.
- Format: CSV with CRSP-Compustat linking data

### ESG Provider Data
- **Refinitiv**: CSV with `orgpermid`, `year`, `cusip`, `isin`, `ticker`, `comname`
- **MSCI**: Excel files with `ISSUERID`, `YEAR`, `ISSUER_CUSIP`, `ISSUER_ISIN`, etc.

## 🎯 Design Principles

1. **Separation of Concerns**: Each mapper handles only its provider's specifics
2. **Reusability**: Common logic is shared via inheritance
3. **Extensibility**: New providers require minimal code changes
4. **Robustness**: Comprehensive error handling and data validation
5. **Transparency**: Detailed progress reporting and match quality scoring

---

*Built for academic research with a focus on modularity, reproducibility, and extensibility.*
