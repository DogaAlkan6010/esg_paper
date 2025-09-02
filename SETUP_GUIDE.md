# ESG Pipeline Setup Guide

This guide shows you exactly where to put your data files to run the ESG mapping pipeline.

## 📁 Directory Structure Overview

```
esg_paper/
├── data/
│   ├── raw/                          # Your raw data files go here
│   │   ├── crsp/                     # Raw CRSP files (OPTION 1)
│   │   │   ├── CRSP_STOCK_EVENTS_HIST_DESC_INF_FULL_DB.csv
│   │   │   └── CRSP_COMPUSTAT_LINKING_TABLE_FULL_DATABASE.csv
│   │   └── esg_ratings/              # ESG provider data
│   │       ├── refinitiv/
│   │       │   └── Refinitiv_Wharton_FULL_DB.csv
│   │       └── msci/
│   │           ├── ESG Ratings Timeseries 2020.xlsx
│   │           ├── ESG Ratings Timeseries 2021.xlsx
│   │           └── ... (more Excel files)
│   └── processed/                    # Generated files (auto-created)
│       ├── security_master/          # Pre-built security master (OPTION 2)
│       │   └── security_master_segments.csv
│       └── id_mappings/              # Pipeline outputs
└── src/                             # Source code (already created)
```

## 🎯 Required Files

### Security Master (Choose ONE option):

#### **OPTION 1: Raw CRSP Files (Recommended)**
The pipeline will automatically build the security master for you.

📍 **Put these files here:**
```
data/raw/crsp/CRSP_STOCK_EVENTS_HIST_DESC_INF_FULL_DB.csv
data/raw/crsp/CRSP_COMPUSTAT_LINKING_TABLE_FULL_DATABASE.csv
```

**Expected columns in CRSP files:**
- **NAMES file**: `permno`, `namedt`, `nameendt`, `comnam`, `ticker`, `ncusip`, `shrcd`, `exchcd`, etc.
- **LINK file**: `permno`/`lpermno`, `gvkey`, `linkdt`, `linkenddt`, `linkprim`, `linktype`, etc.

#### **OPTION 2: Pre-built Security Master**
If you already have a security master file:

📍 **Put this file here:**
```
data/processed/security_master/security_master_segments.csv
```

**Expected columns:** `permno`, `gvkey`, `namedt`, `nameendt`, `ncusip6`, `ticker`, `is_common`, `exchcd`, etc.

### ESG Data Files (Both Required):

#### **Refinitiv Data**
📍 **Put this file here:**
```
data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv
```

**Expected columns:** `orgpermid`, `year`, `cusip`, `isin`, `ticker`, `comname`, etc.

#### **MSCI Data**  
📍 **Put Excel files here:**
```
data/raw/esg_ratings/msci/ESG Ratings Timeseries 2020.xlsx
data/raw/esg_ratings/msci/ESG Ratings Timeseries 2021.xlsx
data/raw/esg_ratings/msci/ESG Ratings Timeseries 2022.xlsx
... (etc)
```

**Expected columns:** `ISSUERID`, `YEAR`, `ISSUER_CUSIP`, `ISSUER_ISIN`, `ISSUER_TICKER`, `ISSUER_NAME`, etc.

## ⚡ Quick Setup Steps

1. **Create directories** (if they don't exist):
   ```bash
   mkdir -p data/raw/crsp
   mkdir -p data/raw/esg_ratings/refinitiv  
   mkdir -p data/raw/esg_ratings/msci
   mkdir -p data/processed/security_master
   ```

2. **Copy your data files** to the locations shown above

3. **Check everything is ready**:
   ```bash
   python3 check_data_files.py
   ```

4. **Run the pipeline**:
   ```bash
   cd src/data_preparation
   python3 run_mappers.py
   ```

## 🔧 Custom File Paths

If your files have different names or locations, you can customize the paths:

### **For Security Master Builder**
Edit `src/data_preparation/security_master/build_security_master.py`:
```python
class SecurityMasterConfig:
    NAMES_CSV = "./data/raw/crsp/YOUR_NAMES_FILE.csv"         # ← Change this
    LINK_CSV = "./data/raw/crsp/YOUR_LINK_FILE.csv"           # ← Change this
```

### **For ESG Mappers**
Edit `src/data_preparation/run_mappers.py`:
```python
DATA_SOURCES = {
    "refinitiv": {
        "path": "./path/to/your/refinitiv_data.csv",          # ← Change this
        "enabled": True
    },
    "msci": {
        "path": "./path/to/your/msci/directory/",             # ← Change this  
        "enabled": True
    },
}

# Also change security master path if needed:
SECURITY_MASTER = "./path/to/your/security_master.csv"       # ← Change this
```

## 📊 Expected File Formats

### **CRSP Names File** (CSV)
```csv
permno,namedt,nameendt,comnam,ticker,ncusip,shrcd,exchcd,...
10001,1986-01-07,1987-01-30,AMERICAN PETROFINA CO,AMF,123456789,11,1,...
10001,1987-01-30,1999-05-27,FINA INC,FI,123456789,11,1,...
```

### **CRSP Link File** (CSV)  
```csv
gvkey,lpermno,linkdt,linkenddt,linkprim,linktype,...
001001,10001,1986-01-07,1987-01-30,P,LC,...
001001,10001,1987-01-30,1999-05-27,P,LC,...
```

### **Refinitiv Data** (CSV)
```csv
orgpermid,year,cusip,isin,ticker,comname,...
4295905160,2020,037833100,US0378331005,AAPL,APPLE INC,...
4295905160,2021,037833100,US0378331005,AAPL,APPLE INC,...
```

### **MSCI Data** (Excel)
```
ISSUERID    YEAR    ISSUER_CUSIP    ISSUER_ISIN       ISSUER_TICKER    ISSUER_NAME  ...
IID000000012345  2020    037833100    US0378331005    AAPL    APPLE INC    ...
IID000000012345  2021    037833100    US0378331005    AAPL    APPLE INC    ...
```

## 🚨 Common Issues

### **File not found errors**
- Double-check file paths and names match exactly
- Make sure files are in the correct directories
- Run `python3 check_data_files.py` to verify

### **Encoding issues** 
- The pipeline handles common encodings automatically (UTF-8, Latin-1, CP1252)
- If you get encoding errors, convert your files to UTF-8

### **Memory issues**
- Large datasets may require 4-8GB RAM
- Consider filtering to smaller date ranges for testing

### **No matches found**
- Check that your CUSIP/ISIN identifiers are properly formatted
- Verify date ranges overlap between ESG data and security master

## ✅ Verification

After setup, run the check script:
```bash
python3 check_data_files.py
```

You should see:
```
🎉 ALL REQUIRED FILES READY!
   You can run the pipeline:
   cd src/data_preparation  
   python3 run_mappers.py
```

## 📈 Expected Outputs

The pipeline will create these files:
```
data/processed/id_mappings/
├── refinitiv_orgpermid_year_match.csv       # Detailed yearly matches
├── refinitiv_orgpermid_to_gvkey.csv         # Entity-level crosswalk  
├── msci_issuer_id_year_match.csv            # Detailed yearly matches
├── msci_issuer_id_to_gvkey.csv              # Entity-level crosswalk
└── consolidated_esg_to_gvkey.csv            # Combined cross-provider mapping
```

---

**Questions?** Check the detailed documentation in `src/data_preparation/README.md` or examine the example usage in `src/data_preparation/example_usage.py`.
