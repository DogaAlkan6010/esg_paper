# ESG Pipeline Setup Guide

This guide shows you exactly where to put your data files so the pipeline doesn't throw a bunch of "file not found" errors at you.

## Directory Structure

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

## What Files You Need

### Security Master (Pick ONE option, not both):

#### **OPTION 1: Raw CRSP Files (Recommended)**
Let the pipeline build the security master automatically. Less work for you.

**Put these files here:**
```
data/raw/crsp/CRSP_STOCK_EVENTS_HIST_DESC_INF_FULL_DB.csv
data/raw/crsp/CRSP_COMPUSTAT_LINKING_TABLE_FULL_DATABASE.csv
```

**Expected columns in CRSP files:**
- **NAMES file**: `permno`, `namedt`, `nameendt`, `comnam`, `ticker`, `ncusip`, `shrcd`, `exchcd`, etc.
- **LINK file**: `permno`/`lpermno`, `gvkey`, `linkdt`, `linkenddt`, `linkprim`, `linktype`, etc.

#### **OPTION 2: Pre-built Security Master**
If you already have a security master file (lucky you):

**Put this file here:**
```
data/processed/security_master/security_master_segments.csv
```

**Expected columns:** `permno`, `gvkey`, `namedt`, `nameendt`, `ncusip6`, `ticker`, `is_common`, `exchcd`, etc.

### ESG Data Files (You need at least one of these):

#### **Refinitiv Data**
**Put this file here:**
```
data/raw/esg_ratings/refinitiv/Refinitiv_Wharton_FULL_DB.csv
```

**Expected columns:** `orgpermid`, `year`, `cusip`, `isin`, `ticker`, `comname`, plus whatever ESG scores you have

#### **MSCI Data**  
**Put Excel files here (one file per year):**
```
data/raw/esg_ratings/msci/ESG Ratings Timeseries 2020.xlsx
data/raw/esg_ratings/msci/ESG Ratings Timeseries 2021.xlsx
data/raw/esg_ratings/msci/ESG Ratings Timeseries 2022.xlsx
... (you get the idea)
```

**Expected columns:** `ISSUERID`, `YEAR`, `ISSUER_CUSIP`, `ISSUER_ISIN`, `ISSUER_TICKER`, `ISSUER_NAME`, plus ESG ratings

#### **FMP Data**  
**Put Parquet file here:**
```
data/raw/esg_ratings/fmp/fmp_esg_panel.parquet
```

**Expected columns:** `symbol`, `periodEndDate`, `isin`, `Environmental`, `Social`, `Governance`, `ESG`, etc.

## Quick Setup Steps

1. **Create directories** (if they don't exist):
   ```bash
   mkdir -p data/raw/crsp
   mkdir -p data/raw/esg_ratings/refinitiv  
   mkdir -p data/raw/esg_ratings/msci
   mkdir -p data/raw/esg_ratings/fmp
   mkdir -p data/processed/security_master
   ```

2. **Copy your data files** to the locations shown above

3. **Check if you did it right**:
   ```bash
   python3 check_data_files.py
   ```

4. **Run the pipeline** (grab some coffee, this takes a while):
   ```bash
   cd src/data_preparation
   python3 run_mappers.py
   ```

## Custom File Paths

If your files have different names or are in different locations (because of course they are), you can customize the paths:

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
    "fmp": {
        "path": "./path/to/your/fmp_data.parquet",            # ← Change this
        "enabled": True
    },
}

# Also change security master path if needed:
SECURITY_MASTER = "./path/to/your/security_master.csv"       # ← Change this
```

## Expected File Formats

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

## When Things Go Wrong

### **File not found errors**
- Double-check that file paths and names match exactly (case-sensitive on Linux/Mac)
- Make sure files are actually in the directories you think they are
- Run `python3 check_data_files.py` to see what's missing

### **Encoding issues** 
- The pipeline tries to handle common encodings automatically (UTF-8, Latin-1, CP1252)
- If you still get encoding errors, open your files in Excel and save as UTF-8 CSV

### **Memory issues**
- Large datasets may need 4-8GB RAM (especially MSCI Excel files)
- Try processing smaller date ranges first to test
- Close other programs to free up memory

### **No matches found**
- Check that your CUSIP/ISIN identifiers aren't mangled (missing leading zeros, etc.)
- Make sure date ranges overlap between your ESG data and security master
- Check that company names look reasonable (not all NaN or weird encoding)

## Check If It Worked

After setup, run the check script:
```bash
python3 check_data_files.py
```

If everything is set up correctly, you should see:
```
🎉 ALL REQUIRED FILES READY!
You can run the pipeline:
cd src/data_preparation  
python3 run_mappers.py
```

If not, it'll tell you what's missing.

## What You'll Get

The pipeline will create these files in `data/processed/id_mappings/`:
```
├── refinitiv_orgpermid_year_match.csv       # Detailed yearly matches
├── refinitiv_orgpermid_to_gvkey.csv         # Entity-level crosswalk  
├── msci_issuer_id_year_match.csv            # Detailed yearly matches
├── msci_issuer_id_to_gvkey.csv              # Entity-level crosswalk
├── fmp_symbol_year_match.csv                # Detailed yearly matches
├── fmp_symbol_to_gvkey.csv                  # Entity-level crosswalk
└── consolidated_esg_to_gvkey.csv            # Combined cross-provider mapping
```

The crosswalk files (`*_to_gvkey.csv`) are probably what you want for most research.

---

**Still confused?** Check the detailed documentation in `src/data_preparation/README.md` or look at the examples in `src/data_preparation/example_usage.py`.
