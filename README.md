# AMFI Mutual Fund NAV Data

A fast, reliable, and API-friendly Python pipeline to **collect, update, and organize Indian Mutual Fund NAV data** from the [MFAPI](https://api.mfapi.in/) (Association of Mutual Funds in India).

This project fetches NAV data using public APIs, stores **scheme-wise NAV history**, and generates **year-wise consolidated NAV files** for analytics, dashboards, and portfolio tracking apps.

---

## ✨ Features

- 📥 Fetch NAV data for **all mutual fund schemes** from MFAPI
- ⚡ Ultra-fast incremental updates (only new NAVs)
- 🗂 Scheme-wise NAV history storage
- 📅 Year-wise consolidated NAV files
- 🔁 Safe re-runs (no duplicate entries)
- 🚀 Parallel API requests (configurable workers)
- 🛡 API-friendly rate limiting
- 📊 Ready for analytics, AppSheet, Excel, Power BI
- ⚙️ Automated updates via GitHub Actions (runs every 2 hours)
- ✅ Data quality with pytest testing and type checking

---

## 📁 Project Structure

```
amfi-mf-nav-data/
├── .github/workflows/          # GitHub Actions automation
│   ├── update_mfapi_data.yml   # MFAPI data updates
│   ├── process_nav_history.yml # NAV history processing
│   └── process_mf_master_data.yml # Master data generation
├── data/                       # Data storage
│   └── NAV/
│       ├── Latest_NAV/         # Latest NAV data
│       ├── nav_history/        # Scheme-wise NAV history (CSV)
│       └── nav_year/           # Year-wise consolidated NAV files
├── scripts/                    # Main scripts
│   ├── generate_master_scheme_data.py   # Master data pipeline
│   ├── run_mfapi_data_update.py         # MFAPI data update
│   ├── run_nav_history_update.py        # NAV history update
│   ├── Latest_nav/              # Fetch latest NAV
│   ├── RAW_data_fetcher/       # Raw data fetching
│   ├── Master_data/            # Master data processing
│   ├── MF_data_analysis/       # Data analysis & filtering
│   └── Export_NAV_history/     # NAV history export
├── Test/                       # Test files
├── requirements.txt           # Python dependencies
├── mypy.ini                    # Type checking config
└── README.md                   # This file
```

---

## 🛠️ Installation

1. **Clone the repository:**
   
```
bash
   git clone https://github.com/yourusername/amfi-mf-nav-data.git
   cd amfi-mf-nav-data
   
```

2. **Create a virtual environment (optional but recommended):**
   
```
bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
```

3. **Install dependencies:**
   
```
bash
   pip install -r requirements.txt
   
```

### Dependencies

- **requests** - HTTP library for API calls
- **tqdm** - Progress bar for loops
- **pandas** - Data manipulation and CSV handling
- **mypy** - Type checking
- **pytest** - Testing framework
- **pytest-cov** - Code coverage
- **flake8** - Code linting

---

## 🚀 Usage

### Option 1: Automated Updates (GitHub Actions)

The project includes GitHub Actions workflows that automatically update data every 2 hours:

1. **Fork this repository**
2. **Add a PAT_TOKEN secret** (for push permissions)
3. **Workflows will run automatically** on schedule

Or trigger manually via GitHub UI: **Actions → Workflow → Run workflow**

### Option 2: Local Execution

Run individual scripts as needed:

```
bash
# Generate master scheme data (run first)
python scripts/generate_master_scheme_data.py

# Update MFAPI data
python scripts/run_mfapi_data_update.py

# Update NAV history
python scripts/run_nav_history_update.py

# Fetch latest NAV for all schemes
python scripts/Latest_nav/fetch_latest_nav.py
```

---

## ⚙️ Configuration

Edit values directly in scripts:

```
python
# In scripts/Latest_nav/fetch_latest_nav.py
max_workers = 5    # Parallel API requests
max_retries = 5   # Retry attempts

# In other scripts
MAX_WORKERS = 8        # Parallel API requests
REQUEST_DELAY = 0.12   # API-friendly delay
CONNECT_TIMEOUT = 2
READ_TIMEOUT = 5
```

**Recommended limits:**
- Workers: 5–8
- Avoid more than 10 to prevent API blocking

---

## 📊 Data Formats

### 1️⃣ Scheme-wise NAV History  
Stored in `data/NAV/nav_history/<SchemeCode>.csv`

```
csv
Date,NAV
2014-06-12,10.8737
2014-06-13,10.8921
...
```

### 2️⃣ Year-wise NAV Files  
Stored in `data/NAV/nav_year/nav_year_YYYY.csv`

```
csv
SchemeCode,Date,NAV
123184,2017-10-17,14.2022
123186,2017-10-17,14.1577
...
```

✅ Sorted  
✅ No duplicates  
✅ Incremental updates  

### 3️⃣ Latest NAV  
Stored in `data/NAV/Latest_NAV/latest_nav.csv`

---

## 🔄 GitHub Actions Automation

| Workflow | Schedule | Purpose |
|----------|----------|---------|
| `update_mfapi_data.yml` | Every 2 hours | Fetches all mutual fund data |
| `process_nav_history.yml` | Every 2 hours | Updates NAV history files |
| `process_mf_master_data.yml` | Every 2 hours | Generates master scheme data |

All workflows can also be triggered manually via GitHub UI.

---

## 🧪 Testing

Run tests with pytest:

```
bash
pytest Test/ -v
```

Run with coverage:

```
bash
pytest --cov=. Test/
```

Type checking:

```
bash
mypy .
```

Code linting:

```bash
flake8 .
```

---

## 🙏 Acknowledgments

- [MFAPI](https://api.mfapi.in/) - Official AMFI API for Mutual Fund NAV data
- [Association of Mutual Funds in India (AMFI)](https://www.amfiindia.com/)
