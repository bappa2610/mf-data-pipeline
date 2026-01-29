# Mutual Fund NAV Data Pipeline

A fast, reliable, and API-friendly Python pipeline to **collect, update, and organize Indian Mutual Fund NAV data**.

This project fetches NAV data using public APIs, stores **scheme-wise NAV history**, and generates **year-wise consolidated NAV files** for analytics, dashboards, and portfolio tracking apps.

---

## ✨ Features

- 📥 Fetch NAV data for **all mutual fund schemes**
- ⚡ Ultra-fast incremental updates (only new NAVs)
- 🗂 Scheme-wise NAV history storage
- 📅 Year-wise consolidated NAV files
- 🔁 Safe re-runs (no duplicate entries)
- 🚀 Parallel API requests (configurable workers)
- 🛡 API-friendly rate limiting
- 📊 Ready for analytics, AppSheet, Excel, Power BI

---

## 📁 Project Structure

mf-nav-data-pipeline/
├── data/
│ ├── scheme_codes.csv # List of SchemeCode & metadata
│ ├── nav_history/ # Scheme-wise NAV CSVs
│ ├── nav_year/ # Year-wise consolidated NAV files
│
├── scripts/
│ ├── fetch_nav_history.py # Download & update NAV history
│ ├── build_nav_year.py # Build year-wise NAV files
│
├── README.md
├── requirements.txt
└── .gitignore

---

## 📊 Data Formats

### 1️⃣ Scheme-wise NAV History  
Stored in `data/nav_history/<SchemeCode>.csv`

Date,NAV
2014-06-12,10.8737
2014-06-13,10.8921


---

### 2️⃣ Year-wise NAV Files  
Stored in `data/nav_year/nav_year_YYYY.csv`

SchemeCode,Date,NAV
123184,2017-10-17,14.2022
123186,2017-10-17,14.1577


✔ Sorted  
✔ No duplicates  
✔ Incremental updates  

---

## ⚙️ Configuration

Edit values directly in scripts:

```python
MAX_WORKERS = 8        # Parallel API requests
REQUEST_DELAY = 0.12  # API-friendly delay
CONNECT_TIMEOUT = 2
READ_TIMEOUT = 5


Recommended limits

Workers: 5–8

Avoid more than 10 to prevent API blocking