# 📊 Monday.com Data Auditor

A streamlined, local web application built to audit Monday.com workspaces. This tool connects directly to the Monday.com GraphQL API to scan multiple boards simultaneously, pinpointing missing data (like empty "Files" or "Notes" cells), and generating clean, actionable reports. 

Instead of manually exporting dozens of Excel sheets to find missing information, this auditor pulls the data into memory using Python and Pandas, allowing for instant filtering and export.

## ✨ Features
* **Multi-Board Scanning:** Select and scan multiple Monday.com boards at once via a dynamic dropdown.
* **Deep Pagination:** Utilizes cursor-based pagination to ensure no rows are missed, even on massive boards with thousands of items.
* **Smart Filtering:** Instantly filter the audit results to show only specific missing columns (e.g., isolate tasks that are only missing "Files").
* **Clickable Task Links:** Generates direct URLs to the exact Monday.com task that needs updating.
* **CSV Export:** Download the filtered audit report as a clean CSV file.
* **Secure & Local:** Runs entirely on your local machine. No third-party automation subscriptions (like Zapier or Make) required.

## 🛠️ Tech Stack
* **Language:** Python 3
* **Frontend/UI:** Streamlit
* **Data Manipulation:** Pandas
* **API Integration:** Requests, Monday.com GraphQL API (v2023-10)

## 🚀 How to Install and Run

### Prerequisites
You will need [Python](https://www.python.org/downloads/) installed on your machine and a valid Monday.com API Key (available in your Monday.com Developer/Admin settings).

### 1. Clone the Repository
```bash
git clone [https://github.com/Thanuka9/Monday.git](https://github.com/Thanuka9/Monday.git)
cd Monday
