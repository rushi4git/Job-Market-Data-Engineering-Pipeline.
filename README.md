# 🧠 Job Market Data Engineering Pipeline (AWS + Python)

This project builds a serverless data pipeline on AWS to analyze real-time job market data using two public job APIs: **Adzuna** and **Arbeitnow**. It automates ingestion, transformation, and querying using AWS services like **S3, Lambda, Glue, and Athena**.

---

## 🚀 Project Overview

- **APIs Used:**  
  - [Adzuna](https://developer.adzuna.com/)  
  - [Arbeitnow](https://documenter.getpostman.com/view/18545278/2s93z9dZgD)

## 🚀 Tech Stack

- **Python** (ETL logic)
- **APIs**: Adzuna, Arbeitnow
- **AWS Services**:
  - AWS Lambda (ETL execution)
  - Amazon S3 (storage)
  - AWS Glue (catalog)
  - AWS Athena (SQL querying)
  - Amazon CloudWatch (monitoring)

## 🚀 Architecture Overview

1. **Data Ingestion Lambda**
   - Calls **Adzuna** and **Arbeitnow** APIs
   - Combines and stores raw job data to `S3/job_data/` as `.json`

2. **S3 Trigger**
   - Triggers transformation Lambda upon new `.json` in `job_data/`

3. **Transformation Lambda**
   - Applies multiple transformations:
     - Company unification
     - Job title frequency
     - Daily & monthly job trends
     - Top hiring companies
     - Location analysis
     - Contract type distribution
   - Saves CSVs in S3 under structured folders:
     - `processed/cleaned_jobs/`
     - `processed/job_titles/`
     - `processed/daily_postings/`
     - `processed/monthly_postings/`
     - `processed/top_companies/`
     - `processed/location_analysis/`
     - `processed/contract_types/`

4. **AWS Glue Crawler**
   - Crawls S3 `processed/` folder
   - Creates tables in Glue Data Catalog

5. **Athena**
   - Queries data for trend analysis, reports, or dashboards
   - 
![Architecture Diagram.](https://github.com/rushi4git/spotify-end-to-end-data-engineering-project/blob/main/architecture_diagram_spotify.jpg)
---

## 📂 Folder Structure in S3
s3://job-market-destination-bucket-rushi/
│
└── processed/
    ├── cleaned_job_data/
    ├── job_title_counts/
    ├── daily_postings/
    ├── monthly_postings/
    ├── top_companies/
    ├── location_analysis/
    └── contract_type_distribution/

---

## 🔁 Transformations Performed

1. ✅ Flatten & unify job data from both APIs  
2. ✅ Job title frequency  
3. ✅ Jobs posted per day  
4. ✅ Jobs posted per month  
5. ✅ Top hiring companies  
6. ✅ Location analysis  
7. ✅ Contract type distribution

Each transformation result is stored as a separate CSV in `processed/`.

---

## 🧪 AWS Setup

### ✅ Lambda Function

- Triggered via S3 event on upload to `job_data/` with `.json` suffix
- Performs all transformations and writes processed CSVs to `processed/` subfolders

### ✅ Glue Crawler

- Points to the `processed/` prefix
- Creates separate tables in a single database (one per transformation output)

### ✅ Athena

- Query the output tables for analytics and visualization

---

## 🛠️ How to Run
1.Clone this repo
2.Set up your AWS credentials
3.Deploy the Lambda using AWS Console or SAM/CDK
4.Upload raw .json files into the job_data/ folder in S3
5.Trigger fires → data processed → Glue crawler updates → Athena ready

📌 Future Improvements
1.Add data quality checks
2.Integrate with BI tools like QuickSight

🧑‍💻 Author
Rushikesh Rajulwar
Data Engineer | Python & AWS | [LinkedIn](https://www.linkedin.com/in/rushikesh-rajulwar/).

