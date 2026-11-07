# 🏦  World Bank Data Ingestion with Airflow & GCP
&nbsp;&nbsp;&nbsp;This end-to-end Ingestion pipeline using Airflow & GCP , the Goal is to Set up an Airflow DAG on GCP that retrieves economic and social indicators from the World Bank’s public API, stores the raw data in Cloud Storage, and then loads it into BigQuery for analysis.
&nbsp;
&nbsp;

##  Tools and Platforms :

![GCP](https://img.shields.io/badge/GCP-4285F4?style=flat&logo=googlecloud&logoColor=white)
![Compute Engine](https://img.shields.io/badge/Compute%20Engine-4285F4?style=flat&logo=googlecloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=googlebigquery&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=flat&logo=gnubash&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=flat&logo=linux&logoColor=black)
![ELT Pipeline](https://img.shields.io/badge/ELT-Pipeline-2E8B57?style=flat)
![CSV Processing](https://img.shields.io/badge/CSV-Processing-00897B?style=flat)
&nbsp;
## Architecture Overview
This project models a simple ELT architecture workflow, Here we focus more about Ingestion in addition of Data Quality checks along with Airrflow:
<>
&nbsp;
```Text
World Bank API 
      │ (HTTP GET)
      ▼
Airflow Task: extract_and_upload
      │ (Local CSV -> GCS Upload)
      ▼
Cloud Storage: gs://<project>-worldbank-raw/worldbank/<ds>/indicators.csv
      │ (GCSToBigQueryOperator)
      ▼
BigQuery Table: raw_worldbank.indicators (Partitioned & Clustered)
      │
      ▼
Airflow Task: check_data_quality (BigQueryCheckOperator)
```

## 🏗️ GCP Infrastructure Setup

Enable Required APIs

```Bash

export PROJECT_ID=$(gcloud config get-value project)
export REGION="europe-west9"

gcloud services enable composer.googleapis.com \
    storage.googleapis.com \
    bigquery.googleapis.com

```
Create Storage Bucket and BigQuery Dataset

```Bash

# Bucket for raw ingestion
gsutil mb -p ${PROJECT_ID} -l ${REGION} gs://${PROJECT_ID}-worldbank-raw

# BigQuery dataset
bq mk --project_id=${PROJECT_ID} --location=${REGION} raw_worldbank

```
Create Cloud Composer 3 Environment

```Bash

gcloud composer environments create wb-bootcamp-env \
    --location ${REGION} \
    --image-version composer-3-airflow-2.11.1-build.5

```

Configure Minimal IAM Permissions


```Bash
SA_EMAIL="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
```

Grant Minimal BigQuery permissions:

```Bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.jobUser"

bq add-iam-policy-binding \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.dataEditor" \
    ${PROJECT_ID}:raw_worldbank
```


Grant Minimal Storage permissions:

```Bash
gsutil iam ch serviceAccount:${SA_EMAIL}:objectAdmin gs://${PROJECT_ID}-worldbank-raw
```


## ▶️ Deploying and Running the DAG

Find Composer's DAG bucket:

```Bash
gcloud composer environments describe airflow-en \
    --location europe-west9 \
    --format="get(config.dagGcsPrefix)"
```
Deploy DAG to Composer:

```Bash
gsutil cp worldbank_gdp_ingestion.py <COMPOSER_DAG_BUCKET>/

```

## 📋 Deliverables Checklist & Bonus Answers :

In the following we will include screenshots that will illustrate Airflow Composer Up & Running in a Healthy status.&nbsp;

&nbsp;
<img width="1901" height="330" alt="1  Airflow Up   Running" src="https://github.com/user-attachments/assets/ad66ca5e-0f2d-4a1e-9d7b-d6a829000024" />

&nbsp;
Airflow Ingestion Dag Graph showcase the different ingestion layersr from extraction, bigquery loading , and data quality checks with one run succeeded.

&nbsp;
<img width="1865" height="855" alt="2  Airflow Ingestion Dag" src="https://github.com/user-attachments/assets/9d715eba-8ff2-4984-8080-e079f6431322" />

&nbsp;
As result we can see now our data successfully ingested into BigQuery Warehouse and we can run the following SQL script:

```SQL
SELECT * FROM `raw_worldbank.indicators` LIMIT 20;
```

&nbsp;
<img width="1891" height="765" alt="6  BigQuery Select Query" src="https://github.com/user-attachments/assets/4ce89b14-f07f-4efc-956a-c047b41abddb" />

## 🧑‍💻 About Me
&nbsp;   Hey , My name is Mohammed Herradi Welcome to my Github I’m a Data Engineering Enthusiast with a strong interest in building reliable, scalable, and production-ready data solutions. My anthem in life is "Always Learning" . Today, my main focus is Data Engineering, particularly the design and development of end to end data pipelines delivering data products. Always learning. Always building. Always looking for better ways to work with data .

💼 Let's Connect on LinkedIn: www.linkedin.com/in/mohammed-herradi


