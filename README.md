# Inventory Forecasting System for Retail Supply Chain Optimization

![Python](https://img.shields.io/badge/python-v3.8-blue)

A robust, machine learning-driven inventory forecasting system designed for retail shopping malls to optimize supply chain management by addressing overstocking and understocking issues. The system leverages a blend of data processing, ML algorithms, and deployment infrastructure to deliver reliable forecasts.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Testing](#testing)
- [Results](#results)
- [Future Enhancements](#future-enhancements)

## Project Overview

The **Inventory Forecasting System** is built to predict stock levels with a Mean Squared Error (MSE) of 26.7%, effectively minimizing overstocking and understocking risks. It incorporates:

- **Automated ETL Pipelines** to handle raw data from multiple sources.
- **Machine Learning Models** including Prophet, XGBoost, and Random Forest.
- **Hyperparameter Optimization** with Optuna.
- **Model Tracking** with MLflow.
- **Deployment on AWS** (EC2, RDS, S3).
- **Visualization** through an interactive Looker Studio dashboard on Google Cloud.

## Architecture

1. **Data Collection**: Multi-source data ingestion.
2. **ETL Pipeline**: Automated data processing.
3. **Forecasting Models**: Models like Prophet, XGBoost, and Random Forest for inventory forecasting.
4. **Hyperparameter Tuning**: Optuna for tuning parameters.
5. **Model Tracking**: MLflow for tracking iterations and metrics.
6. **Deployment**: Scalable deployment using AWS.
7. **Visualization**: Real-time insights using Looker Studio (Google Cloud Platform).

## Technologies

- **Languages**: Python, SQL
- **ML Models**: Prophet, XGBoost, Random Forest
- **Data Management**: MySQL Workbench, AWS RDS, S3, Google Sheets
- **ETL Pipeline**: Python, pandas
- **Deployment**: AWS (EC2, RDS, S3), Docker, Flask API
- **CI/CD**: Jenkins, GitHub Actions, Bash
- **Visualization**: Google Cloud Platform (Looker Studio)
- **Hyperparameter Tuning**: Optuna
- **Model Tracking**: MLflow
- **Testing**: Pytest, Postman

## Setup and Installation

### Prerequisites

1. **Python 3.8** or higher.
2. **Docker**.
3. **AWS account** for EC2, S3, and RDS.
4. **MySQL Workbench** for managing SQL queries.

### Installation

1. **Clone the GitHub repository**:
```bash
git clone -b automation https://github.com/Aditya-NeuralNetNinja/d2p_proj.git
cd d2p_proj
```

2. **Create a Python virtual environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4. **Set up environment variables**:

   Configure the following environment variables for AWS, MySQL, and Google Cloud credentials. Replace placeholder values with your actual credentials.

   ```bash
   # MySQL Database Configuration
   export MYSQL_SERVER_USER="your_mysql_user"
   export MySQL_Server_Password="your_mysql_password"
   export DB_HOST="your_db_host"

   # AWS Configuration
   export AWS_ACCESS_KEY_ID="your_aws_access_key"
   export AWS_SECRET_ACCESS_KEY="your_aws_secret_key"

   # Google Cloud Configuration
   export PRIVATE_KEY_ID="your_private_key_id"
   export PRIVATE_KEY="your_private_key"
   export CLIENT_EMAIL="your_client_email"
   export CLIENT_ID="your_client_id"
   export CLIENT_X509_CERT_URL="your_client_x509_cert_url"
   ```

5. **Run the bash script to automate the ETL pipeline and model training process**:
    ```bash
    bash config/run_pipeline.sh
    ```