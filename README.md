# Insurance Data Analysis Pipeline

This project demonstrates a simple data engineering pipeline that processes an insurance dataset and generates analytical insights using Python, SQLite, and SQL.

## Project Architecture

CSV Dataset → Python Processing → SQLite Database → SQL Views → Aggregated CSV Outputs

## Features

- Data cleaning and normalization
- Loading CSV data into SQLite database
- SQL views for analytical queries
- Automated pipeline execution
- Generation of aggregated datasets

## Technologies Used

Python  
Pandas  
SQLite  
SQL  

## Running the Pipeline

Step 1: Install dependencies

pip install pandas

Step 2: Run pipeline

python src/run_pipeline.py

## Output

The pipeline generates analysis datasets inside the `outputs/` folder:

- Average charges by region
- Smoker vs non-smoker analysis
- Age group analysis
- BMI category analysis
