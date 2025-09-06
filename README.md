🍏 Apple Data Analysis – PySpark ETL Project
Welcome to the Apple Data Analysis project, a complete end-to-end ETL pipeline built using PySpark on Databricks. This project demonstrates scalable data engineering practices, including modular design patterns, multi-source ingestion, transformation logic, and data loading into various destinations.

Inspired by hands-on tutorials from YouTube, this project is ideal for learners and professionals preparing for PySpark interviews or building real-world data pipelines.

📌 Project Overview
This repository showcases:

Modular ETL pipeline architecture using PySpark

Implementation of Factory Design Pattern for flexible data ingestion and loading

Business transformation logic using PySpark DataFrame API and Spark SQL

Writing data to multiple destinations including DBFS, S3, and Delta Lake

🧱 Project Architecture
Refer to Project_Architecture.md for a detailed breakdown of the system design and file responsibilities.

📁 File Structure & Roles
File Name	Description
apple_analysis.py	Main ETL driver script. Orchestrates the entire pipeline by initiating extractor, transformer, and loader modules. Ideal for scheduling and automation.
reader_factory.py	Implements the Factory Pattern to read data based on file type (CSV, Parquet, Delta). Provides modularity and scalability for ingestion.
extractor.py	Calls reader_factory methods to extract data from various sources. Handles logic for dynamic file type execution.
transform.py	Contains business transformation logic:<br>• Customers who bought AirPods just after buying iPhones<br>• Customers who bought both AirPods and iPhones
loader_factory.py	Factory class for loading data into different destinations (DBFS, S3, Delta Lake). Mirrors the ingestion pattern for consistency.
loader.py	Uses loader_factory to load transformed data into target systems. Supports multiple output formats and destinations.
🛠️ Technologies Used
Apache Spark (PySpark) – Distributed data processing

Databricks – Cloud-based Spark platform

Python – Scripting and orchestration

Delta Lake – Storage format for Lakehouse architecture

DBFS / S3 – Data storage destinations
