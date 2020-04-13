# Event Logs Data Pipeline using Airflow
> 
This project makes use of Apache Airflow to automate and monitor Sparkify's data warehouse pipelines. Airflow's Directed Acyclic Graph (DAG) is used to implement a data pipeline responsible for reading all Sparkify's event logs, processes and create fact and dimensions tables on Amazon RedShift.

* A high-level implementation of the pipeline is as shown below:
![ERD image](/high-level-dag.PNG)
* The load_dimensions_subdag is a subdag that uses an operator to load data into dimension tables, and it's implementation is as shown in the figure below:
![ERD image](/sub-dag.PNG)

## Table of contents

* [Data and Code](#data-and-code)
* [Prerequisites](#prerequisites)
* [Instructions on running the application](#instructions-on-running-the-application)

## Data and Code
The dataset for the project resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in Sparkify app.

In addition to the data files, the project workspace includes:
* **create_tables.py** - contains sql statements for creating database tables.
* **dags folder** - contains twor files:
    * sparkify_etl_pipeline_dag - contains code for building the Airflow DAG.
    * subdag_factory - contains a factory method for creating tasks for the load_dimensiongs_subdag.
* **plugins folder** - this is a repository for operators that stage the data, transform the data, and run checks on data quality. The folder also contains a helper class for INSERT sql statements.

## Prerequisites
* AWS RedShift cluster
* Apache Airflow
* psycopg2
python 3 is needed to run the python scripts.

## Database structure
![ERD image](/songplays_erd.png)
## Instructions on running the application
* You must have an access to an AWS RedShift cluster
* Use the sql queries in create_tables.py to create database tables.
* Ensure that Airflow is installed and running before copying the code into the airflow directory
