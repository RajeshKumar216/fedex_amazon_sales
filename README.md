# FedEx Amazon Sales Data Repository

This repository contains all the necessary files and scripts to clean, process, and model the FedEx Amazon sales data. The workflow is organized into two main parts:

  *  Data Cleaning
  * DBT Modeling for Analytics

### 1. Data Cleaning

The data_cleaning folder includes a dataset in Amazon Sale Report.csv format and a Jupyter Notebook Data_sanity.ipynb file containing the code for data cleaning. The cleaning steps performed are:
* File_path: fedex_amazon_sales/data_cleaning/Data_sanity.ipynb

    Removing Duplicates: Eliminate duplicate records from the dataset to ensure data integrity.
    Removing Duplicate Order IDs: Specifically target and remove duplicate entries of Order ID to avoid redundant sales records.
    Stripping Data: Clean all column data by stripping leading and trailing whitespace to maintain consistency.
    Classified_city: Created classified_city field to store major citied extracted from ship_city.
    Typecasting Fields: Convert fields to their correct data types (e.g., dates, numeric fields) to facilitate proper data handling and analysis.
    Cleaning ship_state Values: Standardize the ship_state field to correct state codes for consistency.
    Renaming Columns: Update all column names to a standardized format for better readability and usability.
    Exporting Cleaned Data: The cleaned data is exported and stored in a database for further analysis.

### 2. DBT Modeling for Analytics

The dbt_model folder contains the amazon_sales_data project, which includes all necessary DBT models to transform the cleaned data into a star schema, optimizing it for analytical purposes. The key components are:

    Data Loading: Load cleaned data from the database into DBT models.
    Data Processing: Apply various filterings to structure the data in a meaningful way.
    Star Schema Creation: Organize the data into a star schema that includes:
        Dimension Tables: Contain descriptive attributes related to sales, such as dim_product, dim_location,dim_order_status,dim_date,dim_promotion, dim_sales_channel and more.
        Fact Tables: fact_sales Store quantitative data, that are linked to dimension tables for comprehensive analytics.
    Data Insertion: Insert the processed data into the appropriate tables in the database, ensuring data is loaded in the correct order to maintain referential integrity.

Usage

    Data Cleaning: Navigate to the data_cleaning folder, open the Data_sanity.ipynb file in Jupyter Notebook, and run the cells to clean your data.
    DBT Models: Navigate to the dbt_model/amazon_sales_data folder, configure your DBT profile, and run the DBT commands(dbt run) to load and transform the data according to your analytics needs.