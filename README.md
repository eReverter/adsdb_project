# Data Science Pipeline for Clustering Asian Countries Based on Developmental, Geo-Political and Economical Data

This project aims to create a data management and analysis backbone for clustering Asian countries based on various indicators, including economical and governmental data. Three different data sets are used along with various tools, and the project involves four different zones for data management. The final schema is queried, and the preprocess and cluster functions are used for data analysis. While the project increases automation and streamlines processes, there is still room for improvement. Everything is computed through Python and SQL.

The following is a brief summary of each section of the report. For more details, see the [pdf](ADSDB.pdf).

## TOC
- [Project Aim](#project-aim)
- [Data Sources and Tool Choices](#data-sources-and-tool-choices)
- [Data Management Backbone](#data-management-backbone)
  - [Landing Zone](#landing-zone)
  - [Formatted Zone](#formatted-zone)
  - [Trusted Zone](#trusted-zone)
  - [Exploitation Zone](#exploitation-zone)
- [Conclusion](#conclusion)

## Project Aim

The aim of the project is to create a Data Management Backbone and a Data Analysis Backbone for various data sets, with a focus on developing good habits for structuring Data Science projects. The data analysis involves clustering Asian countries based on economical and governmental indicators, but this is not the primary objective of the project.

## Data Sources and Tool Choices

The project uses three data sets for the Data Management backbone: World Governance Indicators, World Bank Data, and United Nations Data Bank. The report also outlines the different tools used for collaboration (Github), data storage (**PgAdmin 4** and **DBeaver**), data transformation (**Python** scripts with packages like `psycopg2`, `sqlalchemy`, `pandas`, and `pandas_profiling`), pipelining (wrapper functions in Python), and data analysis (`scikit-learn` and `plotly` packages in Python).

## Data Management Backbone

Involves four different zones, namely Landing Zone, Formatted Zone, Trusted Zone, and Exploitation Zone. The Landing Zone contains data sources that can be directly downloaded, while the Formatted Zone includes a local database where the source tables are stored, and data quality processes are performed. In the Trusted Zone, different versions of a source are integrated into a single table, while in the Exploitation Zone, the final schema where the data has to be inserted is considered, and data is organized into different data frames for analysis.

### Landing Zone

It retrieves the World Bank source data using the API provided and creates a .csv file containing the chosen indicators for a time span of 20 years. The core of the landing zone consists of a script that moves the files from the temporal zone to the persistent zone using the built-in shutil package while automatically adding a timestamp.

### Formatted Zone

A connection to a local database is initiated, and the tables_to_load function acts as a wrapper that creates and fills different tables in the database. The pandas df.to_sql then bulk loads that table using sqlalchemy tools, where we also specify the method used to load it, called psql_insert_copy. Data quality processes are performed, and the outliers_duplicated_profiling function generates .html files for each table, which contain an extensive profiling report for each column.

### Trusted Zone

Different versions of a source are integrated into a single table using the integrate_source_versions function. It takes as arguments the source to be considered and the url to the database containing the tables to be integrated.

### Exploitation Zone

The final schema is queried for joining by time dimension and country code. The core of the Data Analysis Backbone consists of two important functions: preprocess and cluster. The preprocess function first removes rows that only contain NA’s and imputes the others. The cluster function uses the list of data frames as input and applies a k-means clustering algorithm for which the user specifies the desired amount of clusters.

## Conclusion

In general, the objective of the project was to increase automation and streamline all processes performed by creating a pipeline. However, there is still room for improvement, as the final outcome is not a conclusive solution for all potential challenges that may arise.
