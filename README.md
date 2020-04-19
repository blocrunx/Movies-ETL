# Movies-ETL
ETL for online movie streaming service
## Project Overview

Write a Python script that will automate the process of extracting, transforming, and loading Wikipedia Movies Data, Kaggle Movies Metadata and MovieLens rating data from Kaggle by doing the following:

- Create a function that takes in three arguments: 
  - Wikipedia data
  - Kaggle metadata
  - MovieLens rating data(from Kaggle)
- Extract data from three sources
  - Read in .json data from Wikipedia
  - Read in .csv from each Kaggle data set
- Clean and transform the data automatically using Pandas and regular expressions.
  - Identify and address data entry formatting anomalies.
  - Identify identify and address duplicate data.
  - Identify and remove columns lacking useful data.
  - Merge data to address missing and inferrior data concerns.
- Load new data into PostgreSQL.
  - delete existing data in tables without dropping the tables themselves.
  
  ## Resources
  
  - Data Source: ratings.csv, moviesmetadata.csv, wikipedia.movies.json
  - Languages: Python, SQL
  - Software: Jupyter Lab 1.2.6, pgAdmin 4.19
  - Database: PostgreSQL 11.7
  
  ## Summary
  
  
  


