# Movies-ETL
ETL for online movie streaming service
## Project Overview

Write a Python script that will automate the process of extracting, transforming, and loading Wikipedia movies data, Kaggle movie metadata and MovieLens rating data by doing the following:

- Create a function that takes in three arguments: 
  - Wikipedia data
  - Kaggle metadata
  - MovieLens rating data(from Kaggle)
- Extract data from three sources
  - Read in .json data from Wikipedia
  - Read in .csv from each Kaggle data set
- Clean and transform the data automatically using Pandas and regular expressions.
  - Identify and address data entry formatting anomalies.
  - Identify and address duplicate data.
  - Identify and remove columns lacking useful data.
  - Merge data to address missing and inferior data concerns.
- Load new data into PostgreSQL.
- Delete existing data in tables without dropping the tables themselves.
  
  ## Resources
  
  - Data Source: ratings.csv, moviesmetadata.csv, wikipedia.movies.json
  - Languages: Python 3.7.6, SQL
  - Software: Jupyter Lab 1.2.6, pgAdmin 4.19
  - Database: PostgreSQL 11.7
  
  ## Summary
  
  To automate the ETL pipeline some assumptions need to be made:
  
  - Assumption one:
    - Users will have data files stored in a 'Resources' folder which will exist in the same directory as the challenge.py script. 
  
  - Assumption two: 
    - Users will import dependencies. 
    
  - Assumption three:
    - Length of wikipedia.movies.json original file remains unchanged as there is a condition that checks the length of the file to ensure json data was loaded without errors. If the file changes this condition must also change. 
  
  - Assumption four:
    - Loss of a small portion of data in the data cleaning process is acceptable as a trade-off for time and efficiency. 
  
  - Assumption five: 
    - Users have access to a postgreSQL database and have created a table named "movie_data".
    
  - Assumption six:  
    - Users will change variable "db_string" to hold the url of their own database.
    
  - Assumption seven: 
    - Users will have enough RAM operate at a chunksize of 1000000 rows.
  
 
  
  


