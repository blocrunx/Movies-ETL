#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from config import db_password
import json
import re
import time
import psycopg2


# In[ ]:


# ETL pipeline for wikipedia.movies.json, kaggle_metadat.csv, and ratings.csv

def etl_wiki_kaggle_ratings(wikipedia_data, kaggle_data, movielens_ratings):
    
# Function to get json data and check to make sure there were no problems importing

    try:
        # Import wikipedia json data
        with open (f'Resources/{wikipedia_data}', mode='r')as file:
            wiki_movies_raw = json.load(file)
        print(f"Successfully imported {wikipedia_data} file.")
    
    except Exception as e: print(e)
    
    # Confirm wiki file imported correctly then do some house cleaning on json file.
    try:
        if len(wiki_movies_raw) == 7311:
            wiki_movies = [movie for movie in wiki_movies_raw
            if ('Director' in movie or 'Directed by' in movie)
            and 'imdb_link' in movie
            and 'No. of episodes' not in movie]
            print(f"Wikipedia movies json data matches specified requirements.")

        else:
            print("Error importing json data. Please try again.")
    
    except Exception as e: print(e)
    
    # Import kaggle data
    try:
        file_path_kaggle_metadata = os.path.join('Resources', kaggle_data)
        kaggle_metadata = pd.read_csv(file_path_kaggle_metadata, low_memory=False)
        print(f"Successfully imported Kaggle data.")
       
    except Exception as e: print(e)
        
    # Import ratings data
    try:
        file_path_ratings = os.path.join('Resources', movielens_ratings)
        ratings = pd.read_csv(file_path_ratings, low_memory=False)
        print(f"Successfully imported Ratings data.")
        
    except Exception as e: print(e)
    
    # Clean movie data
    def clean_movie(movie):
        
        try:
            #remember movie is local and will not affect the global movie variable
            movie = dict(movie)
            alt_titles = {}
            for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                        'Hangul','Hebrew','Hepburn','Japanese','Literally',
                        'Mandarin','McCune–Reischauer','Original title','Polish',
                        'Revised Romanization','Romanized','Russian',
                        'Simplified','Traditional','Yiddish']:
                
                if key in movie:
                    alt_titles[key] = movie[key]
                    movie.pop(key)
            
            if len(alt_titles) > 0:
                movie['alt_titles'] = alt_titles

             # merge column names
            def change_column_name(old_name, new_name):
                if old_name in movie:
                    movie[new_name] = movie.pop(old_name)
            
            change_column_name('Adaptation by', 'Writer(s)')
            change_column_name('Country of origin', 'Country')
            change_column_name('Directed by', 'Director')
            change_column_name('Distributed by', 'Distributor')
            change_column_name('Edited by', 'Editor(s)')
            change_column_name('Length', 'Running time')
            change_column_name('Original release', 'Release date')
            change_column_name('Music by', 'Composer(s)')
            change_column_name('Produced by', 'Producer(s)')
            change_column_name('Producer', 'Producer(s)')
            change_column_name('Productioncompanies ', 'Production company(s)')
            change_column_name('Productioncompany ', 'Production company(s)')
            change_column_name('Released', 'Release Date')
            change_column_name('Release Date', 'Release date')
            change_column_name('Screen story by', 'Writer(s)')
            change_column_name('Screenplay by', 'Writer(s)')
            change_column_name('Story by', 'Writer(s)')
            change_column_name('Theme music composer', 'Composer(s)')
            change_column_name('Written by', 'Writer(s)')            
            
            return(movie)
        
        except Exception as e: print(e)
            
    try:        
        # Create pandas DataFrame
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        wiki_movies_df = pd.DataFrame(clean_movies)
        
        # Let the user know what's happening
        print("Successfully created wiki_movies_df DataFrame.")
        
    except Exception as e: print(e)
    
    try:
        # Drop duplicate IMDB ID's
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')

        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)


        # Remove mostly null columns
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
        

        # Clean box office column using regex
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

        # Drop na
        box_office = wiki_movies_df['Box office'].dropna()

        # Convert lists to strings
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Find values given as range
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    except Exception as e: print(e)
    
    def parse_dollars(s):
        try:
            # if s is not a string, return NaN
            if type(s) != str:
                return np.nan

            # if input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

                # remove dollar sign and " million"
                s = re.sub('\$|\s|[a-zA-Z]','', s)

                # convert to float and multiply by a million
                value = float(s) * 10**6

                # return value
                return value

            # if input is of the form $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

                # remove dollar sign and " billion"
                s = re.sub('\$|\s|[a-zA-Z]','', s)

                # convert to float and multiply by a billion
                value = float(s) * 10**9

                # return value
                return value

            # if input is of the form $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

                # remove dollar sign and commas
                s = re.sub('\$|,','', s)

                # convert to float
                value = float(s)

                # return value
                return value

            # otherwise, return NaN
            else:
                return np.nan
        
        except Exception as e: print(e)
            
    try:        
        # Change formatting for values in box office to new column 'box_0ffice' and drop old column 'Box office'
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        wiki_movies_df.drop('Box office', axis=1, inplace=True)
        #print(wiki_movies_df.columns.tolist())
    
    except Exception as e: print(e)
    
    # Clean wiki budget column
    try:
        # Drop na
        budget = wiki_movies_df['Budget'].dropna()
       
        # Convert any lists to strings
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        
        # Remove any values between dollar sign and hyphen
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        
        # Remove citation references
        budget = budget.str.replace(r'\[\d+\]\s*', '')
        
        # Assign values extrated with regex to new column
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        
        # Drop the original budget column
        wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    except Exception as e: print(e)
    
    # Clean release date data:
    try:
        # Drop na values in release date column
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Apply regex forms
        # Release date regex forms
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'
        
        # Use forms and change datatype to datetime 
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    except Exception as e: print(e)
        
    try:
        # Clean Running times data:
        # Drop na
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        
        # Apply regex
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        
        # Convert empty strings to nan with 'coerce' then to 0 with fillna(0)
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        
        #function that will convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        
        # Drop Running time column
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    except Exception as e: print(e)
    
    print("Successfully cleaned wiki_movies_df DataFrame.")
    
    ### Kaggle Data ###
    
    try:
        # Drop adult column from hackathon data
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
        print(f"Successfully dropped Kaggle 'adult' column.")
    
    except Exception as e: print(e)
        
    try:
        # create a column with boolean masks
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
        
        # Change columns data type
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
        print(f"Successfully cleaned Kaggle data.")
    
    except Exception as e: print(e)
    
    ### Ratings Data ###
    
    # Convert ratings timestamp to datetime
    try:
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    
    except Exception as e: print(e)
    
    ### Merge wiki_movies_df and kaggle_metadata ###
    
    try:
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
        print(f"Successfully merged wiki_movies_df and kaggle_metadata.")
    
    except Exception as e: print(e)
        
    try:
        # Drop lofi dupplicate columns
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
        
        #  Fill missing kaggle data with wiki data
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
            df.drop(columns=wiki_column, inplace=True)
            
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
        print(f"Successfully filled missing Kaggle data with wiki data.")
        
    except Exception as e: print(e)
    
    try:
        # Reorder columns
        movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]
        print(f"Successfully reordered columns in movies_df.")
    
    except Exception as e: print(e)
    
    try:
        # Rename columns
        movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
        print(f"Successfully renamed columns.")
    
    except Exception as e: print(e)
    
    ### Transform Rating Data and merge with movies_df ###
    try:
        # pivot data so movieId is the index, rating values are the columns and rows are the count for each rating value.
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'}, axis=1).pivot(index='movieId',columns='rating', values='count')
        
        # Rename columns with list comprehension.
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
        
        # Merge data with movies_df.
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
        
        # Fill na values with 0.
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
        
        print(f"Successfully merged ratings data and movies_df")
    
    except Exception as e: print(e)
        
 ### Connect to database ###
    
    try:
        # Create db engine and connect to database
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)
        print(f"Successfully created database engine.")
    
    except Exception as e: print(e)
    
    try:
        # Import movies info to postgres
        movies_df.to_sql(name='movies', con=engine, if_exists='replace')
        print(f"Successfully imported movies data to Postgresql")
    except Exception as e: print(e)
        
        
    try:
        # Import ratings to Postgres
        rows_imported = 0
        
        # get the start_time from time.time()
        start_time = time.time()
        
        
        for data in pd.read_csv(f'Resources/ratings.csv', chunksize=1000000):
            
             # print out the range of rows that are being imported
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            
            # Delete data but not table then append for next chunk
            if rows_imported == 0 :
                data.to_sql(name='ratings', con=engine, if_exists='replace')
            else:
                data.to_sql(name='ratings', con=engine, if_exists='append')
            
            rows_imported += len(data)

            # Add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
        

    except Exception as e: print(e)        


# In[ ]:


etl_wiki_kaggle_ratings('wikipedia.movies.json', 'movies_metadata.csv', 'ratings.csv')

