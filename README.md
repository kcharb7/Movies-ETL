# ETL
## Overview
### *Purpose*
AMAZING PRIME, the world’s largest retailer, provides streaming services for movies and tv shows through AMAZING PRIME Video. The AMAZING PRIME Video team wanted to develop an algorithm that predicted which low-budget movies being released would become popular so they could buy the streaming rights at a bargain. So, AMAZING PRIME planned to sponsor a hackathon for participants to predict the most popular movies and needed to create a clean dataset of all the movie data, including scraped Wikipedia data of all movies released since 1990 and rating data from MovieLens through Kaggle. 


## Extract the Data
### *Extract the Wikipedia Data*
To extract the data, I created a new Python3 notebook in Jupyter Notebook. Within the new file, I imported three dependencies: JSON library, Pandas library, and NumPy library:
```
import json
import pandas as pd
import numpy as np
```
After importing the dependencies, I imported the Wikipedia JSON file as a list of dictionaries using the load() method:
```
with open(f'{file_dir}wikipedia-movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
```
Using the len() function, I determined how many records were pulled in:
```
len(wiki_movies_raw)
```
A total of 7,311 records were pulled. I additionally ensured the data imported correctly by taking a look at a few individual records using index slicing:
```
# First 5 records
wiki_movies_raw[:5]

# Last 5 records
wiki_movies_raw[-5:]

# Some records in the middle
wiki_movies_raw[3600:3605]
```
The data seemed to have imported correctly, and so I moved on to importing the Kaggle data.

### *Extract the Kaggle Data*
To gather the ratings data from Kaggle, I navigated to their Movies Dataset and downloaded the zip file. After decompressing the CSV files, I selected the two required files: movies_metadata.csv and ratings.csv. Since Kaggle data was already in flat-file formats, I pulled them directly into a Pandas DataFrames using the following code:
```
kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
ratings = pd.read_csv(f'{file_dir}ratings.csv')
```
Then, I used the head(), tail(), and sample() methods to inspect each DataFrame.

## Clean Individual Datasets
### *Wikipedia Data*
Using the following code, I created a DataFrame of the wiki_movies_raw list of dictionaries to inspect the data:
```
wiki_movies_df = pd.DataFrame(wiki_movies_raw)
wiki_movies_df.head()
```
Printing the first five rows of the DataFrame showed that there were 5 rows of data and 193 columns. To see all of the columns, I converted wiki_movies_df.columns to a list:
```
wiki_movies_df.columns.tolist()
```
Several column names jumped out that did not relate to movie data, including “Dewey Decimal”, “Headquarters”, and “Number of employees”. So, I modified the JSON data by restricting it to only entries that had a director and an IMDb link. Using a list comprehension, I filtered results to see if ‘Director’ and ‘Directed by’ were keys in the current dictionary and if a director was listed, to check that the dictionary contained an IMDb link. Results were compiled into a new variable, wiki_movies and the length was checked:
```
wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie]
len(wiki_movies)
```
The number of movies included was 7,080. After making a DataFrame from wiki_movies, the DataFrame contained 78 columns:
```
wiki_movies_director = pd.DataFrame(wiki_movies)
wiki_movies_director.head()
```
Further looking at the data, the column “No. of episodes” stood out, indicating that some TV shows were included in the data instead of movies. So, I added this filter to my list comprehension:
```
wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
```
After filtering out the bad data, I cleaned up each movie entry so it was in standard format by creating a function. First, I wrote a simple function to make a copy of the movie and return it. As the movies were dictionaries and I wanted to make non-destructive edits, I made a copy of the incoming movie using the dict() constructor and assigned it to a new local variable called movie:
```
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    return movie
```
When looking at the previous DataFrame, different language columns were included. Taking a closer look at the columns, I used the followed code to see which movies had a value for “Arabic”, as it was the first language in the list:
```
wiki_movies_df[wiki_movies_df['Arabic'].notnull()]
```
The results returned two movies, and using the url listed, I looked at the Wikipedia pages for more details. It was found that the different language columns are for alternate titles of the movie. So, I combined all alternate titles into one dictionary by first going through each of the columns one by one and determining which are alternate titles. After looking at the columns, the following held alternate title data: Also known as, Arabic, Cantonese, Chinese, French, Hangul, Hebrew, Hepburn, Japanese, Literally, Mandarin, McCune-Reischauer, Original title, Polish, Revised Romanization, Romanized, Russian, Simplified, Traditional, and Yiddish. To handle the alternative titles, I created an empty dictionary to hold all of the alternative titles within my function. Then, I created a for loop to loop through a list of all alternative title keys. Within the for loop, I added an if statement to check if the current key exists in the movie object, and if so, remove the key-value pair and add to the alternative titles dictionary. After looping through every key, the alternative titles dictionary was then added to the movie object:
```
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy

    
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

    return movie
```
Then, I created a list of cleaned movies with a list comprehension and set wiki_movies_df to be the DataFrame created from clean_movies. Subsequently, I printed a list of the columns:
```
clean_movies = [clean_movie(movie) for movie in wiki_movies]

wiki_movies_df = pd.DataFrame(clean_movies)
sorted(wiki_movies_df.columns.tolist())
```
Next, I worked on cleaning up the column names. Some columns, such as “Directed by” and “Director” contained the same data. So, I created a new function within clean_movie() to consolidate columns with the same data into one column. This function checked if a key existed in a given movie record and if the statement was true, the pop() method was used to change the name of the dictionary key:
```
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy

    
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
        
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
            
    return movie
```
Once the function was created, I inputted ‘Directed by’ as the old_name and ‘Director’ as the new_name inside the parentheses of change_column_name and placed the statement below the change_column_name function. The same was done for the remaining columns with the same data:
```
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
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

    return movie
```
After running the updated function, I reran the list comprehension to clean wiki_movies and recreate wiki_movies_df.

With the columns tidied up, I moved on to cleaning the rows. Since I planned to use the IMDb ID to merge with the Kaggle data, I removed any duplicate rows according to IMDb ID. To do this, I used Pandas’ built-in string methods and str.extract(). The IMDb ID is the last portion of the IMDb link, with the general format “tt1234567”, so I used this format for my regular expression. Within the parenthesis of str.extract() I placed the regular expression, along with an r before the quotes to tell Python to treat the regular expression as a raw string of text. Then, I used the drop_duplicates method with the subset argument equal to IMDb ID and the inplace equal to True to drop any duplicates of IMDb IDs. I additionally added a print statement to determine the number of rows dropped. 
```
wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
print(len(wiki_movies_df))
wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
print(len(wiki_movies_df))
wiki_movies_df.head()
```
After consolidating redundant columns, I determined which columns did not contain useful data. To get the count of null values for each column I used a list comprehension:
```
[[column,wiki_movies_df[column].isnull().sum()] for column in wiki_movies_df.columns]
```
Over half the columns contained more than 6,000 null values. To remove these columns, I first created a list of the columns I wished to keep with less than 90% null values by tweaking my list comprehension.
```
[column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
```
Then, I selected these columns from the wiki_movies_df:
```
wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
```
After trimming down the wiki_movies_df DataFrame to include only the columns of interest, I used the following code to determine the data types for each column and which columns needed to be converted:
```
wiki_movies_df.dtypes
```
Looking through the data, it was apparent that “Box office”, “Budget”, and “Running time” each needed to be numeric, while “Release date” needed to be a date object. The box office and budget amounts, however, were not written in a consistent style. Starting with the box office data, I used the dropna() method to look only at rows where box office data was defined:
```
box_office = wiki_movies_df['Box office'].dropna()
```
In order to use a regular expression, I first ensured all box office data was entered as a string by creating a lambda function with a map() call:
```
box_office[box_office.map(lambda x: type(x) != str)]
```
The output indicated there were a number of data points stored as lists. Using a simple space as the joining character, and applying the join() function only when the data points were a list, I concatenated list items into one string:
```
box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
```
Further investigation of the data showed that the box office numbers were written either as “/$123.4 million” (or billion) or “/$123,456,789”. To find out how many of each style were within the data, I used regular expressions. First, I imported the Python module for regular expressions by adding it with my other import statements and rerunning the cell. Then I created a regular expression for each form. For the first form, I created the regular expression “\$\d+\.?\d*\s*[mb]illion” and used the str.contains() method and sum() method on box_office to count how many box office values match the first form:
```
form_one = r'\$\d+\.?\d*\s*[mb]illion'
box_office.str.contains(form_one, flags=re.IGNORECASE).sum()
```
A total of 3,896 box office values matched the form “/$123.4 million/billion”.

The same method was used for the second form:
```
form_two = r'\$\d{1,3}(?:,\d{3})+'
box_office.str.contains(form_two, flags=re.IGNORECASE).sum()
```
A total of 1,544 box office values matched the form “$123,456,789”.

To determine the box office values that were not described by either form, I created two Boolean Series called matches_form_one and matches_form_two and then selected the box office values that did not match either:
```
matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)

box_office[~matches_form_one & ~matches_form_two]
```
The output indicated that some box office values contained spaces in between the dollar sign and the number, some used a period as a thousands separator, some values were given as a range, and “million” was sometimes misspelled as “millon”. To capture any spaces after the dollar sign, I added \s* after the dollar signs in both forms:
```
form_one = r'\$\s*\d+\.?\d*\s*[mb]illion'
form_two = r'\$\s*\d{1,3}(?:,\d{3})+'
```

To capture any periods used as a thousands separator, I changed form_two to include [,\.]:
```
form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+'
```
To ensure I only captured values of the form “$123.456.789” and not “1.234 billion”, I additionally added a negative lookahead group that looked ahead for “million” or “billion” to reject any matches with those strings:
```
form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
```
As some values were given as a range, I searched strings that started with a dollar sign and ended with a hyphen, replacing any matches with a dollar sign using the replace() method:
```
box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
```
Finally, to capture box office values misspelled as “millon”, I changed the form_one expression so that the second “i” was optional:
```
form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
```
With expressions to match almost all of the box office values, I used the str.extract() method to extract the parts of the strings that matched. Within the parenthesis of the str.extract(), I created a regular expression to capture data that matched either form_one or from_two:
```
box_office.str.extract(f'({form_one}|{form_two})')
```
Next, I created a function that turned the extracted values into a floating-point number. I used re.match(pattern, string) to determine if the string matched the pattern and re.sub(pattern, replacement_string, string) to remove dollar signs, spaces, commas, and letters. Then all strings were converted to floats and multiplied by a million or billion:
```
def parse_dollars(s):
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
```
Then I created code that used the str.extract to extract the values from box_office and applied parse_dollars:
```
wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
```
As I no longer needed the original “Box Office” column, I dropped it from the wiki_movies_df DataFrame:
```
wiki_movies_df.drop('Box office', axis=1, inplace=True)
```
To similarly parse the budget data, I first created a budget variable with the following code:
```
budget = wiki_movies_df['Budget'].dropna()
```
Next, I converted any lists to strings:
```
budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
```
After converting the lists to strings, I removed any values between a dollar sign and a hyphen:
```
budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
```
I applied the same pattern matches created for parsing the box office data to the budget data:
```
matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
budget[~matches_form_one & ~matches_form_two]
```
Running the above code parsed almost all of the budget data, except citation references. To remove these references, I created another regular expression and placed it within the parentheses of str.replace():
```
budget = budget.str.replace(r'\[\d+\]\s*', '')
budget[~matches_form_one & ~matches_form_two]
```
Using the same code that was used to parse the box office values, I parsed the budget values:
```
wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
```
Once the data was parsed, I dropped the original Budget column:
```
wiki_movies_df.drop('Budget', axis=1, inplace=True)
```
To parse the release date data, I created a variable to hold the non-null release date values and converted the lists to strings:
```
release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
```
Then, I created regular expressions to capture four different forms: full month name, one- to two-digit day, four-digit year (i.e., April 11, 2021); four-digit year, two-digit month, two-digit day, with any separator (i.e., 2021-04-11); full month name, four-digit year (i.e., April 2021); and four-digit year:
```
date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
date_form_two = r'\d{4}.[01]\d.[123]\d'
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
date_form_four = r'\d{4}'
```
After creating the four forms, I extracted the dates with the following code:
```
release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
```
Using the to_datetime() method in Pandas and setting the infer_datetime_format option to True, I parsed the dates:
```
wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
```
To parse the running time data, I created a variable to hold the non-null values and converted the lists to strings:
```
running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
```
Most forms appeared to look like “100 minutes” and so I used the following code to determine how many running times actually followed this form:
```
running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE).sum()
```
The output indicated that 6,528 entries followed this form. To determine the form of the remaining 366 entires, I used the following code:
```
running_time[running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE) != True]
```
The output showed several entries with “min” instead of “minutes”. So, I made my regular expression more general by only marking the beginning of the string with the letter “m”:
```
running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE).sum()
```
Changing the regular expression captured 6,877 entries. To check the remaining forms, I ran the following code:
```
running_time[running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE) != True]
```
To capture the remaining forms, I removed the caret(^) and added an additional regular expression to capture all of the hour + minute patterns. With this new expression, I used the str.extract() to extract only the digits: 
```
running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
```
Within the new DataFrame, I converted the strings to numeric values using the to_numeric() method and set the errors argument to ‘coerce’ to turn any empty strings to NaN. Then, I used the fillna() to change all NaNs to zero:
```
running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
```
Next, I applied a function to convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group was zero:
```
wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
```
Finally, I dropped the Running time column from the wiki_movies_df DataFrame:
```
wiki_movies_df.drop('Running time', axis=1, inplace=True)
```

### *Kaggle Data*
As the first step in cleaning the Kaggle data, I checked the data types for each column:
```
kaggle_metadata.dtypes
```
I identified six columns with an “object” data type that needed to be converted. Specifically, the “release_date” column needed to be converted to a datetime data type, the “video” and “adult” columns needed to be converted to a Boolean data type, while the “popularity”, “ID”, and “budget” columns needed to be converted to a numeric data type. Starting with the “video” and “adult” columns, I checked that all values were either “True” or “False” using the following code:
```
kaggle_metadata['adult'].value_counts()
```
The output showed several lines of text in addition to “True” and “False”, indicating that some bad data was included. To remove the bad data, I used the following code:
```
kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]
```
The hackathon did not want any adult films included in the dataset, so I additionally removed any rows where the “adult” column was “True” and then dropped the “adult” column:
```
kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
```
After dropping irrelevant rows, I moved on to the “video” column and used the same code as above to check its values:
```
kaggle_metadata['video'].value_counts()
```
The output indicated the only values were “True” or “False”. So, I converted the “video” column using the following code:
```
kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
```
For the columns that needed to be converted to a numeric data type, I used the to_numeric() method from Pandas and set the “errors=” argument to “raise” to identify any data that couldn’t be converted to numbers:
```
kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
```
The code ran without errors, so I moved on to converting the “release_date” column to a datetime data type using the to_datetime() function:
```
kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
```
Once the Kaggle metadata was cleaned, I used the info() method on the ratings DataFrame to take a look at the ratings data:
```
ratings.info(null_counts=True)
```
The output indicated that the “timestamp” column needed to be converted to a datetime data type. Based on the MovieLens documentation, the timestamp is the number of seconds since midnight of January 1, 1970. To convert the “timestamp” column, I used the to_datetime() function and included the origin as ‘unix’ and the time unit as seconds in the parentheses:
```
pd.to_datetime(ratings['timestamp'], unit='s')
```
As the output looked reasonable, I assigned it to the timestamp column:
```
ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
```
To determine any glaring errors of the actual ratings, I created a histogram of the rating distributions and used the describe() method to print out some statistics:
```
pd.options.display.float_format = '{:20,.2f}'.format
ratings['rating'].plot(kind='hist')
ratings['rating'].describe()
```

![histogram.png](https://github.com/kcharb7/Movies-ETL/blob/main/Resources/histogram.png)

All ratings were between 0 and 5, with a median of 3.5 and a mean of 3.53. 

## Merge Datasets
### *Merge Wikipedia and Kaggle Metadata*
The Wikipedia data and Kaggle data were merged by IMDb ID. For the merge, I used the suffixes parameter to more easily identify where each column came from:
```
movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
movies_df.head()
```
Seven pairs of columns contained redundant information: title_wiki and title_kaggle; running_time and runtime; budget_wiki and budget_kaggle; box_office and revenue; release_date_wiki and release_date_kaggle; Language and original_language; Production company(s) and production_companies.

First, I took a look at some of the titles, finding that most were consistent:
```
movies_df[['title_wiki','title_kaggle']]
```
Then, I looked at the rows where the titles did not match:
```
movies_df[movies_df['title_wiki'] != movies_df['title_kaggle']][['title_wiki','title_kaggle']]
```
Slight variations were seen between the titles; though, the Kaggle data looked a little more consistent. So, I determined whether they were any missing titles within the Kaggle data:
```
# Show any rows where title_kaggle is empty
movies_df[(movies_df['title_kaggle'] == ' ') | (movies_df['title_kaggle'].isnull())]
```
No results were returned, so I determined the Wikipedia titles needed to be dropped. 

Next looking at the “running_time” and “runtime” columns, I created a scatter plot to get a sense of how similar the columns are to each other and filled in any missing values with zero:
```
movies_df.fillna(0).plot(x='running_time', y='runtime', kind='scatter')
```

![scatter.png](https://github.com/kcharb7/Movies-ETL/blob/main/Resources/scatter.png)

More data points were located on the origin of the y-axis than the x-axis, with the x-axis as Wikipedia and the y-axis as Kaggle. Thus, there was more missing entries in the Wikipedia data set than in the Kaggle data set. Furthermore, the Wikipedia data set appeared to have some outliers, making Kaggle data better for use. However, the Kaggle data contained some zeros for the runtime of some movies while Wikipedia data had values, thus, the Wikipedia data was used to fill in zeroes within the Kaggle dataset. 

I created another scatterplot of the budget_wiki and budget_kaggle columns:
```
movies_df.fillna(0).plot(x='budget_wiki',y='budget_kaggle', kind='scatter')
```

![scatter2.png](https://github.com/kcharb7/Movies-ETL/blob/main/Resources/scatter2.png)

The Wikipedia data contained more outliers than the Kaggle data; though, the Kaggle data had more movies with no budget data. Thus, the Kaggle was kept, and the Wikipedia data was used to fill in any gaps.

A third scatter plot was made of the “box_office” and “revenue” columns:
```
movies_df.fillna(0).plot(x='box_office', y='revenue', kind='scatter')
```

![scatter3.png](https://github.com/kcharb7/Movies-ETL/blob/main/Resources/scatter3.png)

The data plots looked pretty close, however, the large outlier may have thrown off the scale. So, I created another scatter plot for values less than $1 billion in “box_office”:
```
movies_df.fillna(0)[movies_df['box_office'] < 10**9].plot(x='box_office', y='revenue', kind='scatter')
```

![scatter4.png](https://github.com/kcharb7/Movies-ETL/blob/main/Resources/scatter4.png)

The scatter plot looked similar to that seen for budget. Consequently, the Kaggle data was kept and any zeroes were filled with Wikipedia data.

To compare the “release_data_wiki” and “release_date_kaggle” columns, I created a line plot and changed the style to dots:
```
movies_df[['release_date_wiki','release_date_kaggle']].plot(x='release_date_wiki', y='release_date_kaggle', style='.')
```

![line.png](https://github.com/kcharb7/Movies-ETL/blob/main/Resources/line.png)

One substantial outlier was found around 2006. To look more closely at this movie, I searched for any movie in the DataFrame with a release date after 1996 according to Wikipedia and before 1965 according to Kaggle:
```
movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')]
```
The output showed that the movie “The Holiday” in the Wikipedia data got merged with the movie “From Here to Eternity” from the Kaggle data. Thus, this row needed to be dropped. To drop the row, I obtained its index using the following code:
```
movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index
```
Then, the following code was used to drop the row:
```
movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
```
After dropping the row, I checked for any null values within the Wikipedia data:
```
movies_df[movies_df['release_date_wiki'].isnull()]
```
11 release dates were missing in the Wikipedia data. 

Using the same code as above, I checked for any null values within the Kaggle data:
```
movies_df[movies_df['release_date_kaggle'].isnull()]
```
The Kaggle data did not have any missing values and thus the Wikipedia data was dropped. 

For the language data, I converted the lists in the “Language” column from the Wikipedia data to tuples so I could use the value_counts() method and compare the columns:
```
movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)
```
The “original_language” column from the Kaggle data did not contain any lists, so I just ran the value_counts() on it:
```
movies_df['original_language'].value_counts(dropna=False)
```
The Kaggle data appeared to be in a more consistent usable format and thus the Wikipedia data was dropped. 

Finally, I compared the “Production company(s)” and “production_companies” columns by looking at a small number of samples:
```
movies_df[['Production company(s)','production_companies']]
```
The Kaggle data was in a more consistent format and thus the Wikipedia data was dropped. 

Putting all of the above together, I first dropped the “title_wiki”, “release_date_wiki”, “Language” and “Production company(s)” columns:
```
movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
```
Next, I created a function to fill in missing data of the Kaggle column with data from the paired Wikipedia column and then drop the Wikipedia column:
```
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)
```
Then, I ran the function for the three column pairs where zeroes needed to be filled:
```
fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
movies_df
```
With the data merged and values filled, I checked for any columns with only one value by converting any lists to tuples and using the value_counts() method:
```
for col in movies_df.columns:
    lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
    value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
    num_values = len(value_counts)
    if num_values == 1:
        print(col)
```
Running the cell showed that “video” contained only one value. Using the following code, I checked the value_counts for “video:
```
movies_df['video'].value_counts(dropna=False)
```
The output showed that the “video” column contained “False” for every row and thus did not need to be included in the DataFrame.

To make the dataset easier for hackathon participants to read, I reordered the columns:
```
movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
```
Then, I renamed the columns to be more consistent:
```
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
```

### *Transform and Merge Rating Data*
The rating data needed to be included for each movie, however, the rating dataset contained too much information and needed to be reduced to a useful summary. So, I used a groupby on the “movieId” and “rating” columns and the count() method to get the count for how many times a movie received a given rating:
```
rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()
```
Then I renamed the “userId” column to “count”:
```
rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1)
```
Next, I pivoted the data so that the “movieId” was the index, the columns were all the rating values, and the rows were the counts for each rating value:
```
rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
```
To make the columns easier to understand, I renamed the columns using a list comprehension to prepend “rating_” to each column: 
```
rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
```
I merged the movies_df with the rating_counts using a left merge:
```
movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
```
Finally, I filled any missing rating values with a zero:
```
movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
```

## Load the Data
### *Connect Pandas and SQL*
Amazing Prime wanted to provide the data to hackathon participants in a SQL database. In pgAdmin, I created a new database called “movie_date”. In Jupyter Notebook, I imported create_engine from the sqlalchemy module by adding the code to my first cell and re-running the cell:
```
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password 
```
Then, I created a connection string:
```
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
```
Using the connection string, I created the database engine:
```
engine = create_engine(db_string)
```
After creating the database engine, I saved the movies_df DataFrame to a SQL table using the to_sql() method:
```
movies_df.to_sql(name='movies', con=engine)
```
The ratings data was too large to import in one statement and was divided into chunks of data. The data was divided into chunks by reimporting the CSV using the chunksize= parameter in read_csv() and then a for loop was used to append the chunks of data to the new rows to the target SQL table. I additionally added print statements to check how many rows had been imported and how much time had elapsed:
```
rows_imported = 0
start_time = time.time()
for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
    print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    data.to_sql(name='ratings', con=engine, if_exists='append')
    rows_imported += len(data)

    print(f'Done. {time.time() - start_time} total seconds elapsed')
```

# ETL Challenge
## Overview
### *Purporse*
Amazing Prime requested an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables so that the dataset can be updated on a daily basis. To do this, I needed to refactor the code I previously created to create one function that takes in the three files (i.e., Wikipedia data, Kaggle metadata, and MovieLens rating data) and performs the ETL process by adding the data to a PostgreSQL database.

## Analysis
### *Write a Function to Read Three Data Files*
To begin, I created and named a function to read in the three CSV files:
```
# 1. Create a function that takes in three arguments;
# Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)

def extract_transform_load():
```
Next, I read in the Kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames:
```
    # 2. Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(kaggle_file, low_memory=False)
    ratings = pd.read_csv(ratings_file)
```
Following this, I opened the Wikipedia JSON file and used the json.load() function to convert the JSON data to raw data. The raw data was read in as a Pandas DataFrame:
```
    # 3. Open the read the Wikipedia data JSON file.
    with open(wiki_file, mode='r') as file:
        wiki_movies_raw = json.load(file)
    # 4. Read in the raw wiki movie data as a Pandas DataFrame.
    wiki_movies_df = pd.DataFrame(wiki_movies_raw)
```
I created a statement to return the three DataFrames:
```
    # 5. Return the three DataFrames
    return wiki_movies_df, kaggle_metadata, ratings
```
Then, I created paths to the Wikipedia data, Kaggle metadata, and MovieLens ratings data files:
```
# 6 Create the path to your file directory and variables for the three files. 
file_dir = '/Users/kimberlycharbonneau/Documents/Data_Analytics_Boot_Camp/Analysis_Projects/Module8-ETL/Movies-ETL/Resources'
# Wikipedia data
wiki_file = f'{file_dir}/wikipedia-movies.json'
# Kaggle metadata
kaggle_file = f'{file_dir}/movies_metadata.csv'
# MovieLens rating data.
ratings_file = f'{file_dir}/ratings.csv' 
```
I set these three variables equal to the extract_transform_load() function:
```
# 7. Set the three variables in Step 6 equal to the function created in Step 1.
wiki_file, kaggle_file, ratings_file = extract_transform_load()
```
Then, I reassigned the variables created to the DataFrames in the return statement of the function:
```
# 8. Set the DataFrames from the return statement equal to the file names in Step 6. 
wiki_movies_df = wiki_file
kaggle_metadata = kaggle_file
ratings = ratings_file
```
Finally, I checked that all three files were converted to a DataFrame:
```
# 9. Check the wiki_movies_df DataFrame.
wiki_movies_df.head()

# 10. Check the kaggle_metadata DataFrame.
kaggle_metadata.head()

# 11. Check the ratings DataFrame.
ratings.head()
```

### *Extract and Transform the Wikipedia Data*
After creating a new Jupyter Notebook file named ETL_clean_wiki_movies.ipynb, I added my code from earlier for the clean move function that takes in the argument “movie”:
```
# 1. Add the clean movie function that takes in the argument, "movie".
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
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

    return movie
```
Next, I added the function that reads in the three data files:
```
# 2 Add the function that takes in three arguments;
# Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)

def extract_transform_load():
    # Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(kaggle_file, low_memory=False)
    ratings = pd.read_csv(ratings_file)   

    # Open and read the Wikipedia data JSON file.
    with open(wiki_file, mode='r') as file:
        wiki_movies_raw = json.load(file)
```
Inside the function, I included the list comprehension from my original code that filtered out TV shows:
```
    # 3. Write a list comprehension to filter out TV shows.
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
```
Then, I wrote another list comprehension to iterate through the cleaned wiki movies list that was created from the prior list comprehension:
```
    # 4. Write a list comprehension to iterate through the cleaned wiki movies list
    # and call the clean_movie function on each movie.
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
```
The cleaned movie list was subsequently read in as a DateFrame:
```
    # 5. Read in the cleaned movies list from Step 4 as a DataFrame.
    wiki_movies_df = pd.DataFrame(clean_movies)
```
A try-except block was created to catch any errors when extracting the IMDb IDs with a regular expression string and dropping any imdb_id duplicates. Under the except statement, a print statement was included to capture if an error occurred:
```
    # 6. Write a try-except block to catch errors while extracting the IMDb ID using a regular expression string and
    #  dropping any imdb_id duplicates. If there is an error, capture and print the exception.
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    except:
        print("IMDb link not found")
```
I used the list comprehension from above to create a list of columns to keep within the DataFrame that had less than 90% null values and used this list to create a wiki_movies_df DataFrame:
```
    #  7. Write a list comprehension to keep the columns that don't have null values from the wiki_movies_df DataFrame.
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
```
Next, I created a variable to hold all the non-null values from the “Box office” column:
```
    # 8. Create a variable that will hold the non-null values from the “Box office” column.
    box_office = wiki_movies_df['Box office'].dropna()
```
Using the lambda and join functions, I converted the box_office data to string values:
```
    # 9. Convert the box office data created in Step 8 to string values using the lambda and join functions.
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
```
Then, I wrote two regular expressions: one to match the six elements of form_one of the box_office data and another to match the three elements of form_two of the box_office data:
```
    # 10. Write a regular expression to match the six elements of "form_one" of the box office data.
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    
    # 11. Write a regular expression to match the three elements of "form_two" of the box office data.
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
```
After creating the regular expressions, I added the parse_dollars function:
```
    # 12. Add the parse_dollars function.
    def parse_dollars(s):
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
```
Afterwards, I added the code that used the form_one and form_two lists created to clean the “box_office” column within the wiki_movies_df DataFrame:
```
    # 13. Clean the box office column in the wiki_movies_df DataFrame.
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
```
I added the code to clean the “budget”, “release_date”, and “running_time” columns:
```
    # 14. Clean the budget column in the wiki_movies_df DataFrame.
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    # 15. Clean the release date column in the wiki_movies_df DataFrame.
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    # 16. Clean the running time column in the wiki_movies_df DataFrame.
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
```
Next, I added the code to create a path to each data file:
```
# 17. Create the path to your file directory and variables for the three files.
# Create the path to your file directory and variables for the three files.
file_dir = '/Users/kimberlycharbonneau/Documents/Data_Analytics_Boot_Camp/Analysis_Projects/Module8-ETL/Movies-ETL/Resources'
# Wikipedia data
wiki_file = f'{file_dir}/wikipedia-movies.json'
# Kaggle metadata
kaggle_file = f'{file_dir}/movies_metadata.csv'
# MovieLens rating data.
ratings_file = f'{file_dir}/ratings.csv' 
```
The variables wiki_file, kaggle_file, and ratings_file were set equal to the extract_transform_load function:
```
# 18. Set the three variables equal to the function created in D1.
wiki_file, kaggle_file, ratings_file = extract_transform_load()
```
Then, the wikie_movies_df DataFrame was set equal to the wiki_file variable:
```
# 19. Set the wiki_movies_df equal to the wiki_file variable. 
wiki_movies_df = wiki_file
```
I checked that the wiki_movies_df looked correct:
```
# 20. Check that the wiki_movies_df DataFrame looks like this. 
wiki_movies_df.head()
```
Finally, I added the columns from the wiki_movies_df DataFrame to a list to confirm all columns were correct:
```
# 21. Check that wiki_movies_df DataFrame columns are correct. 
wiki_movies_df.columns.to_list()
```

### *Extract and Transform the Kaggle Data*
After adding the function from the ETL_clean_wiki_movies.ipynb file to the ETL_clean_kaggle_data.ipynb, I added the code that cleaned the Kaggle metadata:
```
    # 2. Clean the Kaggle metadata.
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
```
Then, I added the code to merge the wiki_movies_df DataFrame and the Kaggle_metadata DataFrames:
```
    # 3. Merged the two DataFrames into the movies DataFrame.
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
```
The code to drop unnecessary columns was subsequently added:
```
    # 4. Drop unnecessary columns from the merged DataFrame.
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
```
Then, I added the fill_missing_kaggle_data() function:
```
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)
```
After including the function, I called the function with the columns that needed to be cleaned as arguments:
```
    # 6. Call the function in Step 5 with the DataFrame and columns as the arguments.
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
```
The code to filter the columns within the movies_df DataFrame and rename the columns was added:
```
    # 7. Filter the movies DataFrame for specific columns.
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]


    # 8. Rename the columns in the movies DataFrame.
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
```
Next, I added the code to transform and merge the ratings DataFrame with the movies_df DataFrame and clean the new movies_with_ratings_df DataFrame:
```
    # 9. Transform and merge the ratings DataFrame.
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')
    
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
```
Afterwards, I added the code that created the path to each file:
```
# 10. Create the path to your file directory and variables for the three files.
# Create the path to your file directory and variables for the three files.
file_dir = '/Users/kimberlycharbonneau/Documents/Data_Analytics_Boot_Camp/Analysis_Projects/Module8-ETL/Movies-ETL/Resources'
# Wikipedia data
wiki_file = f'{file_dir}/wikipedia-movies.json'
# Kaggle metadata
kaggle_file = f'{file_dir}/movies_metadata.csv'
# MovieLens rating data.
ratings_file = f'{file_dir}/ratings.csv' 
```
The three variables were set equal to the extract_transform_load() function:
```
# 11. Set the three variables equal to the function created in D1.
wiki_file, kaggle_file, ratings_file = extract_transform_load()
```
The DataFrames from the return statement were then set equal to the file names:
```
# 12. Set the DataFrames from the return statement equal to the file names in Step 11. 
wiki_movies_df = wiki_file
movies_with_ratings_df = kaggle_file
movies_df = ratings_file
```
Each DataFrame was checked to ensure it was correct:
```
# 13. Check the wiki_movies_df DataFrame. 
wiki_movies_df.head()

# 14. Check the movies_with_ratings_df DataFrame.
movies_with_ratings_df.head()

# 15. Check the movies_df DataFrame. 
movies_df.head()
```

### *Create the Movie Database*
Before creating the movie database, I made a copy of my ETL_clean_kaggle_data.ipynb file and renamed it ETL_create_database.ipynb. Then, in the first cell, I uncommented the “from config import db_password” and removed the return statement under my extract_transform_load() function. After the “Transform and merge the ratings DataFrame” section, I added the code to create the connection to the PostgresSQL database, as well as the code to add the movies_df DataFrame to the database, using ‘replace’ for the if_exists parameter so the movies_df DataFrame data wouldn’t be added to the table again:
```
    # Create the connection to the PostgreSQL database 
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    
    # Add the movies_df DataFrame to the SQL database
    movies_df.to_sql(name='movies', con=engine, if_exists = 'replace')
```
Using the following code, I dropped the ratings table before adding the code that read the MovieLens rating CSV data into the SQL database:
```
    # Drop the ratings table
    #Establish the connection
    conn = psycopg2.connect(
    database="movie_data", user='postgres', password='db_password', host='127.0.0.1', port= '5432'
)
    # Set autocommit
    conn.autocommit = True
    
    #Create a cursor object using the cursor() method
    cursor = conn.cursor()
    
    #Drop ratings table if already exists
    cursor.execute("DROP TABLE ratings")
    print("Ratings table dropped")
    
    #Commit changes in the database and close the connection
    conn.commit()
    conn.close()
    
    # Add the MovieLens rating CSV data to the SQL database
    rows_imported = 0
    start_time = time.time()
    for data in pd.read_csv(ratings_file, chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)

        print(f'Done. {time.time() - start_time} total seconds elapsed')
```
After running the program, I ran a query on the PostgreSQL database to retrieve the number of rows for the movies table:
```
SELECT COUNT(*) FROM movies

```

![movies_query.png](https://github.com/kcharb7/Movies-ETL/blob/main/Resources/movies_query.png)

This confirmed that the movies table contained 6,052 rows. 

I refactored this code to ensure all of the rows for the ratings table were imported:
```
SELECT COUNT(*) FROM ratings
```

![ratings_query.png](https://github.com/kcharb7/Movies-ETL/blob/main/Resources/ratings_query.png)

The ratings table contained 26,024,289 rows. 
