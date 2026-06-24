# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC
# MAGIC 1. First, upload the `movies_data_1.csv` file as a table into Databricks. To do that, click **New → Add or upload data → Create or modify a table**, then upload the CSV file.
# MAGIC 2. After the file is uploaded, set the **Table Name** and open **Advanced attributes**. Make sure the following options are checked: `First row contains the header`, `Automatically detect column types`, and `Rows span multiple lines` so that the file is properly parsed.
# MAGIC 3. Click the **Create table** button which will redirect you to a new page where you'll see the schema and sample data.
# MAGIC 4. Next, read the table as a pandas dataframe.

# COMMAND ----------

# Read table into spark dataframe and convert it to pandas
df = spark.read.table("workspace.default.movies_data_1").toPandas()

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movies dataset description
# MAGIC
# MAGIC *  **Movie:** movie name
# MAGIC *  **Year:** release year
# MAGIC *  **Genre:** movie genre
# MAGIC *  **Rating:** audience rating
# MAGIC *  **One-Line:** short description about the movie
# MAGIC *  **Stars:** the casting art. Contains the director and star actors
# MAGIC *  **Votes:** audience votes
# MAGIC *  **Runtime:** Duration of movie
# MAGIC *  **Gross:** total amount earned worldwide
# MAGIC *  **Extract_date:** datetime of extraction
# MAGIC *  **owner_company:** Owner company of the movie/tvshow
# MAGIC *  **nr_of_episodes:** number of episodes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirement: Data cleaning
# MAGIC
# MAGIC Apply the following steps for data cleaning:
# MAGIC 1. Clean dataset by removing empty rows. An empty row is considered when column Movies,Year, Genre, Rating, One-Line,Stars, Votes, RunTime and Gross are all null.
# MAGIC 2. Clean whitespaces, remove the '\n' from columns Genre, One-Line and Stars and '\t' from owner_company
# MAGIC

# COMMAND ----------

import pandas as pd
# Data cleaning requirements
df=df.dropna(
    subset=[
        "MOVIES",
        "YEAR",
        "GENRE",
        "RATING",
        "ONE-LINE",
        "STARS",
        "VOTES",
        "RunTime",
        "Gross"
    ],
    how="all"
)

df["GENRE"]=df["GENRE"].str.replace("\n"," ", regex=False)
df["ONE-LINE"]=df["ONE-LINE"].str.replace("\n"," ", regex=False)
df["STARS"]=df["STARS"].str.replace("\n"," ", regex=False)

df["owner_company"]=df["owner_company"].str.replace("\t","", regex=False)

for col in ["GENRE","ONE-LINE","STARS","owner_company"]:
    df[col]=df[col].str.strip()


df.head()

# write your code here

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirement: Column splitting
# MAGIC
# MAGIC Apply the following steps for extracting data into separate columns:
# MAGIC
# MAGIC 3. Create separate columns _"Director"_ and _"Stars"_ from column "STARS". Extract the director name and store it into the newly created column _"Director"_. Do the same thing for stars. In the end, drop the original "STARS" column.
# MAGIC
# MAGIC 4. Store separately date and timestamp from Extract_date column as new columns <code>extraction_date</code> and <code>extraction_time</code>. Drop the original "Extract_date" column in the end.
# MAGIC
# MAGIC

# COMMAND ----------

# write your code here
# Extract Director
df["Director"] = df["STARS"].str.extract(
    r"Director:\s*(.*?)\s*Stars:",
    expand=False
)

# Extract Stars
df["Stars"] = df["STARS"].str.extract(
    r"Stars:\s*(.*)",
    expand=False
)

# Remove original column
df = df.drop(columns=["STARS"])

# Convert Extract_date to datetime
df["Extract_date"] = pd.to_datetime(df["Extract_date"])

# Create new columns
df["extraction_date"] = df["Extract_date"].dt.date
df["extraction_time"] = df["Extract_date"].dt.time

# Remove original column
df = df.drop(columns=["Extract_date"])

df.head()                        
#

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirement: Year logic
# MAGIC
# MAGIC Apply the following steps for extracting data into separate columns:
# MAGIC
# MAGIC
# MAGIC 5. Calculate how long did the movie/tv show last and store the value in a separate column **"lasted"**. Also, store in new columns the **start_year** and **end_year** values of the movie when applicable. If the movie is still in production, then fill end_year with value <code>'present'</code>. In the end, drop the original "YEAR" column from the dataset.
# MAGIC

# COMMAND ----------

import pandas as pd
import re

# Read table into pandas dataframe
df = spark.read.table("workspace.default.movies_data_1").toPandas()

# -----------------------------
# Data cleaning
# -----------------------------

df = df.dropna(
    subset=[
        "MOVIES", "YEAR", "GENRE", "RATING", "ONE-LINE",
        "STARS", "VOTES", "RunTime", "Gross"
    ],
    how="all"
)

df["GENRE"] = df["GENRE"].str.replace("\n", " ", regex=False)
df["ONE-LINE"] = df["ONE-LINE"].str.replace("\n", " ", regex=False)
df["STARS"] = df["STARS"].str.replace("\n", " ", regex=False)
df["owner_company"] = df["owner_company"].str.replace("\t", "", regex=False)

for col in ["GENRE", "ONE-LINE", "STARS", "owner_company"]:
    df[col] = df[col].str.strip()

# -----------------------------
# Column splitting
# -----------------------------

df["Director"] = df["STARS"].str.extract(r"Director:\s*(.*?)\s*Stars:", expand=False)
df["Stars"] = df["STARS"].str.extract(r"Stars:\s*(.*)", expand=False)

df = df.drop(columns=["STARS"])

df["Extract_date"] = pd.to_datetime(df["Extract_date"])

df["extraction_date"] = df["Extract_date"].dt.date
df["extraction_time"] = df["Extract_date"].dt.time

df = df.drop(columns=["Extract_date"])

# -----------------------------
# Year logic
# -----------------------------

def extract_year_logic(year_value):
    if pd.isna(year_value):
        return pd.Series([None, None, None])

    year_text = str(year_value).replace("(", "").replace(")", "").strip()
    years = re.findall(r"\d{4}", year_text)

    if len(years) == 1:
        start_year = years[0]

        if "–" in year_text or "-" in year_text:
            end_year = "present"
            lasted = "present"
        else:
            end_year = years[0]
            lasted = 0

    elif len(years) >= 2:
        start_year = years[0]
        end_year = years[1]
        lasted = int(end_year) - int(start_year)

    else:
        start_year = None
        end_year = None
        lasted = None

    return pd.Series([start_year, end_year, lasted])

df[["start_year", "end_year", "lasted"]] = df["YEAR"].apply(extract_year_logic)

df = df.drop(columns=["YEAR"])

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirement: Dimension dataframes
# MAGIC
# MAGIC Apply the following steps for extracting data into separate columns:
# MAGIC
# MAGIC
# MAGIC 6. Extract unique values from **owner_company** column and store them as separate pandas Dataframe called <code>DimCompany</code>
# MAGIC 7. Similarly, extract unique values from **director column** and store it as separate pandas Dataframe called <code>DimDirector</code>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Write your code here
# Create DimCompany
DimCompany = pd.DataFrame(
    df["owner_company"].dropna().unique(),
    columns=["owner_company"]
)

# Create DimDirector
DimDirector = pd.DataFrame(
    df["Director"].dropna().unique(),
    columns=["Director"]
)

DimCompany.head()
DimDirector.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions
# MAGIC
# MAGIC The cells below are assertions for checking  whether the requirements were fulfilled on the movies dataframe. You can execute the cell to check your work. If everything passes, then you have successfully implemented the requirements.

# COMMAND ----------

import numpy as np

def assert_df(df):

    # Checks for the first csv file
    # the end dataset should be of shape (9999,16)
    assert df.shape[0] == 9999
    assert df.shape[1] == 17

    # The Lasted column should have 3180 rows with 'present' value
    assert df.loc[df["end_year"]=='present'].shape[0] == 3180
    assert df.loc[df["lasted"]==''].shape[0] == 8611 


    # random checks of data
    assert df.iloc[0]['Director'] == 'Peter Thorwarth'
    assert df.iloc[11]['MOVIES'] == 'Lucifer'
    assert df.iloc[11]['lasted'] == 5

    # there should be 10 unique values for companies
    assert len(df.owner_company.unique()) == 10

    # check if the unique values for companies are the same
    np.testing.assert_array_equal(df.owner_company.unique(),['Columbia Pictures', 'Legendary Entertainment',
        'Universal Pictures', 'Paramount Pictures', 'Walt Disney Pictures',
        'Marvel Studios', '20th Century Fox', 'Relativity Media',
        'RatPac-Dune Entertainment', 'Warner Bros.'])

    # there should be 3709 unique values for directors
    assert len(df.Director.unique()) == 3709
    print("Assertion of dataframe is complete!")


# COMMAND ----------

# Pass in the name of your movies dataframe
assert_df(df)