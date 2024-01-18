##### Pyspark #####

#summary statistics of columns present in the data frame (df)
df.describe()

print('Data overview')
df.printSchema()

print('Columns overview')
pd.DataFrame(df.dtypes, columns = ['Column Name','Data type'])

#FIlter singular column
df.filter(df.state == "OH").show(truncate=False)

# Filter multiple conditi don
df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False) 


'''
Using count() action to get the number of rows on DataFrame and len(df.columns()) to get the number of columns.
'''
def spark_shape(sparkDf):
  print(sparkDf.count(), len(sparkDf.columns))

print("spark_shape(sparkDf) loaded")
print("Using count()  to get the number of rows on DataFrame and len(df.columns()) to get the number of columns.")

# Counting distinct values from a column
df.select('column name').distinct().collect()


##### Pandas #####
import pandas

# Printing the percentage of missing values per column
def percent_missing(df:pd.DataFrame, verbose = False) -> pd.DataFrame:
  '''
  Derives the percentage of missing values for each column in a dataframe

  Args:
      df = pandas dataframe

  Returns
      pandas dataframe with column names and percentage of missing values in respective columns
  '''
  col_names_list=[]
  pct_missing_list=[]

  # Summing the number of missing values per column and then dividing by the total rows
  sum_missing = df.isnull().values.sum(axis=0)
  pct_missing = sum_missing / df.shape[0]

  for idx, col in enumerate(df.columns):
    col_names_list.append(col)
    pct_missing_list.append(pct_missing[idx])
    if verbose == True:
      print ('{0}: {1:.2f}%'.format(col, pctMissing[idx] * 100))
    else:
      continue
  # Create dataframe
  df_results = pd.DataFrame({'Columns': col_names_list, "Percent Missing": pct_missing_list})
  return df_results

# Counting the length of column
df['[column_name]']str.len()

def invalid_tweets(tweets: pd.DataFrame) -> pd.DataFrame:
    min = 15
    is_valid = tweets['content'].str.len() > min
    df = tweets[is_valid]
    return df[['tweet_id']]

def invalid_tweets(tweets: pd.DataFrame) -> pd.DataFrame:
    # Filter rows where the length of 'content' is strictly greater than 15
    invalid_tweets_df = tweets[tweets['content'].str.len() > 15]
    
    # Select only the 'tweet_id' column from the invalid tweets DataFrame
    result_df = invalid_tweets_df[['tweet_id']]
    
    return result_df

#Counting the number of rows & columns of a dataframe
# computing number of rows
rows = len(df.axes[0])
 
# computing number of columns
cols = len(df.axes[1])

print("Number of Rows: ", rows)
print("Number of Columns: ", cols)

        
df.head()
df["column_name"].mean()
df["column_name"].median()
df["column_name"].describe()

#Grouping

# counting unique values
n = len(pd.unique(df['height']))
print("No.of.unique values :", n)

#Filtering
rslt_df = dataframe[dataframe['column_name'] > 70])

#Counting columns conditionally
def monthly_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate monthly transactions summary.

    Args:
        transactions (pd.DataFrame): Input DataFrame containing transaction data.

    Returns:
        pd.DataFrame: Monthly transactions summary.

    Notes:
        - Adds 'approved_count' and 'approved_amount' columns based on 'state'.
        - Converts 'trans_date' to month format.
        - Groups by 'trans_date' and 'country', aggregating transaction metrics.
        - Renames columns for clarity.

    Example:
        monthly_summary = monthly_transactions(transactions_df)
    '''
    # Add columns based on 'state' using assign for clarity
    transactions = transactions.assign(
        approved_count=np.where(transactions["state"] == "approved", 1, 0),
        approved_amount=np.where(
            transactions["state"] == "approved", transactions["amount"], 0
        ),
    )

    # Convert 'trans_date' to month format
    transactions["trans_date"] = transactions["trans_date"].dt.strftime("%Y-%m")

    # Group by 'trans_date' and 'country', aggregate transaction metrics
    monthly_summary = (
        transactions.groupby(["trans_date", "country"], sort=False, dropna=False)
        .agg(
            {
                "state": "count",
                "approved_count": "sum",
                "amount": "sum",
                "approved_amount": "sum",
            }
        )
        .reset_index()
    )

    # Rename columns for clarity
    monthly_summary.columns = [
        "month",
        "country",
        "trans_count",
        "approved_count",
        "trans_total_amount",
        "approved_total_amount",
    ]

    return monthly_summary

def monthly_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    # fmt: off
    return (
        transactions
        .assign(
            month=lambda frame: frame.trans_date.apply(lambda date: date.strftime("%Y-%m")),
            approved=lambda frame: frame[["state", "amount"]].apply(
                lambda row: row.amount if row.state == "approved" else None,
                axis=1,
            ),
        )
        .drop(columns=["trans_date"])
        .groupby(["month", "country"])
        .agg(
            trans_count=("state", "count"),
            approved_count=("approved", "count"),
            trans_total_amount=("amount", "sum"),
            approved_total_amount=("approved", "sum"),
        )
        .reset_index()
    )
    # fmt: on

