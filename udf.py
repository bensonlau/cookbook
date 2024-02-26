#################################################################################For useful and/or commonly used user-defined functions#########
########################################################################

#######Pandas Data Frames########
import pandas as pd
def get_df_remove_empty_col(df) -> pd.DataFrame:
  #removing columns that are all 0's
  d = df[[column for column in df if int(df[column].max()) != 0]]
  for c in df.columns:
    if c not in d.columns:
        print(c, end=", ")
  return d

print("Loaded get_df_remove_empty_col(df) -> pd.DataFrame")


import pandas as pd
def get_df_remove_x_missing_values(df,perc_missing=0.3) -> pd.DataFrame:
  df2 = df[[column for column in df if df[column].count() / len(df) >= perc_missing]]
  print("List of dropped columns:", end=" ")
  for c in df.columns:
      if c not in df2.columns:
          print(c, end=", ")
  print('\n')
  df = df2

print("Loaded: get_df_remove_empty_col(df,perc_missing=0.3) -> pd.DataFrame")

'''
Tabulates columns by data types
'''
import pandas as pd
def count_column_types(sparkDf):
    return pd.DataFrame(sparkDf.dtypes).groupby(1, as_index=False)[0].agg({'count':'count', 'names': lambda x: " | ".join(set(x))}).rename(columns={1:"type"})
  
print("count_column_types(sparkDf) loaded")
print("""Count number of columns per type""")

# Counting Column Length
def invalid_column_length(tweets: pd.DataFrame) -> pd.DataFrame:
    min = 15
    is_valid = tweets['content'].str.len() > min
    df = tweets[is_valid]
    return df[['tweet_id']]

def invalid_column_length(tweets: pd.DataFrame) -> pd.DataFrame:
    # Filter rows where the length of 'content' is strictly greater than 15
    invalid_tweets_df = tweets[tweets['content'].str.len() > 15]
    
    # Select only the 'tweet_id' column from the invalid tweets DataFrame
    result_df = invalid_tweets_df[['tweet_id']]
    
    return result_df

# Updating columns names
import pandas as pd
def fix_names(users: pd.DataFrame) -> pd.DataFrame:
    users["name"] = users["name"].str[0].str.upper() + users["name"].str[1:].str.lower()
    return users.sort_values("user_id")

def fix_names(users: pd.DataFrame) -> pd.DataFrame:
    users["name"] = users["name"].str.capitalize()
    return users.sort_values("user_id")

# Filtering and Subselecting
import pandas as pd
def find_products(products: pd.DataFrame) -> pd.DataFrame:
    filtered_df = products[(products.low_fats =='Y') & (products.recyclable =='Y')]
    result = filtered_df[['product_id']] 
    return result

####Single Line Approach
import pandas as pd
def find_products(products: pd.DataFrame) -> pd.DataFrame:
    return products[(products.low_fats =='Y') & (products.recyclable =='Y')][['product_id']] 

####Including Null/NA values
 def find_customer_referee(customer: pd.DataFrame) -> pd.DataFrame:
    return customer[ (customer['referee_id']!=2) | customer['referee_id'].isna()][['name']]

####...Sorting the Data Frame
def not_boring_movies(cinema: pd.DataFrame) -> pd.DataFrame:
    return cinema[(cinema.description != 'boring') & (cinema.id % 2 == 1) ].sort_values(by='rating',ascending = False)

# Printing the percentage of missing values per column
import pandas as pd
def get_percent_missing(df:pd.DataFrame, verbose = False) -> pd.DataFrame:
  '''
  Derives the percentage of missing values for each column in a dataframe

  Args:
      df: pandas dataframe

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

# Counting columns conditionally
import pandas as pd
import numpy as np
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
        monthly_summary = monthly_transactions(transactions)
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

# Counting columns conditionally
def monthly_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
	 '''
    Calculate monthly transactions summary.

    Args:
        transactions (pd.DataFrame): Input DataFrame containing transaction data.

    Returns:
        pd.DataFrame: Monthly transactions summary.

    Example:
        monthly_summary = monthly_transactions(transactions)
    '''
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

# Calculating Ratio or Weighted Metrics
def confirmation_rate(signups: pd.DataFrame, confirmations: pd.DataFrame) -> pd.DataFrame:
	'''
    Calculate ratio metrics.

    Args:
        signups (pd.DataFrame): Input DataFrame containing transaction
        confirmations (pd.DataFrame): Input DataFrame containing sign-ups

        confirmations: Input dataframe containing the confirmations received

    Returns:
        pd.DataFrame: Monthly transactions summary.

    Example:
        monthly_summary = monthly_transactions(transactions)
    '''
	return (
	    pd.merge(signups[["user_id"]], confirmations[["action", "user_id"]], on="user_id", how="left")
	    .assign(confirmation_rate=lambda frame: frame.action.apply(lambda action: action == "confirmed"))
	    .groupby("user_id", as_index=False)
	    .confirmation_rate
	    .mean()
	    .round(2)
	)

# Calculating Ratio or Weighted Metric Conditionally Using Self-Joins
import pandas as pd
def gameplay_analysis(activity: pd.DataFrame) -> pd.DataFrame:
	# For each player ('player_id'), derive earliest log-in date
   first_login = activity.groupby('player_id')['event_date'].min().reset_index()

   activity['day_before_event'] = activity['event_date'] - pd.to_timedelta(1, unit='D')

   merged_df = activity.merge(first_login, on='player_id', suffixes=('_actual', '_first'))

   consecutive_login = merged_df[merged_df['day_before_event'] == merged_df['event_date_first']]

   fraction = round(consecutive_login['player_id'].nunique() / activity['player_id'].nunique(),2)

	output_df = pd.DataFrame({'fraction': [fraction]})
	return output_df

# Calculating rank of columns
import pandas as pd
def top_three_salaries(employee: pd.DataFrame, department: pd.DataFrame) -> pd.DataFrame:
    
    Employee_Department = employee.merge(department, left_on='departmentId', right_on='id').rename(columns = {'name_y': 'Department'})

    Employee_Department = Employee_Department[['Department', 'departmentId', 'salary']].drop_duplicates()
    
    top_salary = Employee_Department.groupby(['Department', 'departmentId']).salary.nlargest(3).reset_index()
    
    df = top_salary.merge(employee, on=['departmentId', 'salary'])
    
    return df[['Department', 'name', 'salary']].rename(columns = {'name': 'Employee', 'salary': 'Salary'})

#######Pyspark Data Frames########
# Deriving dataframe dimensions
from pyspark.sql import DataFrame
def pyspark_df_shape(df: DataFrame):
	'''
	Derive the dimensions of a pyspark data frame.
	
	Args:
		df: pyspark dataframe 
	
	Notes:
		Using count() action to get the number of rows on DataFrame 
		and len(df.columns()) to get the number of columns.
	'''

	# extracting number of distinct rows from the Dataframe
	row = df.distinct().count()

	# extracting total number of rows from the Dataframe
	all_rows = df.count()

	# extracting number of columns from the Dataframe
	col = len(df.columns)

	print(f'Dimensions of the Dataframe are: {(row,col)}')
	print(f'Distinct Number of Rows are: {row}')
	print(f'Total Number of Rows are: {all_rows}')
	print(f'Number of Columns are: {col}')

# Summarizing data frame dimensions
def spark_shape(sparkDf):
  print(sparkDf.count(), len(sparkDf.columns))

print("spark_shape(sparkDf) loaded")
print("Using count()  to get the number of rows on DataFrame and len(df.columns()) to get the number of columns.")
