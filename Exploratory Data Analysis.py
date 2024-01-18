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

##### Pandas #####
import pandas

# Printing the percentage of missing values per column
def percent_missing(df:pd.DataFrame)->str:
    '''
    Derives the percentage of missing values for each column in a dataframe

    Args:
        df
    '''
    # Summing the number of missing values per column and then dividing by the total rows
    sumMissing = df.isnull().values.sum(axis=0)
    pctMissing = sumMissing / df.shape[0]
def percent_missing(df:pd.DataFrame) -> pd.DataFrame:
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
    print ('{0}: {1:.2f}%'.format(col, pctMissing[idx] * 100))

  # Create dataframe
  df_results = pd.DataFrame({'Columns': col_names_list, "Percent Missing": pct_missing_list})
  return df_results
    
    if sumMissing.sum() == 0:
        print('No missing values')
    else:
        # Looping through and printing out each columns missing value percentage
        print('Percent Missing Values:', '\n')
        for idx, col in enumerate(dataframe.columns):
        for idx, col in enumerate(df.columns):
            if sumMissing[idx] > 0:
                print('{0}: {1:.2f}%'.format(col, pctMissing[idx] * 100))

print("Number of Rows: ", rows)
print("Number of Columns: ", cols)

        
df.head()
df["column_name"].mean()
df["column_name"].median()
df["column_name"].describe()        .reset_index()
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

