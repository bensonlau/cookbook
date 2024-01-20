#################################################################################For useful and/or commonly used user-defined functions#########
########################################################################

###Pandas Data Frames###
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