import numpy as np
import pandas as pd
import glob


# Given a directory, returns a dictionary containing all CSV(s) files as panda dataframes.
# {<filename> : <data>, ...}
def csv_grabber(path):
    directory = glob.glob(path + '/*.csv')
    list_of_csv = {}
    for filename in directory:
        list_of_csv[filename.split("\\")[-1]] = pd.read_csv(filename, index_col=None, header=0)  # works only if first row is header;
        print('Imported {}'.format(filename.split('\\')[-1]))
    print('[ Imported {} files ]'.format(len(list_of_csv)))
    return list_of_csv


# Takes in a dictionary of dataframes. If number of columns is the same for each df, returns a table (df) of all column names.
# -> Return df of all column names
def get_col_names(dict_of_csv):
    list_n_cols = [len(dict_of_csv[df].columns) for df in list(dict_of_csv.keys())]
    if all(n == list_n_cols[0] for n in list_n_cols):
        return pd.DataFrame([dict_of_csv[df].columns for df in list(dict_of_csv.keys())],
                            columns=[f'col_{n}' for n in range(list_n_cols[0])],
                            index=list(dict_of_csv.keys()))
    else:
        print('Dataframes do not have same number of columns')


# Given the dataframe of colnames, check if all dataframes have same column names.
# -> Returns boolean (found at least one mismatching name?)
def check_col_names(df_of_colnames, returning=False):
    mismatch_somewhere = False
    for col in df_of_colnames:
        if not df_of_colnames[col].eq(df_of_colnames[col][0]).all():
            print('{} has some mismatching colnames.'.format(col))
            mismatch_somewhere = True
    if not mismatch_somewhere:
        print('All column names are matching')
    if returning:
        return mismatch_somewhere


# Takes in a list of dataframes. If number of columns is the same for each df, returns a table (df) of all column datatype.
# -> Return df of all column names
def get_col_types(dict_of_csv):
    list_n_cols = [len(dict_of_csv[df].columns) for df in list(dict_of_csv.keys())]
    if all(n == list_n_cols[0] for n in list_n_cols):
        return pd.DataFrame([list(dict_of_csv[df].dtypes) for df in list(dict_of_csv.keys())],
                            columns=[f'col_{n}' for n in range(list_n_cols[0])],
                            index=list(dict_of_csv.keys()))


# Given the dataframe of column-datatypes, check if all dataframes have same column data types.
# -> Returns boolean (found at least one mismatching type?)
def check_col_types(df_of_coltypes, returning=False):
    mismatch_somewhere = False
    for col in df_of_coltypes:
        if not df_of_coltypes[col].eq(df_of_coltypes[col][0]).all():
            print('[ ] {} has these data types:'.format(col))
            print(" ".join(str(datatype) for datatype in df_of_coltypes[col].unique()))
            mismatch_somewhere = True
    if not mismatch_somewhere:
        print('All column datatypes are matching')
    if returning:
        return mismatch_somewhere


# Takes in a dictionary of dataframes to be concatenated
# -> Returns concatenated dataframe
def concatenate_csv(dict_of_csv, index=""):
    df = dict_of_csv[list(dict_of_csv.keys())[0]]
    for file in list(dict_of_csv.keys())[1:]:
        df = df.append(dict_of_csv[file])
    if index:
        df.set_index(index, inplace=True)
    return df


# Controls for duplicate rows
# -> Returns dataframe containing only duplicate rows
def find_duplicate_rows(df, returning=False, printing=True):
    df_dupl = df[df.duplicated()]
    if printing:
        if len(df_dupl):
            print(df_dupl)
        else:
            print("No duplicate rows detected.")
    if returning:
        return df_dupl


# Controls for rows with duplicate index
# Returns dataframe containing index-duplicate rows
def check_index_duplicates(df, returning=False, printing=True):
    df_dupl = df.loc[df.index.duplicated()]
    if printing:
        if len(df_dupl):
            print(df_dupl)
        else:
            print("No rows with duplicate index were detected.")
    if returning:
        return df_dupl


# -> Returns number of rows with at least one missing value
def count_missing_values(df, returning=False, printing=True):
    df_missing = df[df.isnull().any(axis=1)]
    missing_perc = len(df_missing) / len(df)
    if printing:
        print('{}% of rows have at least one missing value'.format(round(missing_perc * 100, 1)))
    if returning:
        return missing_perc


# Controls for rows with missing values
# -> Returns dataframe with only rows with at least one missing value
def find_row_missing_values(df, returning=True, printing=False):
    df_missing = df[df.isnull().any(axis=1)]
    if printing:
        if len(df_missing):
            print(df_missing)
        else:
            print("No rows with missing values detected")
    if returning:
        return df_missing


# -> Returns dataframe with count of missing values (number and %) per column
# Arg: show_all - false: only return columns with missing values
def summarize_col_missing_values(df, show_all=False):
    n_values = len(df)
    df_missing = pd.DataFrame(index=['n_missing', 'perc_missing'])
    for col in df.columns:
        n_missing = len(df[df[col].isnull()])
        if n_missing == "NaN":
            n_missing = 0
        if n_missing | show_all:
            perc_missing = round(n_missing / n_values * 100, 1)
            df_missing[col] = [n_missing, perc_missing]
    return df_missing


# Checks dataframe columns. If data (string) corresponds to format,
# -> Returns dataframe with datetime columns converted from string into datetime dtype
def datetime_autoconvert(df):
    data = [
        pd.to_datetime(df[col])
        if df[col].astype(str).str.match(r'\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2}').all()
        else df[col]
        for col in df.columns]

    df = pd.concat(data, axis=1, keys=[s.name for s in data])
    return df


# Lighter version of datetime_autoconverter. Checks only first element of column to determin if it should be converted to datetime.
# If value is missing, keeps looking for first available value in same column.
def datetime_autoconverter_lite(df):
    for col in df.columns:
        for index_row in range(len(df)):
            # need to control 2 cells in order to return series and not just a string. Series has str.match method, easier to manage.
            cell = df[col][index_row: index_row + 1]
            if not any(cell.isna()):
                if cell.astype(str).str.match(r'\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2}').all():
                    df[col] = pd.to_datetime(df[col])
                break
    return df


def summarize_datetime(df):
    dt_cols = df.select_dtypes([np.datetime64, 'datetime', 'datetime64', 'datetimetz', 'datetimetz'])
    summary_df = pd.DataFrame(index=dt_cols.columns)
    summary_df['min'] = dt_cols.min()
    summary_df['max'] = dt_cols.max()
    summary_df['time_span'] = dt_cols.max() - dt_cols.min()
    return summary_df


def count_by_weekday(df):
    dt_cols = df.select_dtypes([np.datetime64, 'datetime', 'datetime64', 'datetimetz', 'datetimetz'])
    summary_df = pd.DataFrame(columns=dt_cols.columns)
    for col in summary_df.columns:
        summary_df[col] = dt_cols[col].groupby(pd.DatetimeIndex(dt_cols[col]).dayofweek).agg('count')
    summary_df.index.rename('weekday', inplace=True)
    return summary_df


def count_by_day(df):
    dt_cols = df.select_dtypes([np.datetime64, 'datetime', 'datetime64', 'datetimetz', 'datetimetz'])
    summary_df = pd.DataFrame(columns=dt_cols.columns)
    for col in summary_df.columns:
        summary_df[col] = dt_cols[col].groupby(pd.DatetimeIndex(dt_cols[col]).day).agg('count')
    summary_df.index.rename('day_of_month', inplace=True)
    return summary_df


def count_by_month(df):
    # Retrieve datetime columns
    dt_cols = df.select_dtypes([np.datetime64, 'datetime', 'datetime64', 'datetimetz', 'datetimetz'])
    summary_df = pd.DataFrame(columns=dt_cols.columns)
    for col in summary_df.columns:
        summary_df[col] = dt_cols[col].groupby(pd.DatetimeIndex(dt_cols[col]).month).agg('count')
    summary_df.index.rename('month', inplace=True)
    return summary_df


def summarize_numeric(df):
    numeric_cols = df.select_dtypes([np.number, 'number'])
    summary_df = pd.DataFrame(index=numeric_cols.columns)
    summary_df['min'] = numeric_cols.min()
    summary_df['1st_Qu'] = numeric_cols.quantile(0.25)
    summary_df['median'] = numeric_cols.median()
    summary_df['mean'] = numeric_cols.mean()
    summary_df['3rd_Qu'] = numeric_cols.quantile(0.75)
    summary_df['max'] = numeric_cols.max()
    summary_df['range'] = numeric_cols.max() - numeric_cols.min()
    summary_df['std'] = numeric_cols.std()
    return summary_df


def summarize_categorical(df):
    df_categ = df.select_dtypes('category')
    summary_df = pd.DataFrame(index=df_categ.columns, columns=['n_missing', 'complete_rate', 'n_unique'])
    for categ in summary_df.index:
        n_missing = df[categ].isna().sum()
        complete_rate = round(1 - (n_missing / len(df[categ])), 2)
        n_unique = len(df[categ].unique())
        summary_df.at[categ, 'n_missing'] = n_missing
        summary_df.at[categ, 'complete_rate'] = complete_rate
        summary_df.at[categ, 'n_unique'] = n_unique
    print(summary_df)

    print('\nMost frequent cagetories:')
    for i, categ in enumerate(summary_df.index):
        print('\n[{}] {}'.format(i, categ))
        print(df_categ[categ].value_counts()[:5])


def summary_report(df):

    print(" ******* SUMMARY REPORT ******* ")
    print("\nHere's a glimpse of your data:\n")
    print(df.head(5))

    # First manually ask to detect variables that should be treated as categorical
    dict_of_cols = dict(zip(list(range(len(df.columns))), df.columns))
    print(dict_of_cols)
    categorical_cols = input('\nAbove you can find a glimpse of your data and a numbered list of columns.\n'
                             'Write here the column numbers that you want to treat as CATEGORICAL variables:\n'
                             'Separate by comma, e.g.: 1, 4, 7')
    categorical_cols = list(map(lambda x: int(x), categorical_cols.split(',')))
    categorical_cols = [dict_of_cols[x] for x in categorical_cols]
    for col in categorical_cols:
        df[col] = pd.Categorical(df[col])

    # Dataframe shape
    print('\nGreat, now your dataset looks like this:')
    [n_obs, n_cols] = df.shape
    print('\n___DATASET SHAPE AND DTYPES___________________________________')
    print('Dataframe contains data of {} variables for {} observations'. format(n_cols, n_obs))

    # Dataframe columns, dtypes and missing values
    missing = summarize_col_missing_values(df, show_all=True).transpose()
    #   Initialize table
    vars_table = pd.DataFrame(index=df.columns)
    #   Merge in series dtypes
    vars_table = vars_table.merge(df.dtypes.rename('dtype'), left_index=True, right_index=True)
    #   Join in dataframe with missing values and % of missing values
    vars_table = vars_table.join(missing, how="left", on=vars_table.index)

    print('Columns have the following data types:\n')
    print(vars_table)

    print('\n___SUMMARY STATISTICS__________________________________________')
    # Datetime fields statistics
    # Min date, max date, Timespan,
    dt_cols = df.select_dtypes([np.datetime64, 'datetime', 'datetime64', 'datetimetz', 'datetimetz'])
    print('--------------- Datetime variables ---------------')
    print('Datetime fields: {}'.format(dt_cols.columns))
    print('[1] Summary:')
    print(summarize_datetime(dt_cols))
    print('[2] Count of obs per month:')
    print(count_by_month(dt_cols))
    print('[3] Count of obs per day of the month:')
    print(count_by_day(dt_cols))
    print('[4] Count of obs per day of the week (mon=0, sun=6):')
    print(count_by_weekday(dt_cols))

    # Numeric fields statistics
    print('--------------- Numeric variables ---------------')
    print(summarize_numeric(df))

    # Categorical data
    print('--------------- Categorical variables ---------------')
    summarize_categorical(df)
