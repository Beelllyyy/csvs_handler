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
def concatenate_csv(dict_of_csv):
    df = dict_of_csv[list(dict_of_csv.keys())[0]]
    for file in list(dict_of_csv.keys())[1:]:
        df = df.append(dict_of_csv[file])
    return df


# Controls for duplicate rows
# -> Returns dataframe containing only duplicate rows
def find_duplicate_rows(df, returning=False, printing=True):
    df_dupl = df[df.duplicated()]
    if len(df_dupl):
        print(df_dupl)
    else:
        print("No duplicate rows detected.")
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


def summary_report(df):
    df = datetime_autoconvert(df)
    # Dataframe size
    n_cols = len(df.columns)
    n_obs = len(df)
    print('Dataframe contains data of {} variables for {} observations'. format(n_cols, n_obs))
    # Dataframe columns, dtypes and missing values
    missing = summarize_col_missing_values(df, show_all=True).transpose()
    vars_table = pd.concat(
        [df.dtypes, missing['n_missing'], missing['perc_missing']],
        axis=1,
    )
    vars_table.columns = ['dtype', 'missing_values', 'missing_values_perc']
    return vars_table
    # Datetime statistics
    # Min date, max date, Timespan,
    dt_cols = df.loc[:, vars_table.dtype == 'datetime64[ns]']


# TODO: continue summary_report function. Last row ATM locates datetime columns. Use that to run summary statistics. Then do the same for numeric columns and string columns.

