import pandas as pd
import glob


# Given a directory, returns a dictionary containing all CSV(s) files as panda dataframes.
# {<filename> : <data>, ...}
def csv_grabber(path):
    directory = glob.glob(path + '/*.csv')
    list_of_csv = {}
    for filename in directory:
        list_of_csv[filename.split("\\")[-1]] = pd.read_csv(filename, index_col=None, header=0)  # works only if first row is header;
        print('Imported {}'.format(filename))
    return list_of_csv


# Takes in a dictionary of dataframes. If number of columns is the same for each df, returns a table (df) of all column names.
#   get list with number of columns for each dataframe
#   iterate through list and check if element is == to first one ( => all elements of list are equal)
#       return df of all column names
def get_col_names(dict_of_csv):
    list_n_cols = [len(dict_of_csv[df].columns) for df in list(dict_of_csv.keys())]
    if all(n == list_n_cols[0] for n in list_n_cols):
        return pd.DataFrame([dict_of_csv[df].columns for df in list(dict_of_csv.keys())],
                            columns=[f'col_{n}' for n in range(list_n_cols[0])],
                            index=list(dict_of_csv.keys()))


def check_col_names(df_of_colnames):
    mismatch_somewhere = False
    for col in df_of_colnames:
        if not df_of_colnames[col].eq(df_of_colnames[col][0]).all():
            print('{} has some mismatching colnames.'.format(col))
            mismatch_somewhere = True
    if not mismatch_somewhere:
        print('All column names are matching')


# Takes in a list of dataframes. If number of columns is the same for each df, returns a table (df) of all column datatype.
#   get list with number of columns for each dataframe
#   iterate through list and check if element is == to first one ( => all elements of list are equal)
#   return df of all column types
def get_col_types(dict_of_csv):
    list_n_cols = [len(dict_of_csv[df].columns) for df in list(dict_of_csv.keys())]
    if all(n == list_n_cols[0] for n in list_n_cols):
        return pd.DataFrame([list(dict_of_csv[df].dtypes) for df in list(dict_of_csv.keys())],
                            columns=[f'col_{n}' for n in range(list_n_cols[0])],
                            index=list(dict_of_csv.keys()))

