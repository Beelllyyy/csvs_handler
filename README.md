# csvs_handler
Part of my reusable code directory. Comes in handy when dealing with import of multiple csv files in data analysis / data science projects.

Situation: You are preparing your data. Your dataset is split into several csv files. You are about to import them and concatenate them into a single comprehensive dataframe.
Problem: you want to make sure the datasets have consistent data.
Solution: assembled reusable functions that:
  [1] load all csv files from a given directory and store them as dataframes into a dictionary
  [2] check if all dataframes have same column names in same position, return mismatching values.
  [3] check if all dataframes' columns have consistent data types, returning mismatching values.
