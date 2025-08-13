

from pyspark.sql import DataFrame
from typing import List

def check_missing_values(df: DataFrame, columns: List[str]) -> None:
    for col_name in columns:
        missing_count = df.filter(df[col_name].isNull()).count()
        if missing_count > 0:
            print(f"Column {col_name} has {missing_count} missing values.")

def check_value_ranges(df: DataFrame, column: str, min_value, max_value) -> None:
    out_of_range = df.filter((df[column] < min_value) | (df[column] > max_value)).count()
    if out_of_range > 0:
        print(f"Column {column} has {out_of_range} values out of range [{min_value}, {max_value}].")

def check_unique(df: DataFrame, column: str) -> None:
    total = df.count()
    unique = df.select(column).distinct().count()
    if total != unique:
        print(f"Column {column} is not unique. {total - unique} duplicates found.")
