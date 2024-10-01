import re
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import networkx as nx
import difflib

# Load the data
data_df = pd.read_csv('/Users/ephraimmeiri/gitEtc/Rashi Thesis work/normalized_3.csv',header=0,index_col="Name")
order_df = pd.read_csv('/Users/ephraimmeiri/gitEtc/Rashi Thesis work/ordered_pairs.csv',header=0)

# Remove unwanted columns if they exist
columns_to_drop = ["שם", '# other alternatives']
data_df = data_df.drop(columns=[col for col in columns_to_drop if col in data_df.columns])

def replace_quotes(text):
    if not isinstance(text, str):
        return text
    return text.replace('"', "׳׳").replace('"', "׳׳").strip()


# Apply the replacement function to both columns
# order_df["Early"] = order_df["Early"].apply(replace_quotes)
# order_df["Late"] = order_df["Late"].apply(replace_quotes)

# Create a new DataFrame for results
result_df = order_df.copy()

# Calculate ratios
for name in data_df.columns:
    early_values = data_df.loc[order_df['E2'], name].values
    late_values = data_df.loc[order_df['L2'], name].values

    # Avoid division by zero and handle NaN values
    with np.errstate(divide='ignore', invalid='ignore'):
        ratios = np.where((late_values != 0) & ~np.isnan(late_values),
                          early_values / late_values, np.nan)

    result_df[name] = ratios

result_df.to_excel('/Users/ephraimmeiri/Documents/רשי research docs/Writing/ratios3.xlsx')