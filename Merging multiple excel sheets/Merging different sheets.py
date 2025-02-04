import glob
import pandas as pd

# Get all Excel files in the folder
files = glob.glob("data/*.xlsx")  # Assumes files are in a 'data' folder

all_data = []

for file in files:
    df = pd.read_excel(file, engine="openpyxl")
    all_data.append(df)

# Merge all data
merged_df = pd.concat(all_data, ignore_index=True)

# Save merged file
merged_df.to_excel("merged_sales_data.xlsx", index=False)

print("Merged file saved as 'merged_sales_data.xlsx'.")
