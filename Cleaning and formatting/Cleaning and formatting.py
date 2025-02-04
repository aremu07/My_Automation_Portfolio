import pandas as pd

# Launch the Excel File
df = pd.read_excel("merged_sales_data.xlsx", engine="openpyxl")

# Convert column names to lowercase
df.columns = df.columns.str.lower()

# Fill missing values with 0 (so that you preserve rows with partial missing data)
df.fillna(0, inplace=True)

# If you want to drop rows that are completely empty, do so:
df.dropna(how="all", inplace=True)

# Display the first few rows
print(df.head())

# Calculate profit
df["profit"] = df["revenue"] - df["cost"]
print(df[["revenue", "cost", "profit"]].head())

# Create summary DataFrame
summary = pd.DataFrame({
    "Total Revenue": [df["revenue"].sum()],
    "Total Cost": [df["cost"].sum()],
    "Total Profit": [df["profit"].sum()]
})

# Save to a new Excel file
with pd.ExcelWriter("final_report.xlsx", engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Cleaned Data", index=False)
    summary.to_excel(writer, sheet_name="Summary Report", index=False)

print("Excel report generated successfully!")

