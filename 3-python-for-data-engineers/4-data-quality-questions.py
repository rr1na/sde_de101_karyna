import polars as pl
from cuallee import Check, CheckLevel

# Read CSV file into Polars DataFrame
df = pl.read_csv("./data/sample_data.csv")

# Question: Check for Nulls on column Id and 
# that Customer_ID column is unique
df.columns
check = Check()
check.is_complete("Customer_ID")
check.is_unique("Customer_ID")
#validate - qua la regola salvata sopra viene applicata al db
check.validate(df)
df.columns

for column in df.columns:
    print(f"""'{column}': [None],""")
    
extra_row = pl.DataFrame({
'Customer_ID': [None],
'Customer_Name': [None],
'Age': [None],
'Gender': [None],
'Purchase_Amount': [None],
'Purchase_Date': [None]
})


df1 = df.vstack(extra_row).vstack(df.head(1))
check.validate(df1)

# check docs at https://canimus.github.io/cuallee/polars/ on how to define a check and run it.
# you will end up with a dataframe of results, check that the `status` column does not have any "FAIL" in it

