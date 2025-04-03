print(
    "################################################################################"
)
print("Use standard python libraries to do the transformations")
print(
    "################################################################################"
)

# Question: How do you read data from a CSV file at ./data/sample_data.csv into a list of dictionaries?

import csv

data=[]
with open("./data/sample_data.csv", 'r', encoding="utf-8") as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        data.append(row)    

# Question: How do you remove duplicate rows based on customer ID?
unique_id_data = []
seen_id = set()
for row in data:
    if row['Customer_ID'] not in seen_id:
        seen_id.add(row['Customer_ID'])
        unique_id_data.append(row)
len(data)
len(unique_id_data)


# Question: How do you handle missing values by replacing them with 0?
unique_id_data[0]["Customer_ID"]

for row in unique_id_data:
    if not row['Age']:
        print(f"Customer name {row[Customer_Name]} doesn't has the age specified")
        row['Age']=0
    if not row['Purchase_Amount']:
        print(f"Customer {row[Customer_Name]} doesn't has the amount specified")
        row['Purchase_Amount']=0.0


# Question: How do you remove outliers such as age > 100 or purchase amount > 1000?
for row in unique_id_data:
    if int(row['Age'])>100:
        print(f"Customer name {row[Customer_Name]} has {row['Age']} years")
        row['Age']=0
    if float(row['Purchase_Amount'])>1000:
        print(f"Customer {row[Customer_Name]} has {row['Purchase_Amount']} purchase")
        row['Purchase_Amount']=0.0

#or simplie filter

data_cleaned = [
    row
    for row in unique_id_data
    if int(row['Age'])<=100 and float(row['Purchase_Amount'])<=1000
]

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male)?


data_cleaned[1]

for row in data_cleaned:
    row['Gender'] = 0 if row['Gender'] == 'Female' else  1      

data_cleaned[1]
  

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns?

for row in data_cleaned:
    name_parts = row['Customer_Name'].split()
    row['First_Name'] = name_parts[0]
    row['Last_Name'] = name_parts[1]
    del row["Customer_Name"]

# Question: How do you calculate the total purchase amount by Gender?
fem_purch=0
mal_purch=0
for row in data_cleaned:
    if row['Gender']==0: 
        fem_purch+=float(row['Purchase_Amount']) 
    if row['Gender']==1:
        mal_purch+=float(row['Purchase_Amount'])
fem_purch
mal_purch

#or
total_purchase_by_gender = {}

for row in data_cleaned:
    gender = row["Gender"]
    amount = float(row["Purchase_Amount"])
    total_purchase_by_gender[gender] = total_purchase_by_gender.get(gender, 0) + amount
total_purchase_by_gender


# Question: How do you calculate the average purchase amount by Age group?
# assume age_groups is the grouping we want
# hint: Why do we convert to float?
age_groups = {"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}

for row in data_cleaned:
    row['Purchase_Amount'] = float(row['Purchase_Amount'])
data_cleaned[1]

for group in age_groups.keys():
    for row in data_cleaned:
        if row['Age']>=float(group.split('-')[0]) and row['Age']<float(group.split('-')[1]):
            age_groups[group].append(row['Purchase_Amount'])
age_groups        

average_purchase_by_age_group = {
    group: (sum(amounts)/len(amounts)) if amounts else 0
    for group, amounts in age_groups.items()
}

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group?
your_total_purchase_amount_by_gender = total_purchase_by_gender # your results should be assigned to this variable
average_purchase_by_age_group = {} # your results should be assigned to this variable

print(f"Total purchase amount by Gender: {your_total_purchase_amount_by_gender}")
print(f"Average purchase amount by Age group: {average_purchase_by_age_group}")

print(
    "################################################################################"
)
print("Use DuckDB to do the transformations")
print(
    "################################################################################"
)

# Question: How do you connect to DuckDB and load data from a CSV file into a DuckDB table?
# Connect to DuckDB and load data
import duckdb
duckdb_con = duckdb.connect('duckdb.db')
duckdb_con.close()

con = duckdb.connect(database=":memory:", read_only=False)
con.execute("CREATE TABLE data (Customer_ID INTEGER, Customer_Name VARCHAR, Age INTEGER,Gender VARCHAR, Purchase_Amount FLOAT,Purchase_Date DATE)").fetchall()
con.execute("DROP TABLE data").fetchall()
# Read data from CSV file into DuckDB table

con.execute("COPY data FROM './data/sample_data.csv' WITH HEADER CSV")
con.execute("SELECT * FROM data").fetchall()
con.commit()
# Question: How do you remove duplicate rows based on customer ID in DuckDB?

con.execute("CREATE TABLE unique_data AS SELECT DISTINCT * FROM data")

# Question: How do you handle missing values by replacing them with 0 in DuckDB?
con.execute("DESCRIBE unique_data").fetchall()
con.execute("UPDATE unique_data SET Age=0 WHERE Age IS NULL").fetchall()
con.execute("UPDATE unique_data SET Purchase_Amount=0.0 WHERE Purchase_Amount IS NULL").fetchall()
#or
con.execute(
    "CREATE TABLE data_cleaned_missing AS SELECT \
             Customer_ID, Customer_Name, \
             COALESCE(Age, 0) AS Age, \
             Gender, \
             COALESCE(Purchase_Amount, 0.0) AS Purchase_Amount, \
             Purchase_Date \
             FROM unique_data"
).fetchall()
# Question: How do you remove outliers (e.g., age > 100 or purchase amount > 1000) in DuckDB?
con.execute("CREATE TABLE clean_data AS SELECT * FROM unique_data WHERE Purchase_Amount<= 1000 AND Age<=100")
con.execute("SELECT * FROM clean_data").fetchall()
# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male) in DuckDB?
con.execute(
    "CREATE TABLE data_cleaned_gender AS SELECT *, \
             CASE WHEN Gender = 'Female' THEN 0 ELSE 1 END AS Gender_Binary \
             FROM clean_data"
)
# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns in DuckDB?

con.execute(
    "CREATE TABLE data_names AS SELECT  *,\
             SPLIT_PART(Customer_Name, ' ',1) AS First_Name, SPLIT_PART(Customer_Name, ' ',2) AS Last_Name\
             FROM clean_data"
).fetchall()

# Question: How do you calculate the total purchase amount by Gender in DuckDB?
purchase_amount_by_gender = con.execute(
    "SELECT  Gender, SUM(Purchase_Amount)\
    FROM clean_data\
    GROUP BY Gender"
).fetchall()
# Question: How do you calculate the average purchase amount by Age group in DuckDB?
#{"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}
con.execute("SELECT * FROM clean_data LIMIT 1").fetchall()

avg_purchase_by_age_group = con.execute(
    "SELECT CASE WHEN age>=18 AND age<=30 THEN '18-30' \
             WHEN age>=31 AND age<=40 THEN '31-40'\
             WHEN age>=41 AND age<=50 THEN '41-50'\
             WHEN age>=51 AND age<=60 THEN '51-60'\
             WHEN age>=61 AND age<=70 THEN '61-70'\
    ELSE '>70' END AS  age_group,\
    AVG(Purchase_Amount)\
    FROM clean_data\
    GROUP BY age_group"
).fetchall()


# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group in DuckDB?
print("====================== Results ======================")
print(f"""Total purchase amount by Gender: {purchase_amount_by_gender}""")
print(f"""Average purchase amount by Age group: {avg_purchase_by_age_group}""")
