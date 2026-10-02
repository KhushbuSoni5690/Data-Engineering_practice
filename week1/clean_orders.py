import pandas as pd

df = pd.read_csv("C:\Monash\My Practice projects\Data Engineer Learning\Data-Engineering_practice\data\orders_raw.csv")

print(df)

# below is the data cleaning 
# remove duplicates
cleaned_df = df.drop_duplicates(subset=["order_id"])

# naming convention
cleaned_df['customer_name'] = cleaned_df['customer_name'].str.strip().str.title()
# cleaned_df['customer_names'] = cleaned_df['customer_names'].str.strip().str.title()

#date format globalisation
cleaned_df['order_date'] = pd.to_datetime(cleaned_df['order_date'], format='mixed', dayfirst=True)

#amount $ sign remove
# Complete workflow hint:
# df['amount'] = df['amount'].str.replace('$', '', regex=False).astype(float).round(2)
cleaned_df['amount'] = cleaned_df['amount'].str.replace('$', '', regex = False).astype(float).round(2)

# status lower case
cleaned_df['status'] = cleaned_df['status'].str.strip().str.lower()

# Run this print statement at the end of your script to verify:
print(cleaned_df['status'].unique())

# missing cities replaced with unknown for clarity
cleaned_df['city'] = cleaned_df['city'].fillna('unknown')

#missing amount will be replaced by avg og the amount.
avg_amount = cleaned_df['amount'].mean()
avg_amount_rounded = round(avg_amount, 2) 
cleaned_df['amount'] = cleaned_df['amount'].fillna(avg_amount_rounded)

print(cleaned_df)



