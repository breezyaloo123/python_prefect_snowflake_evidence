import pandas as pd
from sqlalchemy import create_engine
from prefect import flow,task

#Load the raw data
dataframe = pd.read_csv('supermarket_sales.csv')
dataframe.head()


# Snowflake connection details
user = 'BreezyDataAnalyst'
password = 'joxxed-zinsyg-Casje4'
account = 'uqviiry-ir83823'
warehouse = 'your_warehouse'
database = 'SUPERMARKET'
schema = 'PUBLIC'
role = 'ACCOUNTADMIN'

# %%
#Cleaning data
@task
def cleaning_data(df):
    #change the date column to YYYY-MM-DD format
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.strftime('%Y-%m-%d')
    #Change columns names
    df=df.rename(columns={'Invoice ID':'Invoice_ID','Customer type':'Customer_type','Product line':'Product_line','Unit price':'Unit_price','Tax 5%':'Tax_5%','gross margin percentage':'Gross_margin_%',
    'gross income':'gross_income'})
    return df

# %%
#Send data to Snowflake
@task
def send_data_snowflake(df):
    # Create a connection engine
    engine = create_engine(f'snowflake://{user}:{password}@{account}/{database}/{schema}?role={role}')
    # Write the DataFrame to Snowflake
    df.to_sql('Sales', con=engine, index=False, if_exists='replace')
    print("Data sent to Snowflake successfully!")

# %%
# Send the cleaned data to Snowflake

@flow
def data_pipeline():
    #Cleanig data
    df=cleaning_data(dataframe)
    #Send data to snowflake
    send_data_snowflake(df)
    
if __name__ == "__main__":
    data_pipeline.serve(name='Send_data_to_Snowflake',
        # Run every weekday at 7 AM
        rrule="FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=7;BYMINUTE=0"
        #interval=60
        )


