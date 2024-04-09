import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("hotel_dataset.csv")

print(df)

host = 'localhost'
port = '5432'
database = 'makemytrip'
user = 'postgres'
password = '0409'


engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
print("database connected")

df.to_sql(name='hotel_dataset', con=engine, if_exists='replace', index=False)
print("uploaded to database")

engine.dispose()
