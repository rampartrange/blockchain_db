import pandas as pd
from pymongo import MongoClient

titanic_df = pd.read_csv('titanic.csv')

client = MongoClient("mongodb://root:example@localhost:27017/")
db = client['titanic_db']  # Create or use an existing database
collection = db['passengers']  # Create or use an existing collection

records = titanic_df.to_dict(orient='records')
collection.insert_many(records)

print("Dataset imported successfully to MongoDB.")
