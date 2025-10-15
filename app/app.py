import pandas as pd
import requests
import os
from sqlalchemy import create_engine

if __name__ == "__main__":
    

query = "SELECT * FROM cleaned_df"
pd.read_sql(query, engine)