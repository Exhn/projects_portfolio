"""Python script for quering DataBase for universidad del comahue"""
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

# Get the path to the .env file
envFile = Path(__file__).parent.parent.parent / "src" / "py_functions" / "vars.env"

# Loading variables from dotenv
load_dotenv(envFile)

postgres_conn_id = os.getenv('postgres_conn_id')
path = Path(__file__).parent.parent.parent / "src" / "sql" / "univ_comahue.sql"
csvPath = Path(__file__).parent.parent.parent / "destFolder" / "Universidad Nacional del Comahue.csv"


def querycomahue():
    """Python function to download query from directory and querying
    the Postgres database for Universidad del Comahue"""
    engine = create_engine(postgres_conn_id)
    query = open(path, 'r')
    result = pd.read_sql(query.read(), engine)
    result.to_csv(csvPath, index=False)
