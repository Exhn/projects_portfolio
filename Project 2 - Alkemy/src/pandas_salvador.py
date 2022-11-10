"""Python function for pandas processing for universidad del salvador"""
import pandas as pd
from pathlib import Path

filePath = Path(__file__).parent.parent.parent / "destFolder" / "Universidad del Salvador.csv"
postalPath = Path(__file__).parent.parent.parent / "postal_codes" / "codigos_postales.csv"
destPath = Path(__file__).parent.parent.parent / "destFolder" / "universidad_del_salvador.txt"


def pandas_processing_salvador():
    """Python function for pandas processing for universidad del salvador"""
    with open(filePath, 'r') as f:
        df = pd.read_csv(f, encoding='latin-1')
        df = df.astype({'age': 'int'})
        df['university'] = df['university'].str.replace('_', ' ').str.lower().str.strip()
        df['career'] = df['career'].str.replace('_', ' ').str.lower().str.strip()
        df['first_name'] = df['first_name'].str.replace('_', ' ').str.lower()
        df['last_name'] = df['last_name'].str.replace('_', ' ').str.lower()
        df.loc[df['gender'] == 'M', 'gender'] = 'male'
        df.loc[df['gender'] == 'F', 'gender'] = 'female'
        df['location'] = df['location'].str.lower()
        df['email'] = df['email'].str.replace('_', ' ').str.lower()

    with open(postalPath, 'r') as cp:
        dfpostal_code = pd.read_csv(cp, encoding='latin-1')
        dfpostal_code.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace=True)
        dfpostal_code = dfpostal_code.astype({'postal_code': 'str'})
        dfpostal_code['location'] = dfpostal_code['location'].str.lower()

    df = df.merge(dfpostal_code, on='location', how='left')
    df.to_csv(destPath, header=None, index=None, sep=' ', mode='a')
