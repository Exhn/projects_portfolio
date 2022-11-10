"""Python function for pandas processing for universidad nacional del comahue"""
import pandas as pd
from pathlib import Path

filePath = Path(__file__).parent.parent.parent / "destFolder" / "Universidad Nacional del Comahue.csv"
postalPath = Path(__file__).parent.parent.parent / "postal_codes" / "codigos_postales.csv"
destPath = Path(__file__).parent.parent.parent / "destFolder" / "universidad_del_comahue.txt"


def pandas_processing_comahue():
    """Python function for pandas processing for universidad nacional del comahue"""
    with open(filePath, 'r') as f:
        df = pd.read_csv(f, encoding='latin-1')
        df = df.astype({'age': 'int'})
        df = df.astype({'postal_code': 'str'})
        df['university'] = df['university'].str.lower()
        df['career'] = df['career'].str.lower()
        df['first_name'] = df['first_name'].str.lower()
        df['last_name'] = df['last_name'].str.lower()
        df.loc[df['gender'] == 'M', 'gender'] = 'male'
        df.loc[df['gender'] == 'F', 'gender'] = 'female'
        df['email'] = df['email'].str.lower()

    with open(postalPath, 'r') as cp:
        dfpostal_code = pd.read_csv(cp, encoding='latin-1')
        dfpostal_code.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace=True)
        dfpostal_code['location'] = dfpostal_code['location'].str.lower()
        dfpostal_code = dfpostal_code.astype({'postal_code': 'str'})

    df = df.merge(dfpostal_code, on='postal_code', how='left')
    df.to_csv(destPath, header=None, index=None, sep=' ', mode='a')
