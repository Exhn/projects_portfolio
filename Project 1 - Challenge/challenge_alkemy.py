"""Challenge for Alkemy >> Data Analytics + Python"""

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from decouple import config
from datetime import datetime
import shutil
import pandas as pd
import requests
import logging
import sys

# Configuration of logger...
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Get the urls from the settings file
urlMuseo = config('urlMuseo')
urlCine = config('urlCine')
urlBibliotecas = config('urlBibliotecas')

urls = [urlMuseo, urlCine, urlBibliotecas]

# Download files from urls
for url in urls:
    r = requests.get(url, allow_redirects=True)
    x = url.split('/')
    open(x[-1], 'wb').write(r.content)

logging.info("Download complete for csv files...")

now = datetime.now()
date = now.strftime("%d-%m-%Y")

# Get files from directory
cine = 'cine.csv'
museos = 'museos_datosabiertos.csv'
bibliotecas = 'biblioteca_popular.csv'

files = [cine, museos, bibliotecas]

# Move files to new destinations
for file in files:
    if file == 'cine.csv':
        shutil.move('cine.csv', 'cine/2022-junio/cine-21-06-2022.csv')
    elif file == 'biblioteca_popular.csv':
        shutil.move('biblioteca_popular.csv', 'bibliotecas/2022-junio/bibliotecas-21-06-2022.csv')
    else:
        shutil.move('museos_datosabiertos.csv', 'museos/2022-junio/museos-21-06-2022.csv')

logging.info("Finished reorganizing files...")

# Here we normalize the data for further processing
with open("cine/2022-junio/cine-21-06-2022.csv", 'rb') as f:
    df = pd.read_csv(f)
    df.rename(columns={'Cod_Loc': 'cod_localidad', 'IdProvincia': 'id_provincia', 'IdDepartamento': 'id_departamento',
                       'Categoría': 'categoría', 'Provincia': 'provincia', 'Localidad': 'localidad', 'Nombre': 'nombre',
                       'Dirección': 'domicilio', 'CP': 'código postal', 'Teléfono': 'número de teléfono',
                       'Mail': 'mail', 'Web': 'web'}, inplace=True)
    df.to_csv('output/cines_corregido.csv', index=False)

with open("museos/2022-junio/museos-21-06-2022.csv", 'rb') as f:
    df = pd.read_csv(f)
    df.rename(columns={'Cod_Loc': 'cod_localidad', 'IdProvincia': 'id_provincia', 'IdDepartamento': 'id_departamento',
                       'categoria': 'categoría', 'direccion': 'domicilio', 'CP': 'código postal',
                       'telefono': 'número de teléfono', 'Mail': 'mail', 'Web': 'web'}, inplace=True)
    df.to_csv('output/museos_corregido.csv', index=False)

with open("bibliotecas/2022-junio/bibliotecas-21-06-2022.csv", 'rb') as f:
    df = pd.read_csv(f)
    df.rename(columns={'Cod_Loc': 'cod_localidad', 'IdProvincia': 'id_provincia', 'IdDepartamento': 'id_departamento',
                       'Categoría': 'categoría', 'Provincia': 'provincia', 'Localidad': 'localidad', 'Nombre': 'nombre',
                       'Domicilio': 'domicilio', 'CP': 'código postal', 'Teléfono': 'número de teléfono',
                       'Mail': 'mail', 'Web': 'web'}, inplace=True)
    df.to_csv('output/biblioteca_corregido.csv', index=False)

logging.info("Finished normalizing columns...")

# Here we select the columns for the first table
cine = 'output/cines_corregido.csv'
museos_datosabiertos = 'output/museos_corregido.csv'
biblioteca_popular = 'output/biblioteca_corregido.csv'

f = pd.read_csv(cine)
proc_cine = f[['cod_localidad', 'id_provincia', 'id_departamento', 'categoría', 'provincia', 'localidad', 'nombre',
               'domicilio', 'código postal', 'número de teléfono', 'mail', 'web']]
proc_cine.to_csv('output/cines_acotado.csv', index=False)

f = pd.read_csv(museos_datosabiertos)
proc_mus = f[['cod_localidad', 'id_provincia', 'id_departamento', 'categoría', 'provincia', 'localidad', 'nombre',
              'domicilio', 'código postal', 'número de teléfono', 'mail', 'web']]
proc_mus.to_csv('output/museos_acotado.csv', index=False)

f = pd.read_csv(biblioteca_popular)
proc_biblio = f[['cod_localidad', 'id_provincia', 'id_departamento', 'categoría', 'provincia', 'localidad', 'nombre',
                 'domicilio', 'código postal', 'número de teléfono', 'mail', 'web']]
proc_biblio.to_csv('output/biblioteca_acotado.csv', index=False)

logging.info("Columns selected...")

# Now we join the selected dataframes to create the new table
cine = 'output/cines_acotado.csv'
museos_datosabiertos = 'output/museos_acotado.csv'
biblioteca_popular = 'output/biblioteca_acotado.csv'

files = [cine, museos_datosabiertos, biblioteca_popular]
frames = []

for file in files:
    with open(file, 'rb') as f:
        globals()['strg%s' % file] = pd.read_csv(f)
        frames.append(globals()['strg%s' % file])

normal = pd.concat(frames)
normal['date_time'] = date

normal.to_csv('output/normalized.csv', index=False)

logging.info("Created the normalized file...")

# Generate DataFrame with 'registros por categoria'
categoria = []
counts = []

for n in normal['categoría'].unique():
    categoria.append(n)
    counts.append(str(len(normal[normal['categoría'].str.contains(n)])))

d = {'datos conjuntos': 'Cantidad de registros totales por categoría',
     'categoria': categoria,
     'cantidad de registros': counts}
df_porCategoria = pd.DataFrame(data=d)

# Generate DataFrame with 'registros por fuente'
fuente = []
counts = []

fc = pd.read_csv('cine/2022-junio/cine-21-06-2022.csv')

for n in fc['Fuente'].unique():
    fuente.append(n)
    counts.append(str(len(fc[fc['Fuente'].str.contains(n)])))

fm = pd.read_csv('museos/2022-junio/museos-21-06-2022.csv')

for n in fm['fuente'].unique():
    fuente.append(n)
    counts.append(str(len(fm[fm['fuente'].str.contains(n)])))

fb = pd.read_csv('bibliotecas/2022-junio/bibliotecas-21-06-2022.csv')

for n in fb['Fuente'].unique():
    fuente.append(n)
    counts.append(str(len(fb[fb['Fuente'].str.contains(n)])))

d = {'datos conjuntos': 'Cantidad de registros totales por fuente', 'fuente': fuente, 'cantidad de registros': counts}
df_porFuente = pd.DataFrame(data=d)

# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#     print(df_porFuente)

# Generate DataFrame with 'registros por provincia y categoria'
categoria = []
provincia = []
counts = []

for n in normal['categoría'].unique():
    for p in normal['provincia'].unique():
        categoria.append(n)
        provincia.append(p)
        counts.append(str(
            len(
                normal[(
                           normal['provincia'].str.contains(p)
                       ) & (
                           normal['categoría'].str.contains(n)
                       )]
            )
        ))

d3 = {'datos conjuntos': 'Cantidad de registros totales por provincia y por categoría',
      'categoria': categoria,
      'provincia': provincia,
      'cantidad de registros': counts}
df_porCatyProv = pd.DataFrame(data=d3)

# Here we join the DataFrames in 'dfs' to create a new DataFrame
df = pd.DataFrame(columns=['datos conjuntos', 'categoria', 'provincia', 'fuente', 'cantidad de registros'])
cat = pd.concat([df, df_porCategoria, df_porFuente, df_porCatyProv])
cat['date_time'] = date

cat.to_csv('output/records.csv', index=False)

logging.info("File created for 'registros por categoria', "
             "'registros por fuente' y 'registros por categoria y provincia'...")

# Here we select the columns for our third table
with open("cine/2022-junio/cine-21-06-2022.csv", 'rb') as f:
    df = pd.read_csv(f)
cine = df[['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA']].copy()
cine['date_time'] = date

cine.to_csv('output/info_cine.csv', index=False)

logging.info("Created the 'info_cines' file...")

# Here we create the Postgres connection
postgres_url = config('POSTGRES_URL')

engine = create_engine(postgres_url)

logging.info("Connection created...")

# Here we create the tables in Postgres
fd = open('create_table.sql', 'r')
sqlFile = fd.read()
fd.close()

sqlCommands = sqlFile.split(';')

for command in sqlCommands:
    db = scoped_session(sessionmaker(bind=engine))
    db.execute(command)
    db.commit()
    db.close()

logging.info("Created the tables...")

# Here we populate the Postgres tables
normalized = 'output/normalized.csv'
records = 'output/records.csv'
info_cine = 'output/info_cine.csv'

files = [normalized, records, info_cine]

for file in files:
    with open(file, 'rb') as f:
        df = pd.read_csv(f)
    if file.split('/')[-1] == 'normalized.csv':
        df.to_sql('tabla_normalizada', con=engine, index=False, if_exists='replace')
    elif file.split('/')[-1] == 'records.csv':
        df.to_sql('datos_conjuntos', con=engine, index=False, if_exists='replace')
    elif file.split('/')[-1] == 'info_cine.csv':
        df.to_sql('info_cines', con=engine, index=False, if_exists='replace')

logging.info("Finish populating tables...")
