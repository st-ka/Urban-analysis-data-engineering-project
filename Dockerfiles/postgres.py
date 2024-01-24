engine = create_engine(
'postgresql+psycopg2:'
'//postgres:'          # username for postgres
'docker'              # password for postgres
'@postgres-db:5432/'     # postgres server name and the exposed port
'postgres')

con=engine.connect()
# create an empty table xrysi_rentals 
sql = """
  create table if not exists xrysi_rentals (
  Area int,
  Price int,
  URL VARCHAR(50),
  neighbourhood VARCHAR(50),
  price per sqm int,
  date_imported date
);
"""

sql COPY xrysi_rentals FROM f"xe_rentals_{date.today()}.csv" DELIMITER ',' CSV HEADER;

# execute the 'sql' query
with engine.connect().execution_options(autocommit=True) as conn:
    query = conn.execute(text(sql))

# insert the dataframe data to 'xrysi_rentals' SQL table
with engine.connect().execution_options(autocommit=True) as conn:
    df.to_sql(', con=conn, if_exists='append', index= False)

#optional
print(pd.read_sql_query("""
select * from xrysi
""", con))
