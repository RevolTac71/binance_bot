import urllib.parse
from sqlalchemy import create_engine, inspect

user = 'postgres.uvqhpiilmameyjortqoc'
password = urllib.parse.quote_plus('zuzuba3314!!')
host = 'aws-1-ap-southeast-2.pooler.supabase.com'
port = '6543'
database = 'postgres'

uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(uri)

print("Connecting to DB...")
try:
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("Tables:", tables)
    for table in tables:
        print(f"\nTable: {table}")
        for col in inspector.get_columns(table):
            print(f"  - {col['name']}: {col['type']}")
except Exception as e:
    print("Error:", e)
