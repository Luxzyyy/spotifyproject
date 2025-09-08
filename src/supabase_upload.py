from supabase import create_client, Client
import pandas as pd
from typing import Union
from sqlalchemy import create_engine, inspect
import psycopg2


def upload_df_to_supabase(
    df: pd.DataFrame,
    table_name: str,
    supabase_url: str,
    supabase_key: str,
    db_url: str
) -> Union[dict, None]:
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            inspector = inspect(engine)
            if not inspector.has_table(table_name):
                df.to_sql(table_name, con=connection, if_exists='replace', index=False)
                print(f"Table '{table_name}' created and data inserted successfully.")
            else:
                df.to_sql(table_name, con=connection, if_exists='append', index=False)
                print(f"Data appended to existing table '{table_name}' successfully.")
        return {"status": "success", "message": f"Data uploaded to table '{table_name}' successfully."}
    except Exception as e:
        print(f"❌ Error uploading data to Supabase: {e}")
        return {"status": "error", "message": str(e)} 
