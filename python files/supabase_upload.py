from supabase import create_client, Client
import pandas as pd
from typing import Union

def upload_df_to_supabase(
    df: pd.DataFrame,
    table_name: str,
    supabase_url: str,
    supabase_key: str
) -> Union[dict, None]:
    """
    Uploads a DataFrame to a Supabase table.

    Args:
        df (pd.DataFrame): The data to insert.
        table_name (str): The name of the table in Supabase.
        supabase_url (str): The URL of your Supabase project.
        supabase_key (str): Your Supabase anon or service role API key.

    Returns:
        dict or None: Response from Supabase if successful, None if failed.
    """
    try:
        # Create Supabase client
        supabase: Client = create_client(supabase_url, supabase_key)

        # Convert DataFrame to list of records (dicts)
        records = df.to_dict(orient="records")

        # Insert records (up to 1000 at a time for Supabase API)
        if records:
            response = supabase.table(table_name).insert(records).execute()
            print(f"Inserted {len(records)} records into '{table_name}'")
            return response
        else:
            print("DataFrame is empty. Nothing to insert.")
            return None

    except Exception as e:
        print(f"❌ Error uploading to Supabase: {e}")
        return None
