from api_request import get_spotify_token, get_all_artist_data, get_artist_albums, get_artist_top_tracks, get_multiple_artists
from supabase_upload import upload_df_to_supabase

artist_ids = [
    '2YZyLoL8N0Wb9xBt1NhZWg',  # Kendrick Lamar
    '0Y4inQK6OespitzD6ijMwb',  # Freddie Gibbs
    '4V8LLVI7PbaPR0K2TGSxFF',  # Tyler, The Creator
    '6fxyWrfmjcbj5d12gXeiNV',  # Denzel Curry
    '3zz52ViyCBcplK0ftEVPSS'   # Quadeca
]

# Main execution
try:

    # Load secrets from environment variables
    load_dotenv()
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    
    # Get access token
    token = get_spotify_token(client_id, client_secret)
    print("Got access token successfully!")
    
    # Get all data
    artists_data, all_albums, all_top_tracks = get_all_artist_data(artist_ids, token)
    
    # Create DataFrames
    artists_df = create_artists_dataframe(artists_data)
    albums_df = create_albums_dataframe(all_albums)
    top_tracks_df = create_top_tracks_dataframe(all_top_tracks)

    print(artists_df.head())
    print(albums_df.head())
    print(top_tracks_df.head())

except Exception as e:
    print(f"❌ Error: {e}")