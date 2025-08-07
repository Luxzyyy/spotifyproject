from api_request import get_spotify_token, get_all_artist_data, get_artist_albums, get_artist_top_tracks, get_multiple_artists, create_albums_dataframe, create_artists_dataframe, create_top_tracks_dataframe
from supabase_upload import upload_df_to_supabase
import pandas as pd 
import requests
import time
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Union


artist_ids = [
    '2YZyLoL8N0Wb9xBt1NhZWg',  # Kendrick Lamar
    '0Y4inQK6OespitzD6ijMwb',  # Freddie Gibbs
    '4V8LLVI7PbaPR0K2TGSxFF',  # Tyler, The Creator
    '6fxyWrfmjcbj5d12gXeiNV',  # Denzel Curry
    '3zz52ViyCBcplK0ftEVPSS'   # Quadeca
]


try:

    # Load secrets from environment variables
    load_dotenv()
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    # Get access token
    token = get_spotify_token(client_id, client_secret)
    print("Got access token successfully!")
    
    # Get all data
    artists_data, all_albums, all_top_tracks = get_all_artist_data(artist_ids, token)
    
    # Create DataFrames
    artists_df = create_artists_dataframe(artists_data)
    albums_df = create_albums_dataframe(all_albums)
    top_tracks_df = create_top_tracks_dataframe(all_top_tracks)

    # Edit df's to enable upload to Supabase
    artists_df = artists_df[['artist_id', 'artist_name', 'popularity', 'followers', 'genres', 'album_image', 'spotify_url']]
    artists_df.rename(columns={"album_image": "image", "spotify_url": "url"}, inplace=True)
    albums_df = albums_df[['album_id', 'album_name', 'album_type', 'artist_id', 'release_date', 'album_image', 'total_tracks',  'spotify_url']]
    albums_df.rename(columns={"spotify_url": "url"}, inplace=True)
    top_tracks_df = top_tracks_df[['track_id', 'track_name', 'artist_id', 'track_rank', 'album_name', 'popularity', 'duration_ms',  'explicit', 'spotify_url']]
    top_tracks_df.rename(columns={"spotify_url": "url"}, inplace=True)

    # Load Dataframes to Supabase
    upload_df_to_supabase(artists_df, "artists", supabase_url, supabase_key)
    upload_df_to_supabase(albums_df, "albums", supabase_url, supabase_key)
    upload_df_to_supabase(top_tracks_df, "tracks", supabase_url, supabase_key)

except Exception as e:
    print(f"❌ Error: {e}")