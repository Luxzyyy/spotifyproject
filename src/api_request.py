import pandas as pd 
import requests
import time
import os
from dotenv import load_dotenv




def get_spotify_token(client_id, client_secret):
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    auth_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    
    response = requests.post(auth_url, headers=auth_headers, data=auth_data)
    return response.json()['access_token']

def get_multiple_artists(artist_ids, access_token):
    ids_string = ','.join(artist_ids)
    
    url = f'https://api.spotify.com/v1/artists?ids={ids_string}'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    response = requests.get(url, headers=headers)
    return response.json()

def get_artist_albums(artist_id, access_token, limit=20):
    """Get albums for a specific artist"""
    url = f'https://api.spotify.com/v1/artists/{artist_id}/albums'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'include_groups': 'album,single',  # album, single, appears_on, compilation
        'market': 'US',
        'limit': limit
    }
    
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def get_artist_top_tracks(artist_id, access_token):
    """Get top tracks for a specific artist"""
    url = f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'market': 'US'
    }
    
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def create_artists_dataframe(artists_data):
    """Convert artists data to DataFrame"""
    artists = artists_data['artists']
    
    artist_records = []
    for artist in artists:
        record = {
            'artist_id': artist['id'],
            'artist_name': artist['name'],
            'popularity': artist['popularity'],
            'followers': artist['followers']['total'],
            'genres': ', '.join(artist['genres']),
            'spotify_url': artist['external_urls']['spotify']
        }

        # Add album image if available
        if artist['images']:
            record['album_image'] = artist['images'][0]['url']
        else:
            record['album_image'] = None
        artist_records.append(record)
    
    return pd.DataFrame(artist_records)

def create_albums_dataframe(all_albums_data):
    """Convert albums data to DataFrame"""
    album_records = []
    
    for artist_id, albums_data in all_albums_data.items():
        if 'items' in albums_data:
            for album in albums_data['items']:
                record = {
                    'artist_id': artist_id,
                    'album_id': album['id'],
                    'album_name': album['name'],
                    'album_type': album['album_type'],
                    'release_date': album['release_date'],
                    'total_tracks': album['total_tracks'],
                    'spotify_url': album['external_urls']['spotify']
                }
                
                # Add album image if available
                if album['images']:
                    record['album_image'] = album['images'][0]['url']
                else:
                    record['album_image'] = None
                    
                album_records.append(record)
    
    return pd.DataFrame(album_records)

def create_top_tracks_dataframe(all_tracks_data):
    """Convert top tracks data to DataFrame"""
    track_records = []
    
    for artist_id, tracks_data in all_tracks_data.items():
        if 'tracks' in tracks_data:
            for i, track in enumerate(tracks_data['tracks'], 1):
                record = {
                    'artist_id': artist_id,
                    'track_rank': i,
                    'track_id': track['id'],
                    'track_name': track['name'],
                    'album_name': track['album']['name'],
                    'popularity': track['popularity'],
                    'duration_ms': track['duration_ms'],
                    'explicit': track['explicit'],
                    'spotify_url': track['external_urls']['spotify']
                }
                
                # Add track preview if available
                record['preview_url'] = track.get('preview_url')
                
                track_records.append(record)
    
    return pd.DataFrame(track_records)

def get_all_artist_data(artist_ids, access_token):
    """Get artists, albums, and top tracks data"""
    # Get artists data
    artists_data = get_multiple_artists(artist_ids, access_token)
    
    # Get albums and top tracks for each artist
    all_albums = {}
    all_top_tracks = {}
    
    for artist_id in artist_ids:
        # Add small delay to be respectful to the API
        time.sleep(0.1)
        
        # Get albums
        albums_data = get_artist_albums(artist_id, access_token)
        all_albums[artist_id] = albums_data
        
        # Get top tracks
        top_tracks_data = get_artist_top_tracks(artist_id, access_token)
        all_top_tracks[artist_id] = top_tracks_data
        
        print(f"Retrieved data for artist: {artist_id}")
    
    return artists_data, all_albums, all_top_tracks
