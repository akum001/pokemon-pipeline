import requests
import json

from .pg_session import get_postgres_conn


URL = 'https://pokeapi.co/api/v2/pokemon'

def fetch_pokemon():
    conn = get_postgres_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT p_limit, p_offset FROM pokemon_page ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()

    if row:
        limit, offset = row
        limit = int(limit)
        offset = int(offset)
    else:
        limit = 5
        offset = 0
    response = requests.get(URL, params={'limit': limit, 'offset': offset})
    response.raise_for_status()

    data = response.json()
    new_offset = offset + limit
    cursor.execute("INSERT INTO pokemon_page (p_limit, p_offset) VALUES (%s, %s)", (limit, new_offset))
    conn.commit()

    for pokemon in data['results']:
        cursor.execute("INSERT INTO pokemon_character (p_name, p_url) VALUES (%s, %s)", (pokemon['name'], pokemon['url']))
    conn.commit()
