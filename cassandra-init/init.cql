CREATE KEYSPACE IF NOT EXISTS pokemon_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE pokemon_keyspace;

CREATE TABLE IF NOT EXISTS pokemon_detail (
    id UUID PRIMARY KEY,
    name text,
    url text,
    weight int,
    height int
);