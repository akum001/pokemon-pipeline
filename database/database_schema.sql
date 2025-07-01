CREATE TABLE IF NOT EXISTS pokemon_character (
    id SERIAL PRIMARY KEY,
    p_name VARCHAR(200),
    p_url VARCHAR(200),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pokemon_page (
    id SERIAL PRIMARY KEY,
    p_limit VARCHAR(50),
    p_offset VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
