#!/usr/bin/env python3
"""
Sokti OTT Platform - MongoDB Content Metadata Seeder
Populates at least 100 synthetic pieces of content across movies, series, episodes,
and metadata collections.
"""

import os
import random
from pymongo import MongoClient, ReplaceOne
from dotenv import load_dotenv

load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27018"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
MONGO_PASS = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin")
MONGO_DB = os.getenv("MONGO_DATABASE", "sokti_metadata")

GENRES_LIST = [
    "Sci-Fi", "Action", "Thriller", "Drama", "Comedy", "Romance", 
    "Crime", "Mystery", "Documentary", "Animation", "Adventure", "Horror"
]
LANGUAGES = ["en", "hi", "es", "ja", "ko", "fr", "de"]
MATURITY_RATINGS = ["U/A 7+", "U/A 13+", "U/A 16+", "A", "PG-13", "R"]

DIRECTORS = [
    "Christopher Nolan", "Denis Villeneuve", "S.S. Rajamouli", "Bong Joon-ho",
    "Greta Gerwig", "Anurag Kashyap", "David Fincher", "Hayao Miyazaki",
    "Ridley Scott", "Guillermo del Toro", "Zoya Akhtar", "Martin Scorsese"
]

ACTORS = [
    "Leonardo DiCaprio", "Cillian Murphy", "Deepika Padukone", "Song Kang-ho",
    "Florence Pugh", "Ranbir Kapoor", "Timothée Chalamet", "Manoj Bajpayee",
    "Emma Stone", "Zendaya", "Shah Rukh Khan", "Christian Bale", "Pedro Pascal"
]

TITLES_VOCAB = [
    ("Cosmic Horizon", "Sci-Fi", "An intrepid crew journeys across wormholes to rescue humanity from ecological collapse."),
    ("The Delhi Ledger", "Crime", "A cynical investigative journalist uncovers a multi-billion dollar real estate syndicate in Old Delhi."),
    ("Silent Echoes", "Thriller", "A forensic audio analyst reconstructs a mysterious tape that implicates high-ranking cabinet ministers."),
    ("Neon Nights", "Action", "In cyberpunk Mumbai, an augmented bounty hunter takes on a rogue autonomous drone swarm."),
    ("Bengaluru Tech Heist", "Comedy", "Three underpaid junior software engineers accidentally hijack an offshore hedge fund's encrypted database."),
    ("The Himalayan Ascent", "Adventure", "A family of mountaineers braves an unseasonal blizzard to reach the mystical summit of Kanchenjunga."),
    ("Monsoon Rhapsody", "Romance", "Two rival classical violinists find harmony and secrets during an extended monsoon in Kolkata."),
    ("Shadows of Kyoto", "Mystery", "A retired detective in Kyoto is drawn into a cold case involving missing Edo-period woodblock prints."),
    ("Quantum Paradox", "Sci-Fi", "When a particle accelerator simulation leaks into reality, parallel versions of Earth collide."),
    ("The Last Scribe", "Drama", "An elderly bookbinder preserves forbidden manuscripts during the twilight of an authoritarian regime."),
    ("Velocity Point", "Action", "Undercover agents infiltrate a high-stakes street racing syndicate controlling maritime smuggling ports."),
    ("Midnight Radio", "Horror", "A late-night radio host begins receiving frantic distress calls from frequencies that were decommissioned decades ago.")
]

def get_mongo_client():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
    return MongoClient(uri)

def generate_content(num_movies=85, num_series=25):
    movies = []
    series = []
    episodes = []
    content_meta = []
    subtitles = []

    # 1. Generate Movies
    for i in range(1, num_movies + 1):
        content_id = f"cnt_mov_{i:04d}"
        base_title, base_genre, base_synopsis = random.choice(TITLES_VOCAB)
        title = f"{base_title} (Part {i})" if i > len(TITLES_VOCAB) else f"{base_title} {i}"
        genres = list(set([base_genre] + random.sample(GENRES_LIST, random.randint(1, 2))))
        lang = random.choice(LANGUAGES)
        year = random.randint(2005, 2026)
        director = random.choice(DIRECTORS)
        cast = random.sample(ACTORS, random.randint(2, 4))
        duration = random.randint(5400, 10800) # 90 - 180 min
        rating = random.choice(MATURITY_RATINGS)
        keywords = [g.lower() for g in genres] + ["cinema", "sokti-original", "4k-hdr"]

        sub_locs = {
            "en": f"s3://sokti-raw/subtitles/{content_id}_en.vtt",
            "es": f"s3://sokti-raw/subtitles/{content_id}_es.vtt",
            "hi": f"s3://sokti-raw/subtitles/{content_id}_hi.vtt"
        }

        movie_doc = {
            "content_id": content_id,
            "title": title,
            "synopsis": f"{base_synopsis} Directed with stunning visual fidelity by {director}.",
            "genres": genres,
            "language": lang,
            "release_year": year,
            "cast": cast,
            "director": director,
            "duration_seconds": duration,
            "maturity_rating": rating,
            "keywords": keywords,
            "subtitle_locations": sub_locs
        }
        movies.append(movie_doc)
        content_meta.append({**movie_doc, "type": "movie"})

        for sub_lang, loc in sub_locs.items():
            subtitles.append({
                "content_id": content_id,
                "language": sub_lang,
                "storage_uri": loc,
                "format": "vtt",
                "char_count": random.randint(12000, 35000)
            })

    # 2. Generate Series & Episodes
    for s_idx in range(1, num_series + 1):
        series_id = f"cnt_ser_{s_idx:04d}"
        base_title, base_genre, base_synopsis = random.choice(TITLES_VOCAB)
        title = f"Chronicles of {base_title} S{s_idx}"
        genres = list(set([base_genre] + random.sample(GENRES_LIST, random.randint(1, 2))))
        lang = random.choice(LANGUAGES)
        year = random.randint(2018, 2026)
        director = random.choice(DIRECTORS)
        cast = random.sample(ACTORS, random.randint(2, 4))

        series_doc = {
            "content_id": series_id,
            "title": title,
            "synopsis": f"Multi-season investigative saga: {base_synopsis}",
            "genres": genres,
            "language": lang,
            "release_year": year,
            "cast": cast,
            "director": director,
            "maturity_rating": random.choice(MATURITY_RATINGS),
            "total_seasons": 2
        }
        series.append(series_doc)
        content_meta.append({**series_doc, "type": "series"})

        # 3 episodes per season
        for season in range(1, 3):
            for ep_num in range(1, 4):
                ep_id = f"{series_id}_s{season}e{ep_num}"
                episodes.append({
                    "episode_id": ep_id,
                    "content_id": series_id,
                    "season": season,
                    "episode_number": ep_num,
                    "title": f"Episode {ep_num}: The Awakening",
                    "synopsis": f"In season {season} episode {ep_num}, tensions escalate following an unexpected discovery.",
                    "duration_seconds": random.randint(2400, 3600)
                })

    return movies, series, episodes, content_meta, subtitles

def seed_mongodb():
    print(f"Connecting to MongoDB at {MONGO_HOST}:{MONGO_PORT}...")
    client = get_mongo_client()
    db = client[MONGO_DB]

    movies, series, episodes, content_meta, subtitles = generate_content(num_movies=90, num_series=20)
    total_content = len(movies) + len(series)
    print(f"Generated {len(movies)} movies, {len(series)} series (total: {total_content} items), and {len(episodes)} episodes.")

    # Upsert movies
    movie_ops = [ReplaceOne({"content_id": doc["content_id"]}, doc, upsert=True) for doc in movies]
    res_m = db.movies.bulk_write(movie_ops)
    print(f"Movies synced: {res_m.upserted_count} upserted, {res_m.modified_count} modified.")

    # Upsert series
    series_ops = [ReplaceOne({"content_id": doc["content_id"]}, doc, upsert=True) for doc in series]
    res_s = db.series.bulk_write(series_ops)
    print(f"Series synced: {res_s.upserted_count} upserted, {res_s.modified_count} modified.")

    # Upsert episodes
    ep_ops = [ReplaceOne({"episode_id": doc["episode_id"]}, doc, upsert=True) for doc in episodes]
    res_e = db.episodes.bulk_write(ep_ops)
    print(f"Episodes synced: {res_e.upserted_count} upserted, {res_e.modified_count} modified.")

    # Upsert content_metadata
    meta_ops = [ReplaceOne({"content_id": doc["content_id"]}, doc, upsert=True) for doc in content_meta]
    res_meta = db.content_metadata.bulk_write(meta_ops)
    print(f"Unified Content Metadata synced: {res_meta.upserted_count} upserted, {res_meta.modified_count} modified.")

    # Upsert subtitles
    sub_ops = [ReplaceOne({"content_id": doc["content_id"], "language": doc["language"]}, doc, upsert=True) for doc in subtitles]
    res_sub = db.subtitles_metadata.bulk_write(sub_ops)
    print(f"Subtitles Metadata synced: {res_sub.upserted_count} upserted, {res_sub.modified_count} modified.")

    print("MongoDB metadata seeding completed successfully!")

if __name__ == "__main__":
    seed_mongodb()
