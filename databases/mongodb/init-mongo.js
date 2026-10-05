// MongoDB Initialization script for Sokti OTT Metadata
db = db.getSiblingDB('sokti_metadata');

// Create collections
db.createCollection('movies');
db.createCollection('series');
db.createCollection('episodes');
db.createCollection('content_metadata');
db.createCollection('subtitles_metadata');

// Create indexes
db.movies.createIndex({ content_id: 1 }, { unique: true });
db.movies.createIndex({ genres: 1 });
db.movies.createIndex({ release_year: -1 });
db.movies.createIndex({ title: "text", synopsis: "text" }, { default_language: "english", language_override: "none" });

db.series.createIndex({ content_id: 1 }, { unique: true });
db.episodes.createIndex({ content_id: 1, season: 1, episode_number: 1 }, { unique: true });
db.subtitles_metadata.createIndex({ content_id: 1, language: 1 });

print("Sokti metadata database and indexes initialized successfully.");
