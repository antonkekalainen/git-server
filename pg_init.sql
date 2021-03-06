CREATE TABLE objects (hash BYTEA PRIMARY KEY, pack TEXT NOT NULL, type TEXT NOT NULL, compressed_size INTEGER NOT NULL, size INTEGER NOT NULL, location INTEGER NOT NULL);
CREATE TABLE deltas (hash BYTEA PRIMARY KEY, pack TEXT NOT NULL, base BYTEA NOT NULL, compressed_size INTEGER NOT NULL, size INTEGER NOT NULL, location INTEGER NOT NULL);
CREATE TABLE parents (child BYTEA PRIMARY KEY, parents BYTEA ARRAY NOT NULL);
CREATE INDEX ON parents USING hash (child);
CREATE TABLE refs (repo TEXT NOT NULL, name TEXT NOT NULL, hash BYTEA NOT NULL);
