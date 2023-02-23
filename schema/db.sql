CREATE TABLE crawls (crawl_id SMALLSERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, year INT, comment TEXT);

CREATE TABLE warc_files(warc_file_id SERIAL PRIMARY KEY, warc_file_name text UNIQUE);

CREATE TABLE fulltext (fulltext_hash TEXT, crawl_id INT, fulltext TEXT, fulltext_fts tsvector GENERATED ALWAYS AS (to_tsvector('norwegian', fulltext)) STORED, PRIMARY KEY (fulltext_hash, crawl_id)) PARTITION BY LIST(crawl_id);

CREATE TABLE warcinfo(record_id text, crawl_id INT, type text, concurrent_to text, refers_to text, target_uri text, date text, content_hash text, payload_digest text, content_type text, content_length bigint, response_mime_type text, response_status text, redirect_location text, warc_file_id int REFERENCES warc_files(warc_file_id), fulltext_hash text) PARTITION BY LIST(crawl_id);
ALTER TABLE warcinfo ADD CONSTRAINT warcinfo_record_id_crawl_id PRIMARY KEY(record_id,crawl_id);
ALTER TABLE warcinfo ADD CONSTRAINT warcinfo_fulltext_hash_crawl_id FOREIGN KEY(fulltext_hash,crawl_id) REFERENCES fulltext(fulltext_hash,crawl_id);
