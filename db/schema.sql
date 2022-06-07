CREATE TABLE crawls (crawl_id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE, year INT, comment TEXT);
CREATE TABLE entities ("entityID" INT PRIMARY KEY, "entityName" text, "entityNameAlt" text, "organizationNumber" numeric, creation_date text, deleted bool, deletion_date text, enhetsregisteret_blob json, homepage text, comment text);
CREATE TABLE domains (domainid SERIAL PRIMARY KEY, domain text UNIQUE);
CREAtE TABLE domain_entity (domain_entity_id SERIAL PRIMARY KEY, entityid INT REFERENCES entities("entityID") ON UPDATE CASCADE ON DELETE CASCADE, domainid INT REFERENCES domains(domainid) ON DELETE CASCADE ON UPDATE CASCADE);
-- ALLOW ONLY unqiue pairs of entities and domains
ALTER TABLE domain_entity ADD CONSTRAINT entityid_domainid UNIQUE(entityid,domainid);
CREATE TABLE warc_files(warc_file_id SERIAL PRIMARY KEY, warc_file_name text UNIQUE);

CREATE TABLE fulltext (fulltext_id SERIAL, crawl_id INT, html text, html_hash text, fulltext text, fulltext_hash text, fulltext_fts tsvector GENERATED ALWAYS AS (to_tsvector('norwegian', fulltext)) STORED, PRIMARY KEY (fulltext_id, crawl_id)) PARTITION BY LIST(crawl_id);
CREATE TABLE fulltext_reduced (fulltext_hash TEXT, crawl_id INT, fulltext TEXT, fulltext_fts tsvector GENERATED ALWAYS AS (to_tsvector('norwegian', fulltext)) STORED, PRIMARY KEY (fulltext_hash, crawl_id)) PARTITION BY LIST(crawl_id);

CREATE TABLE warcinfo(record_id text, crawl_id INT, type text, concurrent_to text, refers_to text, target_uri text, date text, content_hash text, payload_digest text, content_type text, content_length bigint, response_mime_type text, response_status text, redirect_location text, warc_file_id int REFERENCES warc_files(warc_file_id), fulltext_id integer) PARTITION BY LIST(crawl_id);
ALTER TABLE warcinfo ADD CONSTRAINT warcinfo_record_id_crawl_id PRIMARY KEY(record_id,crawl_id);
ALTER TABLE warcinfo ADD CONSTRAINT warcinfo_fulltext_id_craw_id FOREIGN KEY(fulltext_id,crawl_id) REFERENCES fulltext(fulltext_id,crawl_id);

--CREATE TABLE warc_domain(warc_domain_id SERIAL PRIMARY KEY, warc_file_id INT REFERENCES warc_files(warc_file_id) ON UPDATE CASCADE ON DELETE CASCADE, domain text, topdomain text);
CREATE TABLE doclangs (doclang_id SERIAL PRIMARY KEY, fulltext_id int, crawl_id int, lang text, paralang json, tokens int, paras int);
ALTER TABLE doclangs ADD CONSTRAINT doclangs_fulltext_id_craw_id FOREIGN KEY(fulltext_id,crawl_id) REFERENCES fulltext(fulltext_id,crawl_id);

CREATE TABLE doclangs_para (doclang_para_id SERIAL PRIMARY KEY, fulltext_id int, crawl_id int, lang text, tokens int, paras int);
ALTER TABLE doclangs_para ADD CONSTRAINT doclangs_para_fulltext_id_craw_id FOREIGN KEY(fulltext_id,crawl_id) REFERENCES fulltext(fulltext_id,crawl_id);

CREATE TABLE paths (pathid SERIAL, crawl_id INT, record_id text, path text, surt text, contenttype varchar(4), domainid int REFERENCES domains(domainid), fulltext_id integer, PRIMARY KEY (pathid,crawl_id)) PARTITION BY LIST(crawl_id);
ALTER TABLE paths ADD CONSTRAINT paths_record_id FOREIGN KEY(record_id,crawl_id) REFERENCES warcinfo(record_id,crawl_id) ON UPDATE CASCADE ON DELETE CASCADE;
ALTER TABLE paths ADD CONSTRAINT paths_fulltext_id_craw_id FOREIGN KEY(fulltext_id,crawl_id) REFERENCES fulltext(fulltext_id,crawl_id);
