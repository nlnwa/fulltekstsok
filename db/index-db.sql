-- FULLTEXT INDEX;
CREATE INDEX _fulltext_fts_ ON fulltext USING GIN (fulltext_fts);
CREATE INDEX _fulltext_fts_reduded_ ON fulltext_reduced USING GIN (fulltext_fts);

CREATE INDEX warcinfo_content_hash ON warcinfo(content_hash);
CREATE INDEX warcinfo_response_status ON warcinfo(response_status);
CREATE INDEX doclangs_fulltext_id ON doclangs(fulltext_id);

-- INSERT only responses with a statuscode of 200, only crawled docs with "acutal text" (after BP removal, OCR etc.)
CREATE TEMP TABLE paths_raw(pathid SERIAL, warcinfo_running_id INT, crawl_id INT, path TEXT, domainid INT, fulltext_id INT);
INSERT INTO paths_raw(warcinfo_running_id, crawl_id, path, domainid, fulltext_id)
SELECT running_id, crawl_id, regexp_replace(target_uri, '^https?\:\/\/', ''), domainid, fulltext_id FROM warcinfo w
JOIN domains d ON d.domain = reverse(split_part(reverse(substring(target_uri from '(?:.*://)?([^:/?]*)')), '.', 2)) || '.' || reverse(split_part(reverse(substring(target_uri from '(?:.*://)?([^:/?]*)')), '.', 1))
WHERE response_status LIKE '200%' and fulltext_id != 1 and crawl_id=3;

-- DEDUP ON DOMAIN LEVEL
-- GROUP BY domainid and fulltext_id, select item with lowest ID
CREATE TEMP TABLE paths_dedup (pathid int);
INSERT INTO paths_dedup(pathid)
SELECT min(pathid) FROM paths_raw p
GROUP BY domainid,fulltext_id;

-- LEFT JOIN AND TAKE ONLY those not in the dedupped table
INSERT INTO paths(warcinfo_running_id, crawl_id, path, domainid, fulltext_id)
SELECT warcinfo_running_id, crawl_id, path, domainid, fulltext_id FROM paths_raw p
JOIN paths_dedup pd ON pd.pathid = p.pathid;

-- add content type
UPDATE paths p
SET contenttype = 'html'
FROM warcinfo w
WHERE w.running_id = p.warcinfo_running_id and p.crawl_id = 3
and w.response_mime_type LIKE 'text/html%';

UPDATE paths p
SET contenttype = 'pdf'
FROM warcinfo w
WHERE w.running_id = p.warcinfo_running_id and p.crawl_id = 3
and w.response_mime_type LIKE 'application/pdf%';

UPDATE paths p
SET contenttype = 'doc'
FROM warcinfo w
WHERE w.running_id = p.warcinfo_running_id and p.crawl_id = 3
and (w.response_mime_type LIKE 'application/msword%' or w.response_mime_type LIKE 'application/vnd.openxmlformats-officedocument.wordprocessingml.document%' or w.response_mime_type LIKE 'application/vnd.oasis.opendocument.text-master%');

CREATE INDEX paths_contenttype ON paths(contenttype);
--CREATE INDEX paths_warc_file_id ON paths(warc_file_id);
CREATE INDEX paths_path ON paths(path);

-- PARAGRAPH LEVEL CLASSICIATIONS

-- explode JSON into table with paragraph classifications
INSERT INTO doclangs_para(fulltext_id,lang,tokens,paras)
SELECT fulltext_id, lang, sum(tokens), count(*)
FROM (SELECT  d.fulltext_id,
        json_data.value->>'lang' AS lang,
        (json_data.value->>'tokens')::int AS tokens
FROM doclangs as d,
    JSON_EACH(d.paralang) as json_data
    WHERE --d.fulltext_id IN (
    	--	SELECT fulltext_id FROM warcinfo_fulltext wf
 		--	JOIN paths p ON p.warcinfo_running_id = wf.running_id
 			--WHERE p.contenttype IN ('html')
    	--)
    -- omit empty lines
    --and
    (json_data.value->>'tokens')::int > 0
    ) x
GROUP BY x.fulltext_id, x.lang;

CREATE INDEX doclangs_para_fulltext_id ON doclangs_para(fulltext_id);

--
-- OPTIONAL
--

-- LEAVE OUT CERTAIN DOMAINS
-- REMOVE domains within entities having 0 paths
DELETE FROM domain_entity WHERE domain_entity_id
IN
(
	SELECT de.domain_entity_id
	FROM domain_entity de
	LEFT JOIN paths p ON p.domainid = de.domainid
	WHERE p.domainid is null
);


-- REMOVE domains from Språkrådet's block list
DELETE FROM domain_entity WHERE domain_entity_id
IN
(
	SELECT de.domain_entity_id
	FROM domain_entity de
	JOIN domains d ON d.domainid = de.domainid
	JOIN blocked_subdomains bs ON bs.subdomain = d.domain
);


-- BLOCK RULES
-- -- HØYESTERETT
INSERT INTO blocked_paths_entity(pathid,entityid)
SELECT pathid,de.entityid FROM paths p
JOIN domain_entity de ON de.domainid = p.domainid
WHERE de.entityid = 103 AND (path NOT LIKE '%Enkelt-domstol/hoyesterett%'
AND path NOT LIKE '%Enkelt-domstol/hogsterett%'
AND path NOT LIKE '%Enkelt-domstol/alimusriekti%'
AND path NOT LIKE '%Enkelt-domstol/supremecourt%');
-- -- NAFO
INSERT INTO blocked_paths_entity(pathid,entityid)
SELECT pathid,de.entityid FROM paths p
JOIN domain_entity de ON de.domainid = p.domainid
WHERE de.entityid = 135 AND (path NOT LIKE '%nafo.oslomet.no%');
-- HIVOLDA
INSERT INTO blocked_paths_entity(pathid,entityid)
SELECT pathid,de.entityid FROM paths p
JOIN domain_entity de ON de.domainid = p.domainid
WHERE de.entityid = 50 AND (path NOT LIKE 'bravo.hivolda.no%');

-- DELETE OBJECTS LATER
CREATE INDEX warcinfo_fulltext_fulltext_id ON warcinfo_fulltext(fulltext_id);

DELETE FROM warcinfo WHERE response_mime_type LIKE 'application/pdf%';
DELETE FROM warcinfo WHERE response_mime_type LIKE 'application/msword%' or response_mime_type LIKE 'application/vnd.openxmlformats-officedocument.wordprocessingml.document%' or response_mime_type LIKE 'application/vnd.oasis.opendocument.text-master%';


-- empty fulltext table
DELETE FROM fulltext ft
WHERE NOT EXISTS (
	SELECT * FROM warcinfo_fulltext wf
	WHERE wf.fulltext_id = ft.fulltext_id
);


-- SPECIAL CASE: WARC FILE NAMES WRONG DURING IMPORT
-- REPLACE IDs based on lookhp
with t as (
	SELECT w.running_id,w1.warc_file_id as old_fileid,w2.warc_file_id as new_fileid FROM warcinfo w
	JOIN warc_files w1 ON w1.warc_file_id = w.warc_file_id
	JOIN warc_files w2 ON w2.warc_file_name = replace(w1.warc_file_name, '//', '/')
	WHERE w1.warc_file_name LIKE '%//%'
)
UPDATE warcinfo warc
SET warc_file_id = t.new_fileid
FROM t
WHERE warc.running_id = t.running_id;

-- DELETE warc_files NOT IN warcinfo
DELETE FROM warc_files WHERE warc_file_name LIKE '%//%';
