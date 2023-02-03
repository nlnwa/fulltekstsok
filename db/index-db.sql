-- FULLTEXT INDEX;
CREATE INDEX _fulltext_fts_ ON fulltext USING GIN (fulltext_fts);
--CREATE INDEX _fulltext_fts_reduded_ ON fulltext_reduced USING GIN (fulltext_fts);

CREATE INDEX warcinfo_content_hash ON warcinfo(content_hash);
CREATE INDEX warcinfo_response_status ON warcinfo(response_status);
