from warcio.archiveiterator import ArchiveIterator
import config as c
import psycopg2 as pg
import os
import uuid
import hashlib
import sys

file = sys.argv[1]
pdf_text_directory = sys.argv[2]
crawl_id = sys.argv[3]

with pg.connect(user=c.user, password=c.password, database=c.database, host=c.host, port=c.port) as con:
    cur = con.cursor()

    if file.endswith('warc.gz') and not file.endswith('meta.warc.gz'):
        print(file)

        with open(file, 'rb') as stream:
            # write warc file record, if existent return

            warcfile_sql = """WITH e AS (
            INSERT INTO warc_files(warc_file_name) VALUES(%s) ON CONFLICT DO NOTHING RETURNING warc_file_id
                    )
                SELECT * FROM e
                UNION
                SELECT warc_file_id FROM warc_files WHERE warc_file_name=%s;"""

            cur.execute(warcfile_sql, (file,file))
            warc_file_id = cur.fetchone()[0]

            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    mime = record.http_headers.get('Content-Type')

                    if mime:
                        if mime.startswith('application/pdf'):
                            statusline = record.http_headers.statusline
                            loc = record.http_headers.get('Location')

                            # we create a hash of the content stream for deduplication (see the problem in: https://github.com/webrecorder/warcio/issues/74)
                            try:
                                content_stream = record.content_stream().read()
                            except:
                                print("problem reading content stream... skipping", record.rec_headers.get('WARC-Record-ID'))
                                continue
                            content_hash = hashlib.sha1(content_stream).hexdigest()

                            try:
                                filename = content_hash + ".txt"
                                path = os.path.join(pdf_text_directory, filename)

                                with open(path, 'r') as pdftotext_file:
                                    para = pdftotext_file.read()

                                # Postgres does not handle the NULL character (\x00), replace it with "replacement character"
                                # https://github.com/cms-dev/cms/issues/888
                                para = para.replace("\x00", "\uFFFD")
                            except:
                                print("problem loading/finding PDF text... skipping", record.rec_headers.get('WARC-Record-ID'), filename)
                                continue

                            hashStr=hashlib.sha1(para.encode('utf-8')).hexdigest()

                            try:
                                fulltext_sql = """WITH e AS (
                                    INSERT INTO fulltext(hash,fulltext) VALUES(%s,%s) ON CONFLICT DO NOTHING RETURNING fulltext_id
                                    )
                                SELECT * FROM e
                                UNION
                                SELECT fulltext_id FROM fulltext WHERE hash=%s;"""
                                cur.execute(fulltext_sql, (hashStr, para, hashStr))
                                fulltext_id = cur.fetchone()
                            except:
                                print("problem inserting PDF text to DB... skipping", record.rec_headers.get('WARC-Record-ID'), filename)
                                continue

                             # INSERT WARCINFO
                            warcinfo_sql = """INSERT INTO warcinfo(crawl_id, type, record_id, concurrent_to, target_uri, date, content_hash, payload_digest, content_type, content_length, response_mime_type, response_status, redirect_location, warc_file_id, fulltext_id)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                            cur.execute(warcinfo_sql, (crawl_id, record.rec_headers.get('WARC-Type'), record.rec_headers.get('WARC-Record-ID'), record.rec_headers.get('WARC-Concurrent-To'), record.rec_headers.get('WARC-Target-URI'), record.rec_headers.get('WARC-Date'), content_hash, record.rec_headers.get('WARC-Payload-Digest'), record.rec_headers.get('Content-Type'), record.rec_headers.get('Content-Length'), mime, statusline, loc, warc_file_id, fulltext_id))

                    con.commit()
