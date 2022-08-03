from warcio.archiveiterator import ArchiveIterator
import config as c
import justext
import psycopg2 as pg
import os
import uuid
import hashlib
import sys
import cchardet
import lxml
import csv

# Justext options (magbb)
MAX_LINK_DENSITY = 0.4
MAX_HEADING_DISTANCE = 150
LENGTH_LOW = 70
LENGTH_HIGH = 200
STOPWORDS_LOW = 0.30
STOPWORDS_HIGH = 0.32
NO_HEADINGS = False

# INSPIRED BY: https://github.com/bitextor/bitextor/blob/master/bitextor-warc2htmlwarc.py and JUSTEXT
def convert_encoding(data):
    if len(data) > 0:
        # first try: strict utf-8
        try:
            decoded = data.decode('utf-8', errors='strict')
            return data
        except:
            pass

        # guess encoding, try fallback encodings
        try_encs = ['iso-8859-1', 'windows‑1252']
        try:
            encoding = cchardet.detect(data)['encoding']
            try_encs.insert(0, encoding)
        except:
            pass

        for enc in try_encs:
            try:
                decoded = data.decode(enc)
                return decoded.encode('utf-8')
            except:
                pass

        # last fallback: utf-8 with replacements
        try:
            decoded = data.decode('utf-8', errors='replace')
            return decoded.encode('utf-8')
        except:
            pass

    return None

def get_all_stop_words():
    stop_words = set()
    for language in justext.get_stoplists():
        stop_words.update(justext.get_stoplist(language))
    return stop_words

def removeBP(utf_stream, stop_words):
    if utf_stream:
        paragraphs = justext.justext(utf_stream, stop_words, encoding='utf-8', length_low=LENGTH_LOW, length_high=LENGTH_HIGH, stopwords_low=STOPWORDS_LOW, stopwords_high=STOPWORDS_HIGH, max_link_density=MAX_LINK_DENSITY, max_heading_distance=MAX_HEADING_DISTANCE, no_headings=NO_HEADINGS)
        return [paragraph["text"] for paragraph in paragraphs if paragraph["class"] == "good"]
    else:
        return ['']

def extract_content(file, con):
    cur = con.cursor()

    try:
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
                try:
                    # filter only status_code 200 HTML responses
                    if record.rec_type == 'response':
                        try:
                            mime = record.http_headers.get('Content-Type')
                        except:
                            continue

                        if mime:
                            if mime.startswith('text/html'):
                                statusline = record.http_headers.statusline
                                if statusline:
                                    if statusline.startswith('200'):
                                        loc = record.http_headers.get('Location')

                                        # we create a hash of the content stream for deduplication (see the problem in: https://github.com/webrecorder/warcio/issues/74)
                                        try:
                                            content_stream = record.content_stream().read()
                                        except:
                                            print("problem reading content stream... skipping", record.rec_headers.get('WARC-Record-ID'))
                                            continue
                                        content_hash = hashlib.sha1(content_stream).hexdigest()

                                        try:
                                            # decode and remove boilerplate
                                            utf_stream = convert_encoding(content_stream)
                                            html_content = removeBP(utf_stream, stopwords)
                                        except:
                                            print("problem loading HTML... skipping", record.rec_headers.get('WARC-Record-ID'))
                                            continue

                                        para = "\n".join(html_content)
                                        hashStr=hashlib.sha1(para.encode('utf-8')).hexdigest()

                                        fulltext_sql = """INSERT INTO fulltext(fulltext_hash, crawl_id, fulltext) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING;"""
                                        cur.execute(fulltext_sql, (hashStr, crawl_id, para))

                                        # INSERT WARCINFO
                                        warcinfo_sql = """INSERT INTO warcinfo(record_id, crawl_id, type, concurrent_to, target_uri, date, content_hash, payload_digest, content_type, content_length, response_mime_type, response_status, redirect_location, warc_file_id, fulltext_hash)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                                        cur.execute(warcinfo_sql, (record.rec_headers.get('WARC-Record-ID'), crawl_id, record.rec_headers.get('WARC-Type'), record.rec_headers.get('WARC-Concurrent-To'), record.rec_headers.get('WARC-Target-URI'), record.rec_headers.get('WARC-Date'), content_hash, record.rec_headers.get('WARC-Payload-Digest'), record.rec_headers.get('Content-Type'), record.rec_headers.get('Content-Length'), mime, statusline, loc, warc_file_id, hashStr))
                    elif record.rec_type == 'revisit':
                        try:
                            mime = record.http_headers.get('Content-Type')
                        except:
                            continue
                        if mime:
                            if mime.startswith('text/html'):
                                statusline = record.http_headers.statusline
                                if statusline:
                                    if statusline.startswith('200'):
                                        loc = record.http_headers.get('Location')

                                        # INSERT WARCINFO
                                        warcinfo_sql = """INSERT INTO warcinfo(record_id, crawl_id, type, concurrent_to, refers_to, target_uri, date, payload_digest, content_type, content_length, response_mime_type, response_status, redirect_location, warc_file_id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                                        cur.execute(warcinfo_sql, (record.rec_headers.get('WARC-Record-ID'), crawl_id, record.rec_headers.get('WARC-Type'), record.rec_headers.get('WARC-Concurrent-To'), record.rec_headers.get('WARC-Refers-To'), record.rec_headers.get('WARC-Target-URI'), record.rec_headers.get('WARC-Date'), record.rec_headers.get('WARC-Payload-Digest'), record.rec_headers.get('Content-Type'), record.rec_headers.get('Content-Length'), mime, statusline, loc, warc_file_id))

                        con.commit()
                except:
                    print("Error in WARC record", file)
                    continue
    except:
        print("General error or error opening file:", file)

stopwords = get_all_stop_words()

input_file = sys.argv[1]

with open(input_file, 'r') as f:
    csvreader = csv.reader(f)

    for row in csvreader:
        crawl_id = int(row[0])
        file = row[2]

        try:
            with pg.connect(user=c.user, password=c.password, database=c.database, host=c.host, port=c.port) as con:

                if file.endswith('warc.gz') and not file.endswith('meta.warc.gz'):
                    print(file)
                    extract_content(file, con)
        except:
            continue
