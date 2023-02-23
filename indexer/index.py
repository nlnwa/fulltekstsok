import datetime
import hashlib
import logging
import os
import sys
import pathlib

import cchardet
import justext
import psycopg2 as pg
from psycopg2 import pool
from warcio.archiveiterator import ArchiveIterator

import config as c

# Justext options (magbb)
MAX_LINK_DENSITY = 0.4
MAX_HEADING_DISTANCE = 150
LENGTH_LOW = 70
LENGTH_HIGH = 200
STOPWORDS_LOW = 0.30
STOPWORDS_HIGH = 0.32
NO_HEADINGS = False

warcfile_sql = """WITH e AS (
            INSERT INTO warc_files(warc_file_name) VALUES(%s) ON CONFLICT DO NOTHING RETURNING warc_file_id
                    )
                SELECT * FROM e
                UNION
                SELECT warc_file_id FROM warc_files WHERE warc_file_name=%s;"""
fulltext_sql = """INSERT INTO fulltext(fulltext_hash, crawl_id, fulltext) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING;"""
warcinfo_response_sql = """INSERT INTO warcinfo(record_id, crawl_id, type, concurrent_to, target_uri, date, content_hash, payload_digest, content_type, content_length, response_mime_type, response_status, redirect_location, warc_file_id, fulltext_hash)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING ;"""
warcinfo_revisit_sql = """INSERT INTO warcinfo(record_id, crawl_id, type, concurrent_to, refers_to, target_uri, date, payload_digest, content_type, content_length, response_mime_type, response_status, redirect_location, warc_file_id)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING ;"""


# INSPIRED BY: https://github.com/bitextor/bitextor/blob/master/bitextor-warc2htmlwarc.py and JUSTEXT
def convert_encoding(data):
    if len(data) > 0:
        # first try: strict utf-8
        try:
            _ = data.decode('utf-8', errors='strict')
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
    return frozenset(stop_words)


def remove_bp(utf_stream, stop_words):
    """Remove boilerplate and stopwords."""
    if utf_stream:
        paragraphs = justext.justext(utf_stream, stop_words, encoding='utf-8', length_low=LENGTH_LOW,
                                     length_high=LENGTH_HIGH, stopwords_low=STOPWORDS_LOW,
                                     stopwords_high=STOPWORDS_HIGH, max_link_density=MAX_LINK_DENSITY,
                                     max_heading_distance=MAX_HEADING_DISTANCE, no_headings=NO_HEADINGS)
        return [p.text for p in paragraphs if not p.is_boilerplate]
    else:
        return ['']


def extract_content(file, crawl_id, conn):
    with open(file, 'rb') as stream:
        with conn:
            with conn.cursor() as cur:
                cur.execute(warcfile_sql, (file, file))
                warc_file_id = cur.fetchone()[0]

        for record in ArchiveIterator(stream):
            if record.rec_type != 'response' and record.rec_type != 'revisit':
                continue
            if not record.http_headers:
                continue

            # only status code 200
            statusline = record.http_headers.statusline
            if not statusline or not statusline.startswith('200'):
                continue
            # only content type text/html
            mime = record.http_headers.get('Content-Type')
            if not mime or not mime.startswith('text/html'):
                continue

            warc_record_id = record.rec_headers.get('WARC-Record-ID')
            loc = record.http_headers.get('Location')

            try:
                if record.rec_type == 'response':
                    # we create a hash of the content stream for deduplication (see the problem in: https://github.com/webrecorder/warcio/issues/74)
                    try:
                        content_stream = record.content_stream().read()
                    except:
                        logging.error(f"problem reading content stream... skipping: {warc_record_id}")
                        continue

                    try:
                        # decode and remove boilerplate
                        utf_stream = convert_encoding(content_stream)
                        if utf_stream is None:
                            logging.warning(f"Could not convert encoding... skipping: {warc_record_id}")
                            continue

                        if len(utf_stream) > 3000000:
                            logging.warning(f"Very long document... skipping: {warc_record_id}")
                            continue

                    except Exception as e:
                        logging.error(f"Problem loading HTML... skipping: {warc_record_id}: {e}")
                        continue

                    try:
                        html_content = remove_bp(utf_stream, stopwords)
                    except Exception as e:
                        logging.error(f"Failed to remove boilerplate... skipping: {warc_record_id}: {e}")
                        continue

                    content_hash = hashlib.sha1(content_stream).hexdigest()

                    para = "\n".join(html_content)
                    hashStr = hashlib.sha1(para.encode('utf-8')).hexdigest()

                    try:
                        with conn:
                            with conn.cursor() as cur:
                                cur.execute(fulltext_sql, (hashStr, crawl_id, para))
                                cur.execute(warcinfo_response_sql, (
                                    record.rec_headers.get('WARC-Record-ID'), crawl_id,
                                    record.rec_headers.get('WARC-Type'),
                                    record.rec_headers.get('WARC-Concurrent-To'),
                                    record.rec_headers.get('WARC-Target-URI'),
                                    record.rec_headers.get('WARC-Date'), content_hash,
                                    record.rec_headers.get('WARC-Payload-Digest'),
                                    record.rec_headers.get('Content-Type'),
                                    record.rec_headers.get('Content-Length'), mime, statusline, loc, warc_file_id,
                                    hashStr))
                    except pg.Error as e:
                        logging.error(
                            f"Failed to save warc info for response record {warc_record_id}: {e}")

                elif record.rec_type == 'revisit':
                    try:
                        with conn:
                            with conn.cursor() as cur:
                                cur.execute(warcinfo_revisit_sql, (
                                    record.rec_headers.get('WARC-Record-ID'), crawl_id,
                                    record.rec_headers.get('WARC-Type'),
                                    record.rec_headers.get('WARC-Concurrent-To'),
                                    record.rec_headers.get('WARC-Refers-To'),
                                    record.rec_headers.get('WARC-Target-URI'), record.rec_headers.get('WARC-Date'),
                                    record.rec_headers.get('WARC-Payload-Digest'),
                                    record.rec_headers.get('Content-Type'),
                                    record.rec_headers.get('Content-Length'), mime, statusline, loc, warc_file_id))
                    except pg.Error as e:
                        logging.error(
                            f"Failed to save warc info for revisit record {warc_record_id}: {e}")
            except Exception as e:
                logging.error(f"Error in WARC record: {file}: {e}")
                continue


stopwords = get_all_stop_words()


def create_partition(conn, crawl_name):
    logging.info(f"Creating new partition: {crawl_name}")
    with conn:
        with conn.cursor() as cur:
            crawls_sql = """WITH e AS
            (INSERT INTO crawls VALUES(DEFAULT, %s) ON CONFLICT (name) DO UPDATE SET name = NULL WHERE FALSE RETURNING crawl_id)
            SELECT crawl_id FROM e
            UNION ALL
            SELECT crawl_id FROM crawls WHERE name = %s LIMIT 1;"""
            cur.execute(crawls_sql, (crawl_name, crawl_name))
            crawl_id = cur.fetchone()[0]

            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "warcinfo_{crawl_name}" PARTITION OF warcinfo FOR VALUES IN (%s);',
                (crawl_id,))
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "fulltext_{crawl_name}" PARTITION OF fulltext FOR VALUES IN (%s);',
                (crawl_id,))
            return crawl_id


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')

    if len(sys.argv) == 0:
        logging.error("Missing argument (path to warc collections)")
        exit(1)

    input_folder = sys.argv[1]
    if not os.path.exists(input_folder):
        logging.error(f"Path does not exist: {input_folder}")
        exit(1)

    try:
        db = pool.SimpleConnectionPool(1, 20, user=c.user, password=c.password, database=c.database, host=c.host,
                                       port=c.port)
        conn = db.getconn()
    except (Exception, pg.DatabaseError) as error:
        logging.error(f"Failed to get database connection: {error}")
        exit(1)

    path = str(pathlib.PurePath(input_folder))
    for root, dirs, files in os.walk(path):
        # Create partitions for all directories at level 1
        if root[len(path):].count(os.sep) == 1:
            crawl_name = pathlib.PurePath(root).name
            crawl_id = create_partition(conn, crawl_name)

        for file in files:
            if not file.endswith(".warc.gz"):
                continue

            file_path = os.path.join(root, file)
            try:
                start = datetime.datetime.now()
                extract_content(file_path, crawl_id, conn)
                duration = (datetime.datetime.now() - start).total_seconds()
                logging.info(f"Indexed \"{file}\" in {duration} seconds")
            except Exception as error:
                logging.error(f"General error or error opening file: {file}: {error}")
