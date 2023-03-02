import concurrent.futures
import hashlib
import logging
import os
import pathlib
import re
import signal
import sys
import time
import getopt
import cchardet
import justext
import psycopg2 as pg
from warcio.archiveiterator import ArchiveIterator

import config as c

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

# Justext options (magbb)
MAX_LINK_DENSITY = 0.4
MAX_HEADING_DISTANCE = 150
LENGTH_LOW = 70
LENGTH_HIGH = 200
STOPWORDS_LOW = 0.30
STOPWORDS_HIGH = 0.32
NO_HEADINGS = False

warcfile_check_sql = """SELECT warc_file_name FROM warc_files WHERE warc_file_name=%s;"""
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
crawls_sql = """WITH e AS
            (INSERT INTO crawls VALUES(DEFAULT, %s) ON CONFLICT (name) DO UPDATE SET name = NULL WHERE FALSE RETURNING crawl_id)
            SELECT crawl_id FROM e
            UNION ALL
            SELECT crawl_id FROM crawls WHERE name = %s LIMIT 1;"""


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
    total = 0
    count = 0

    with open(file, 'rb') as stream:
        with conn:
            with conn.cursor() as cur:
                cur.execute(warcfile_sql, (file, file))
                warc_file_id = cur.fetchone()[0]

        for record in ArchiveIterator(stream):
            total += 1
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
            count += 1

            warc_record_id = record.rec_headers.get('WARC-Record-ID')
            loc = record.http_headers.get('Location')

            try:
                if record.rec_type == 'response':
                    # we create a hash of the content stream for deduplication (see the problem in: https://github.com/webrecorder/warcio/issues/74)
                    try:
                        content_stream = record.content_stream().read()
                    except Exception as e:
                        logging.warning(f"Failed to read content stream... skipping: {file}@{warc_record_id}: {e}")
                        continue

                    try:
                        # decode and remove boilerplate
                        utf_stream = convert_encoding(content_stream)
                        if utf_stream is None:
                            logging.warning(f"Failed to convert encoding... skipping: {file}@{warc_record_id}")
                            continue

                        if len(utf_stream) > 3000000:
                            logging.warning(f"Very long document... skipping: {file}@{warc_record_id}")
                            continue

                    except Exception as e:
                        logging.warning(f"Problem loading HTML... skipping: {file}@{warc_record_id}: {e}")
                        continue

                    try:
                        html_content = remove_bp(utf_stream, stopwords)
                    except Exception as e:
                        logging.warning(f"Failed to remove boilerplate... skipping: {file}@{warc_record_id}: {e}")
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
                            f"Failed to save warc info for response record: {file}@{warc_record_id}: {e}")

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
                            f"Failed to save warc info for revisit record {file}@{warc_record_id}: {e}")
            except Exception as e:
                logging.error(f"Error processing WARC record: {file}@{warc_record_id}: {e}")
    return count, total


stopwords = get_all_stop_words()


def create_partition(conn, partition):
    logging.info(f"Create partition if not exists: {partition}")
    with conn:
        with conn.cursor() as cur:
            cur.execute(crawls_sql, (partition, partition))
            crawl_id = cur.fetchone()[0]

            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "warcinfo_{partition}" PARTITION OF warcinfo FOR VALUES IN (%s);',
                (crawl_id,))
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "fulltext_{partition}" PARTITION OF fulltext FOR VALUES IN (%s);',
                (crawl_id,))
    return crawl_id


def process_file(file, crawl_id):
    conn = getconn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(warcfile_check_sql, (file,))
                if cur.fetchone() is not None:
                    logging.warning(f"Already indexed... skipping file: {file}")
                    return

        start = time.perf_counter()
        count, total = extract_content(file, crawl_id, conn)
        end = time.perf_counter()
        if count and total:
            logging.info(f"Processed {count} of {total} records in {round(end - start, 3)} seconds: {file}")
    except Exception as error:
        logging.error(f"Error processing file {file}: {error}")
    finally:
        conn.close()


def getconn():
    return pg.connect(user=c.user, password=c.password, database=c.database, host=c.host, port=c.port)


def main(root_dir, collection, max_workers=None):
    """Traverse root directory and process WARC files.
    The root directory is expected to contain only subdirectories representing crawl name.
    Each combination of crawl name and collection will become a partition."""

    start = time.perf_counter()
    count = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers) as executor:
        logging.info(f"Using maximum {executor._max_workers} workers")
        signal.signal(signal.SIGTERM, lambda _: executor.shutdown(wait=True, cancel_futures=True))
        conn = getconn()
        results = []
        crawl_id = ""
        root_dir = str(pathlib.PurePath(root_dir))
        for curr_dir, dirs, files in os.walk(root_dir):
            # don't decend into folders containing screenshot or dns
            dirs[:] = [d for d in dirs if not re.findall('screenshot|dns', d.lower())]

            # Create partition for directories at level 1
            if curr_dir[len(root_dir):].count(os.sep) == 1:
                crawl_name = pathlib.PurePath(curr_dir).name
                partition = f"{collection}_{crawl_name}"
                try:
                    crawl_id = create_partition(conn, partition)
                except pg.Error as e:
                    logging.error(f'Failed to get crawl id for "{partition}"... aborting: {e}')
                    break

            if curr_dir == root_dir:
                logging.warning(f'Skipping files in root directory: {curr_dir}')
                continue

            if not crawl_id:
                logging.warning(f'Missing crawl_id... skipping folder: {curr_dir}')
                continue

            files = [file for file in files if
                     file.endswith(".warc.gz") and not file.endswith("meta.warc.gz")]
            count += len(files)
            for file in files:
                results.append(executor.submit(process_file, os.path.join(curr_dir, file), crawl_id))

        conn.close()
        concurrent.futures.wait(results)

    end = time.perf_counter()
    logging.info(f"Processed {count} files in {round(end - start, 2)} seconds")


if __name__ == "__main__":
    root_dir = None
    collection = "default"
    max_workers = None

    opts, args = getopt.getopt(sys.argv[1:], "c:w:",
                               ["collection=", "max-workers="])
    if len(args) < 1:
        logging.error("Missing required argument: <path to directory>")
        exit(1)
    else:
        root_dir = args[0]

    if not os.path.isdir(root_dir):
        logging.error(f"Path is not a directory: {root_dir}")
        exit(1)

    for opt, arg in opts:
        if opt in ['-c', '--collection']:
            collection = arg
        elif opt in ['-w', '--max-workers']:
            max_workers = int(arg)

    main(root_dir=root_dir, collection=collection, max_workers=max_workers)
