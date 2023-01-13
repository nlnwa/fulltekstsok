import psycopg2 as pg
import csv
import config as c
import sys
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen

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

def get_links(html, this_url):
    found_links = []
    soup = BeautifulSoup(html, "html.parser")

    links = soup.findAll('a')
    for link in links:
        content = link.get('href')
        anchor = link.text.strip()
        anchor = anchor.replace('\n', '')
        anchor = anchor.replace('\r', '')

        url = urlparse(content)

        if url.geturl() == b"":  # happens when there is no href or src attribute
            continue
        elif url.scheme in ["http", "https"]:
            target = url.geturl()
        elif url.netloc == "":
            target = urljoin(this_url, url.path)
        else:
            continue

        content = content.strip()

        # save the found connection and its type
        if "robots.txt" not in target and "mailto" not in target:
            found_links.append((target, anchor))

    return found_links

def process_warc(file, con):
    cur = con.cursor()
    try:
        with open(file, 'rb') as stream:
            # write warc file record, if existent return
            for record in ArchiveIterator(stream):
                try:
                    # filter only status_code 200 HTML responses
                    if record.rec_type == 'response':
                        mime = record.http_headers.get('Content-Type')

                        if mime:
                            if mime.startswith('text/html'):
                                statusline = record.http_headers.statusline
                                if statusline:
                                    if statusline.startswith('200'):
                                        loc = record.http_headers.get('Location')

                                        try:
                                            content_stream = record.content_stream().read()
                                            utf_stream = convert_encoding(content_stream)
                                        except:
                                            print("problem reading content stream... skipping", record.rec_headers.get('WARC-Record-ID'))
                                            continue

                                        try:
                                            this_url = record.rec_headers.get('WARC-Target-URI')
                                            out_links = get_links(utf_stream, this_url)
                                        except:
                                            print("error extracting links, skipping", record.rec_headers.get('WARC-Record-ID'))
                                            continue

                                        for link in out_links:
                                            cur.execute("INSERT INTO links VALUES(%s,%s,%s,%s);", (record.rec_headers.get('WARC-Record-ID'), crawl_id, link[0], link[1]))
                except:
                    print("Error in WARC record", file)
                    continue
                con.commit()
    except:
         print("General error or error opening file:", file)

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
                    process_warc(file, con)
        except:
            continue
