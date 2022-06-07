import re
import os
import sys
import config as c
import multiprocessing
import subprocess
import psycopg2 as pg

input_folder = '/mnt/deduppen1'
index_folders = ['kommuner']
next_crawl_id = None

def create_partition(crawl_name):
    with pg.connect(user=c.user, password=c.password, database=c.database, host=c.host, port=c.port) as con:
        cur = con.cursor()
        cur.execute("SELECT max(crawl_id)+1 FROM crawls;")
        next_crawl_id = cur.fetchone()

        if next_crawl_id[0]:
            next_crawl_id = next_crawl_id[0]
        else:
            next_crawl_id = 1

        try:
            cur.execute("INSERT INTO crawls(crawl_id, name) VALUES (%s,%s);", (next_crawl_id, crawl_name))

            cur.execute("CREATE TABLE IF NOT EXISTS warcinfo_{crawl_name} PARTITION OF warcinfo FOR VALUES IN (%s);".format(crawl_name=crawl_name), (next_crawl_id,))

            cur.execute("CREATE TABLE IF NOT EXISTS fulltext_{crawl_name} PARTITION OF fulltext FOR VALUES IN (%s);".format(crawl_name=crawl_name), (next_crawl_id,))
        except:
            return next_crawl_id-1
        return next_crawl_id

def run_in_docker(job):
    crawl_id = str(job[0])
    file_path = job[1]
    # run docker container
    docker_command = ['docker', 'run', '--rm', '--network=lan', '-v', '{input_folder}:{input_folder}'.format(input_folder=input_folder), '-it', 'langdet:test8', "python3", "process-warc-html.py", file_path, crawl_id]
    subprocess.run(docker_command)

jobs = []

for folder in index_folders:
    for root,dirs,files in os.walk(input_folder):
        dirs[:] = [d for d in dirs if not re.findall('screenshot|dns', d.lower())]

        if root[len(input_folder):].count(os.sep) == 2:
            crawl_name = root.split('/')[-1]
            next_crawl_id = create_partition(crawl_name)

        # create crawl
        if next_crawl_id:
            for file in files:
                if file.endswith(".warc.gz"):
                    file_path = os.path.join(root, file)
                    job = [next_crawl_id, file_path]
                    jobs.append(job)


pool = multiprocessing.Pool(10)

for i in pool.imap_unordered(run_in_docker, jobs):
    print(i)
