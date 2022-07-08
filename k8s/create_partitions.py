import csv
import sys
import psycopg2 as pg
import config as c

partitions = set()

def create_partition(partition_id, partition_name):
    with pg.connect(user=c.user, password=c.password, database=c.database, host=c.host, port=c.port) as con:
        cur = con.cursor()

        cur.execute('INSERT INTO crawls(crawl_id, name) VALUES (%s,%s);', (partition_id, partition_name))

        cur.execute('CREATE TABLE IF NOT EXISTS "warcinfo_{partition_name}" PARTITION OF warcinfo FOR VALUES IN (%s);'.format(partition_name=partition_name), (partition_id,))

        cur.execute('CREATE TABLE IF NOT EXISTS "fulltext_{partition_name}" PARTITION OF fulltext FOR VALUES IN (%s);'.format(partition_name=partition_name), (partition_id,))


for line in csv.reader(iter(sys.stdin.readline, '')):
    partition_id = line[0]
    partition_name = line[1]

    pair= (partition_id, partition_name)
    partitions.add(pair)

for partition in partitions:
    create_partition(partition[0], partition[1])

