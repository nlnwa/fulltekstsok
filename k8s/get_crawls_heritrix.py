import os
import re
import csv
import sys

input_folder = '/mnt/data/heritrix/no_crawl'
index_folders = os.listdir(input_folder)
csvwriter = csv.writer(sys.stdout)

for crawl_id,folder in enumerate(index_folders):
    folder_path = os.path.join(input_folder, folder)
    crawl_name = 'heritrix_%s' % (folder)
    for root,dirs,files in os.walk(folder_path):
        for file in files:
            if file.endswith(".warc.gz"):
                file_path = os.path.join(root, file)
                job = [crawl_id, crawl_name, file_path]
                csvwriter.writerow(job)
