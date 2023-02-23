import os
import re
import csv
import sys

if len(sys.argv) == 0:
    print("Missing argument (path to veidemann warc collections)")
    exit(1)

input_folder = sys.argv[1]
index_folders = os.listdir(input_folder)
csvwriter = csv.writer(sys.stdout)
crawl_id = 0

for folder in index_folders:
    folder_path = os.path.join(input_folder, folder)
    for root, dirs, files in os.walk(folder_path):
        # don't decend into folders containing screenshot or dns
        dirs[:] = [d for d in dirs if not re.findall('screenshot|dns', d.lower())]

        # create new crawls for all folders on level 2 (news_2020, news_2021 etc.)
        if root[len(input_folder):].count(os.sep) == 2:
            crawl_name = root.split('/')[-1]
            crawl_id += 1

        # create crawl
        if crawl_id > 0:
            for file in files:
                if file.endswith(".warc.gz"):
                    file_path = os.path.join(root, file)
                    job = [crawl_id, crawl_name, file_path]
                    csvwriter.writerow(job)
