import os
import re
import csv
import sys

input_folder = '/mnt/data/veidemann/validwarcs'
index_folders = os.listdir(input_folder)
csvwriter = csv.writer(sys.stdout)
crawl_id = 0

for folder in index_folders:
    folder_path = os.path.join(input_folder, folder)
    for root,dirs,files in os.walk(folder_path):
        dirs[:] = [d for d in dirs if not re.findall('screenshot|dns', d.lower())]

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
