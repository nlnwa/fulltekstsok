# Helper scripts to generate index configuration

This folder contains helper scripts that presupposes a certain folder structure.  Both scripts output CSV formatted lines in the format:
```csv
crawl_id, crawl_name, file_path
```

### get_crawls.py

Given an `input_folder` containing the following structure:

```shell
collection/collection_dedup_policy_a/**/warc_file.warc.gz
collection/collection_dedup_policy_b/**/warc_file.warc.gz
```

then running

```shell
python get_crawls.py input_folder/
```

will result in crawls named `collection_dedup_policy_a` and `collection_dedup_policy_b`:

```csv
0, coll_dedup_policy_a, .../input_folder/coll/coll_dedup_policy_a/**/warc_file.warc.gz
1, coll_dedup_policy_a, .../input_folder/coll/coll_dedup_policy_b/**/warc_file.warc.gz
```

### get_crawls_heritrix.py

Given an `input_folder` containing the following structure:

```shell
a/**/warc_file.warc.gz
b/**/warc_file.warc.gz
```
then running

```shell
python get_crawls_heritrix.py input_folder/
```

will result in crawls named  `heritrix_a` and `heritrix_b`:

```csv
0, heritrix_a, .../input_folder/a/**/warc_file.warc.gz
1, heritrix_b, .../input_folder/b/**/warc_file.warc.gz
```


