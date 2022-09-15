# Processing Nettarkivet in k8s

## first-time setup

### PV claim for internal SSD of gpu1
```bash
kubectl apply -f k8s/gpu1_pvc.yaml
```

### PV claim for deduppen1 via NFS
```bash
kubectl apply -f k8s/deduppen1_pvc.yaml
```

### setup postgres
```bash
kubectl apply -f k8s/postgres_configmap.yaml
kubectl apply -f k8s/postgres_secrets.yaml
kubectl apply -f k8s/postgres_depolyment.yaml
kubectl apply -f k8s/postgres_service.yaml
```

## create schema (DB nlwa_index must exsist first)
```bash
cat db/schema.sql | k exec -i sprakbank-postgres-POD -- psql -U admin -d nlwa_index
```

## build helper for accessing deduppen1
```bash
cd k8s
docker build -t registry.nb.no:5000/sprakbanken/nlwa_index_helper .
docker push registry.nb.no:5000/sprakbanken/nlwa_index_helper
kubectl apply -f nlwa-index-helper.yaml
```

## get crawls and file list from deduppen1 as a CSV
```bash
k exec -it nlwa-index-helper -- python3 get_crawls.py > crawls.csv
```

## create partitions for each crawl in the postgres DB (one for each crawl/collection)
```bash
cat crawls.csv | k exec -i nlwa-index-helper -- python3 create_partitions.py
```

## create batches (10) for the files
```bash
mkdir crawls && cp crawls.csv crawls && cd crawls
split -d -n l/10 --numeric-suffixes=1 --additional-suffix=.csv files_left.csv x-
```

## create jobs based on template in jobs/
```bash
for i in `seq -f "%02g" 1 10`
do
  cat job-template.yaml | sed "s/-01/-$i/" > jobs/job-$i.yaml
done
```

## build docker image for WARC processing
```bash
cd ..
docker build -t registry.nb.no:5000/sprakbanken/nlwa_index_html:2022-07-08-1
docker push registry.nb.no:5000/sprakbanken/nlwa_index_html:2022-07-08-1
```

## start processing

```bash
kubectl apply -f jobs
```

# Backup

## backup DB
```bash
k exec -i sprakbank-postgres-POD -- pg_dump -U admin -d nlwa_index > db.dump
```

## restore DB
```bash
cat db.dump | k exec -i sprakbank-postgres-POD -- psql -U admin -d nlwa_index
```


# OLD: LOCAL

## Create postgres DB

```bash
psql -c create database nlwa_index tablespace data;
psql -d nlwa_index -f db/schema.sql
```

## Mount WARC data (e.g. using sshfs)

## Harden postgres

```bash
docker network create lan
docker network inspect lan
```

- check/copy subnet
- example: 172.19.0.0/16

## restrict network access to server (example: 128.39.111.15)

```bash
sudo iptables --insert DOCKER-USER -s 172.19.0.0/16 ! -d 128.39.111.15 -j DROP
```

## add to postgres

```bash
sudo vim /etc/postgresql/12/main/pg_hba.conf
```
---
host    all             all             172.19.0.0/16           trust
---

## restart postgres

```bash
sudo systemctl restart postgresql
```

## build Docker image
```bash
docker build -t langdet .
```

## run!
```bash
python3 run.py
```
