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
docker build -t harbor.nb.no/sprakbanken/nlwa_index_helper .
docker push harbor.nb.no/sprakbanken/nlwa_index_helper
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
split -d -n l/10 --numeric-suffixes=1 --additional-suffix=.csv crawls.csv x-
cd ..
```

## create jobs based on template in jobs/
```bash
mkdir jobs
for i in `seq -f "%02g" 1 10`
do
  cat job-template.yaml | sed "s/-01/-$i/" > jobs/job-$i.yaml
done
```

## build docker image for WARC processing
```bash
cd ..
docker build -t harbor.nb.no/sprakbanken/nlwa_index_html .
docker push harbor.nb.no/sprakbanken/nlwa_index_html
```

## start processing

```bash
kubectl apply -f jobs
```
