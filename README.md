# Fulltext search of WARCS in Kubernetes

## Build containers image for WARC processing
```bash
minikube image build webapp/ -t ghcr.io/nlnwa/fts-webapp:latest
minikube image build indexer/ -t ghcr.io/nlnwa/fts-indexer:latest
```

## Deploy

```bash
kubectl apply -k deploy/
```
