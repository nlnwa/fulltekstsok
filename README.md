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
