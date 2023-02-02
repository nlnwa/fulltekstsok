FROM ubuntu:20.04
WORKDIR /code
COPY requirements.txt /code

# install packages
ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update && apt-get install tesseract-ocr libtesseract-dev libleptonica-dev pkg-config python3 python3-pip -y
RUN pip install -r requirements.txt

# copy stoplists for justext
RUN rm /usr/local/lib/python3.8/dist-packages/justext/stoplists/*
COPY models/stoplists_justext/* /usr/local/lib/python3.8/dist-packages/justext/stoplists/

# copy crawl info
COPY k8s/crawls jobs/

# copy processing scripts
COPY config_docker.py config.py
COPY process-warc-html.py .
COPY extract-links.py . 

# create user
RUN useradd -ms /bin/bash langdet
USER langdet

# create a entrypoint
RUN ["python3"]
