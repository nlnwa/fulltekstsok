FROM ubuntu:20.04
WORKDIR /code
COPY requirements.txt /code

# install packages
ENV TZ=Europe/Oslo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update && apt-get install tesseract-ocr libtesseract-dev libleptonica-dev pkg-config python3 python3-pip -y
RUN pip install -r requirements.txt

# copy processing scripts
COPY config_docker.py config.py
COPY create_partitions.py .
COPY get_crawls.py .

# create a entrypoint
RUN ["python3"]
