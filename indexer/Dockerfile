FROM python:3.11-slim-bullseye

# Create user
RUN useradd -ms /bin/bash indexer
USER indexer

WORKDIR /home/indexer

# Setup virtual environment
ENV VIRTUAL_ENV=/home/indexer/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip; pip install --use-pep517 -r requirements.txt

# Replace stopword dictionaries in justext
RUN rm $VIRTUAL_ENV/lib/python3.11/site-packages/justext/stoplists/*
COPY models/stoplists_justext/* $VIRTUAL_ENV/lib/python3.11/site-packages/justext/stoplists/

# Copy app
COPY config.py index.py ./

ENTRYPOINT ["python3", "index.py"]
