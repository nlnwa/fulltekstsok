FROM python:3.11-slim-bullseye

# Create user
RUN useradd -ms /bin/bash helper
USER helper

WORKDIR /home/helper

# Setup virtual environment
ENV VIRTUAL_ENV=/home/helper/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Replace stopword dictionaries in justext
RUN rm $VIRTUAL_ENV/lib/python3.11/site-packages/justext/stoplists/*
COPY models/stoplists_justext/* $VIRTUAL_ENV/lib/python3.11/site-packages/justext/stoplists/

# Copy helper scripts
COPY . .
