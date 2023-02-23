FROM python:3.11-slim-bullseye

RUN useradd -ms /bin/bash app
USER app

WORKDIR /home/app

# Setup virtual environment
ENV VIRTUAL_ENV=/home/app/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m venv $VIRTUAL_ENV

# Install python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --use-pep517 -r requirements.txt

# copy working files to the working directory
COPY app.py config.py ./

ENV STREAMLIT_SERVER_PORT="8501"
ENV STREAMLIT_SERVER_BASE_URL_PATH="/"
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS="false"

CMD ["streamlit", "run", "app.py"]
