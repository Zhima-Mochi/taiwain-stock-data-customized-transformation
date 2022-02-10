FROM python:3.8

ENV POETRY_VERSION=1.1.12

RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

RUN poetry config virtualenvs.create false && poetry install --no-dev

COPY ./data_transformation /app/data_transformation
COPY ./main.py /app/main.py