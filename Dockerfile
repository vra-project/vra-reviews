# syntax=docker/dockerfile:1
FROM python:3.9.13

WORKDIR /app
COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY auto_review_aws.py auto_review.py
COPY secrets.toml secrets.toml

CMD ["python3", "auto_review.py"]