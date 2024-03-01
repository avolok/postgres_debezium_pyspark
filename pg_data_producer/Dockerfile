FROM python:3.10-slim-buster

COPY app ./app

COPY requirements.txt ./setup/requirements.txt

RUN pip install -r ./setup/requirements.txt

WORKDIR /app

CMD ["python3", "-u", "./main.py"]