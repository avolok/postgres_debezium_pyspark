FROM python:3.10-slim-buster

ADD app.py .

COPY requirements.txt ./

RUN pip install -r ./requirements.txt

CMD [ "python3", "-u", "./app.py"]