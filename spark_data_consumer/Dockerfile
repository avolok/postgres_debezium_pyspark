FROM ubuntu:22.04

# Setup python and java and base system
ENV DEBIAN_FRONTEND noninteractive
ENV LANG=en_US.UTF-8

RUN apt-get update && \
  apt-get install -q -y openjdk-11-jdk python3-pip libsnappy-dev language-pack-en supervisor

RUN pip3 install --upgrade pip requests

ADD ./requirements.txt ./requirements.txt

RUN pip install -r  requirements.txt 

COPY app ./app

VOLUME [ "/data" ]

WORKDIR /app

CMD [ "python3", "-u", "./main.py"]