FROM python:3.7.3-slim
MAINTAINER snuids

RUN apt-get update
RUN apt-get install -y vim

COPY ./requirements.txt /opt/sources/requirements.txt
RUN pip install -r /opt/sources/requirements.txt 

COPY ./sources /opt/sources
RUN rm -d -r /opt/sources/logs
RUN rm -d -r /opt/sources/outputs
RUN mkdir  /opt/sources/logs

WORKDIR /opt/sources

CMD ["python", "nyx_rest_api_plus.py"]
