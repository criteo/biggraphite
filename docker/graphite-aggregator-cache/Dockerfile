FROM python:3.6
RUN  pip install cassandra-driver
COPY . /bg/
WORKDIR /bg
ENV GRAPHITE_NO_PREFIX=true
RUN  pip install carbon \
     && pip install -r requirements.txt \
     && pip install -e .
WORKDIR /conf
ENTRYPOINT ["bg-carbon-cache", "--debug", "--nodaemon", "--conf=carbon.conf", "start"]
