FROM python:3.6
RUN  pip install cassandra-driver
COPY . /bg/
WORKDIR /bg
ENV GRAPHITE_NO_PREFIX=true
RUN pip install graphite-web \
    && pip install -r requirements.txt \
    && pip install -e .
ENTRYPOINT ["/bin/bash", "/conf/launch-graphite-web.sh"]
