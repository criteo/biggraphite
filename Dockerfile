FROM python:3

# Install the latest pythons plus a few build deps.
RUN apt-get update && \
	apt-get dist-upgrade -y && \
	apt-get install -y pypy pypy-dev python-dev python3-dev libffi-dev less htop python-virtualenv && \
	apt-get clean

# Change this to use another version of python.
RUN virtualenv /opt/venv -p python3

ENV GRAPHITE_NO_PREFIX=true

# Install upstream packages
RUN /opt/venv/bin/pip install graphite-web carbon biggraphite

# Some extra-packages
RUN /opt/venv/bin/pip install flask_restplus gourde

# Copy config and init.
COPY contrib/docker/conf/carbon.conf /opt/venv/conf/
COPY contrib/docker/conf/local_settings.py /opt/venv/lib/python3.7/site-packages/graphite/
COPY contrib/docker/bginit.sh /opt/venv/bin/
