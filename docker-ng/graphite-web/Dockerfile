FROM bg-image
RUN pip install graphite-web
COPY local_settings.py /usr/local/lib/python3.6/site-packages/graphite/
COPY start-graphite-web.sh /bg
RUN chmod +x /bg/start-graphite-web.sh
ENTRYPOINT /bg/start-graphite-web.sh 
