docker build ../ -t bg-image -f bg-image/Dockerfile
docker build aggregator-cache/ -t aggregator-cache -f aggregator-cache/Dockerfile
docker build graphite-web/ -t graphite-web -f graphite-web/Dockerfile
docker network create biggraphite-network 
