#!/bin/bash
####
#README
####
# load balancer binds to localhost:9999
# localhost:9000/sessions ...


docker build -t ws_test_server server/.

docker network create ws_test_network || echo 'Network created'

docker rm -f redis_ws_test
docker run -d --name=redis_ws_test --network ws_test_network redis

for port in 9000 9001 9002
do
    docker rm -f ws_server$port
    docker run -d --network ws_test_network --name ws_server$port --env RUN_PORT=$port --env REDIS_HOST=redis_ws_test  -p $port:$port ws_test_server
done

#nginx proxy binds to 9999 load balancing beetween 3 preconfigured servers from ws_server.conf
docker rm -f proxy
docker run -d --name=proxy --network ws_test_network -p 9999:9999 -v $PWD/nginx/ws_server.conf:/etc/nginx/conf.d/default.conf nginx

