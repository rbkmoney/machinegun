#!/bin/bash
cat <<EOF
# https://hub.docker.com/r/basho/riak-kv/
version: '2'

services:
  ${SERVICE_NAME}:
    image: ${BUILD_IMAGE}
    volumes:
      - .:$PWD
      - $HOME/.cache:/home/$UNAME/.cache
    working_dir: $PWD
    command: /sbin/init
    depends_on:
      - riakdb

  riakdb:
    image: basho/riak-kv:ubuntu-2.1.4
    ports:
      - "8087:8087"
      - "8098:8098"
    environment:
      - CLUSTER_NAME=riakkv
    labels:
      - "com.basho.riak.cluster.name=riakkv"
    volumes:
      - schemas:/etc/riak/schemas
  member:
    image: basho/riak-kv:ubuntu-2.1.4
    ports:
      - "8087"
      - "8098"
    labels:
      - "com.basho.riak.cluster.name=riakkv"
    links:
      - riakdb
    depends_on:
      - riakdb
    environment:
      - CLUSTER_NAME=riakkv
      - COORDINATOR_NODE=riakdb

volumes:
  schemas:
    external: false
networks:
  default:
    driver: bridge
    driver_opts:
      com.docker.network.enable_ipv6: "true"
      com.docker.network.bridge.enable_ip_masquerade: "false"

EOF
