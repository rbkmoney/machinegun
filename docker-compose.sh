#!/bin/bash
#
# Copyright 2017 RBKmoney
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CONFLUENT_PLATFORM_VERSION="5.1.2"  # with Kafka 2.1.1
CONSUL_VERSION=1.4.2

cat <<EOF
# https://hub.docker.com/r/basho/riak-kv/
version: '2'

services:
  ${SERVICE_NAME}:
    image: ${BUILD_IMAGE}
    volumes:
      - .:$PWD
      - $HOME/.cache:/home/$UNAME/.cache
      - $HOME/.ssh:/home/$UNAME/.ssh:ro
    working_dir: $PWD
    command: /sbin/init
    depends_on:
      - riakdb
      - kafka1
      - kafka2
      - kafka3

  riakdb:
    image: dr.rbkmoney.com/basho/riak-kv:ubuntu-2.1.4-1
    environment:
      - CLUSTER_NAME=riakkv
    labels:
      - "com.basho.riak.cluster.name=riakkv"
    volumes:
      - ./test_resources/riak_user.conf:/etc/riak/user.conf:ro
      - schemas:/etc/riak/schemas
  member1:
    &member-node
    image: dr.rbkmoney.com/basho/riak-kv:ubuntu-2.1.4-1
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
      - ./test_resources/riak_user.conf:/etc/riak/user.conf:ro
  member2:
    <<: *member-node

  zookeeper:
    image: confluentinc/cp-zookeeper:${CONFLUENT_PLATFORM_VERSION}
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka1: &kafka-broker
    image: confluentinc/cp-kafka:${CONFLUENT_PLATFORM_VERSION}
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092
  kafka2:
    <<: *kafka-broker
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka2:9092
  kafka3:
    <<: *kafka-broker
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka3:9092

  consul1: &consul-server
    image: consul:${CONSUL_VERSION}
    volumes:
      - ./test_resources/consul.json:/etc/consul/consul.json
    hostname: consul1
    command:
      agent -server -config-dir=/etc/consul

  consul2:
    <<: *consul-server
    hostname: consul2

  consul0:
    <<: *consul-server
    hostname: consul0

volumes:
  schemas:
    external: false

EOF
