#!/usr/bin/env bash

kafka-topics.sh --zookeeper localhost:2181 --create --topic topic-2 --partitions 10 --replication-factor 1

for i in {1..1000}; do
    echo "$i:{\"message\":\"$i\"}" | kafkacat -P -b localhost:9093 -t topic-3 -K:
done