#!/usr/bin/env python3

import sys
import pathlib
import json
import uuid
from kafka import KafkaProducer

sourceDir = sys.argv[1]
kafkaServers = tuple(sys.argv[2].split(","))
targetTopic = sys.argv[3]

print("Source directory: ", sourceDir)
print("Kafka bootstrap servers: ", kafkaServers)
print("Target topic: ", targetTopic)
print("\n\n")

producer = KafkaProducer(bootstrap_servers=kafkaServers, key_serializer=str.encode, value_serializer=lambda x:json.dumps(x).encode('utf-8'))

def send(record):
    future = producer.send(topic = targetTopic, key = record["guid"], value = record)
    record_metadata = future.get(timeout=10)
    print('--- Message: {}'.format(record)) 
    print('--> The message has been sent to a topic: \
            {}, partition: {}, offset: {}' \
            .format(record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset ))
    print("\n")

def processLine(line):
    record = json.loads(line)
    record["guid"] = str(uuid.uuid4())
    if "id" in record:
        del record["id"]
    send(record)

def readFile(file):
    print("Processing file: ", file, "\n")
    with open(file, 'r') as f:
        for line in f:
            processLine(line)

files = [p for p in pathlib.Path(sourceDir).iterdir() if p.is_file()]

for file in files:
    readFile(file)


producer.flush()
