#!/usr/bin/env python3

import sys
import pathlib
import json
import uuid
import argparse
import time
from kafka import KafkaProducer
from json import JSONDecodeError

parser = argparse.ArgumentParser(description='Process json files and send records to Kafka')
parser.add_argument('-s', '--sourceDir', metavar='N', help='Source directory to read files', required=True)
parser.add_argument('-t', '--topic', metavar='N', help='Target topic', required=True)
parser.add_argument('-b', '--bootstrapServers', metavar='N', nargs='+', help='Kafka bootstrap servers')
parser.add_argument('-v', '--verbose', help='Increase output verbosity', action="store_true")

args = parser.parse_args()

print("======================================================")
for key, value in args._get_kwargs():
    if value is not None:
        print("{}:   {}".format(key, value))
print("======================================================\n")


producer = KafkaProducer(bootstrap_servers=tuple(args.bootstrapServers), key_serializer=str.encode, value_serializer=lambda x:json.dumps(x).encode('utf-8'))
linesProcessed = 0
messagesSent = 0
startTime = time.process_time()

def send(record):
    future = producer.send(topic = args.topic, key = record["guid"], value = record)
    
    global messagesSent
    messagesSent += 1
    
    if args.verbose:
        record_metadata = future.get(timeout=10)
        print(' Message[{}]: vehicle_id: [{}], trip_id: [{}], route_id: [{}]'.format(record["guid"], record["vehicle_id"], record["trip_id"], record["route_id"])) 
        print('   --> Topic: {}, partition: {}, offset: {}'.format(record_metadata.topic, record_metadata.partition, record_metadata.offset ))
        print("\n")


def processLine(line):
    record = parseJson(line)
    
    global linesProcessed
    linesProcessed += 1

    if record is not None:
        record["guid"] = str(uuid.uuid4())
        if "id" in record:
            del record["id"]
        send(record)

def parseJson(line):
    try:
        return json.loads(line)
    except JSONDecodeError as e:
        print("Unable to parse the line: [{}] - {}", line, e)
        return None

def readFile(file):
    print("Processing file: ", file, "\n")
    with open(file, 'r') as f:
        for line in f:
            processLine(line)

files = [p for p in pathlib.Path(args.sourceDir).iterdir() if p.is_file()]

for file in files:
    readFile(file)

elapsedTime = time.process_time() - startTime

if elapsedTime == 0:
    elapsedTime = 1

producer.flush()

throughput = linesProcessed / elapsedTime

print("Completed: {0} lines processed, {1} messages sent, throughput: {2:.2f} lines/sec".format(linesProcessed, messagesSent, throughput))
