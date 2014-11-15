#!/bin/bash

zkQuorum=$1":9092"

java -jar KafkaMsgProducer-assembly-1.0.jar $zkQuorum
