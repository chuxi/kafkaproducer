#!/bin/bash

zkQuorum=$1":9092"

java -jar target/scala-2.10/KafkaMsgProducer-assembly-1.0.jar $zkQuorum