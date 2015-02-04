import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "KafkaMsgProducer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.1.1"
