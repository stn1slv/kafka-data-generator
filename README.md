# Kafka data generator

Simple data generator to Kafka Topic

## Build executable file

```
go build -o kafka-data-generator
```

## Start generator
Generate 1000 messages in output topic for kafka broker (localhost:9092):
```
./kafka-data-generator -c 1000 -t output -b localhost:9092
```
Generate 5000 messages in output topic for kafka broker (localhost:9092) with silent mode:
```
./kafka-data-generator -c 5000 -t output -b localhost:9092 -s true
```
