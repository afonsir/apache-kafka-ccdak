## Docker

- Connect to a broker:

```bash
docker container exec --interactive --tty apache-kafka-ccdak_kafka-1_1 bash
```

## Create a Producer and a Consumer

- Create the topic _thermostat.readings_:

```bash
kafka-topics \
  --bootstrap-server localhost:19092 \
  --create \
  --topic thermostat.readings
```

- Create the producer and add some messages:

```bash
kafka-console-producer \
  --bootstrap-server localhost:29092 \
  --topic inventory.purchases
```

```json
{ "sensor_id": 42, "location": "kitchen", "temperature": 22, "read_at": 1736521921 }
{ "sensor_id": 151, "location": "bedroom", "temperature": 20, "read_at": 1736521923 }
{ "sensor_id": 299, "location": "living room", "temperature": 21, "read_at": 1736521926 }
{ "sensor_id": 42, "location": "kitchen", "temperature": 24, "read_at": 1736521981 }
```

- Create the consumer:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:39092 \
  --topic inventory.purchases \
  --from-beginning
```

- Create the consumer (showing message key):

```bash
kafka-console-consumer \
  --bootstrap-server localhost:39092 \
  --topic inventory.purchases \
  --from-beginning \
  --property print.key=true
```

- Create the consumer (parsing message key):

```bash
kafka-console-consumer \
  --bootstrap-server localhost:39092 \
  --topic inventory.purchases \
  --from-beginning \
  --property parse.key=true \
  --property key.separator=:
```
