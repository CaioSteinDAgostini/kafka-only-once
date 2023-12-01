# Kafka Stream Only-Once

The purpose of this project is to show the common bank ballance example, where there is a stream that listens to a transactions topic and calculates the current ballance for each account.
And, later, test the topology of the Stream.

## what is not here
starting kafka and creating the topics

```bash
kafka-topics.sh  --bootstrap-server localhost:9092 --create  --topic transaction
```
```bash
kafka-topics.sh  --bootstrap-server localhost:9092 --create  --topic balance
```

## About the tests

Kafka's stream API has evolved a lot and there are a lot os deprecated methods, some have already been removed.
Thus, it is difficult to find a complete and current example. So, some times, it is necessary to go by reading the documentation.

### some exception are not what they seem to be

If the topology has steps that require keys, and:
- when you input data into the "pipeInput" withouk a key,
- when you try asserting the results you do a "output.getValue()"

we get an exception

```bash
 produces Tests run: 1, Failures: 0, Errors: 1, Skipped: 0, Time elapsed: 0.235 s <<< FAILURE! -- in com.learning.kafka.only.once.KafkaStreamTransactionTest
com.learning.kafka.only.once.KafkaStreamTransactionTest.testTopology -- Time elapsed: 0.224 s <<< ERROR!
java.util.NoSuchElementException: Uninitialized topic: balance
```

looking at that printstack, it seens you did not declare the output topic while setting the tests. However, that is not the issue. The issue is that the test run and no record where delivered to the output. If you look a little higher, in the logged messages, there is that:

```bash
[main] WARN org.apache.kafka.streams.kstream.internals.KStreamAggregate - Skipping record due to null key or value. topic=[transaction] partition=[0] offset=[0]
```

So, if that happens, just remember to pipe a record with key and value.

```java
input.pipeInput(new TestRecord<>(key, value));
```

