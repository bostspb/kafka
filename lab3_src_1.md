# Источник №1 для лабораторной №3


```Java
%%loadFromPOM
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>2.5.0</version>
</dependency>
```


```Java
%%loadFromPOM
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>1.7.25</version>
</dependency>
```


```Java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.sql.Timestamp;
```


```Java
String srcTopicName = "lab3-05-src01";

Properties propsPr = new Properties();
propsPr.put("bootstrap.servers", "localhost:9092");
propsPr.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
propsPr.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

Producer<String, String> producer = new KafkaProducer<String, String>(propsPr);
Integer i = 1;
while(true){
    producer.send(new ProducerRecord<String, String>(srcTopicName, "src1","src1 msg #" + i));
    System.out.println("Message " + i + " sent");
    Thread.sleep(1000);
    i++;
}

producer.send(new ProducerRecord<String, String>(srcTopicName, record.key(), record.value()));    
```
    Message 1 sent
    Message 2 sent
    Message 3 sent
    Message 4 sent
    Message 5 sent
    Message 6 sent
    Message 7 sent
    Message 8 sent
    Message 9 sent
    Message 10 sent
    Message 11 sent
    Message 12 sent
    Message 13 sent
    Message 14 sent
    Message 15 sent
    Message 16 sent
    Message 17 sent
    Message 18 sent
    Message 19 sent
    Message 20 sent
    Message 21 sent
    Message 22 sent
    Message 23 sent

```Java
producer.close();
```
