# Лабораторная работа 1: Producer & Consumer API


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
    <artifactId>slf4j-simple</artifactId>
    <version>1.7.25</version>
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

# Постановка задачи

В топик requests поступают запросы на страхование - сообщения вида

    insurer=<имя>;model=<модель>;<другой текст>
    
**Задача 1** Откладывать в топик (lab1-`stud_code`-ivanov) все сообщения от страхователя "Ivanov".

**Задача 2** Откладывать в топик (lab1-`stud_code`-doubles) подряд идущие запросы от одного и того же страхователя. Необходимо в сообщение добавлять текст

    ;;previous=<полный текст предыдущего сообщения>

Если сообщения от одного страхователя следуют не подряд, они не считаются повторяющимися.


```Java
String srcTopicName = "requests";
String tgtTopicNameIvanov = "lab1-05-ivanov";
String tgtTopicNameDoubles = "lab1-05-doubles";
```


```Java
// создаем Consumer
Properties propsCs = new Properties();
propsCs.put("bootstrap.servers", "localhost:9092");
propsCs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("group.id", "lab1-05-group4");
propsCs.put("enable.auto.commit", "false"); // "false" для отладки
propsCs.put("auto.offset.reset","earliest");

KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(propsCs);

// создаем Producer
Properties propsPr = new Properties();
propsPr.put("bootstrap.servers", "localhost:9092");
propsPr.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
propsPr.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

Producer<String, String> producer = new KafkaProducer<String, String>(propsPr);

// подписываемся на топик
consumer.subscribe(Arrays.asList(srcTopicName));

// читаем данные
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));

// обрабатываем прочитанные данные
String prevInsurer = "";
String prevValue = "";
for (ConsumerRecord<String, String> record : records){
    System.out.printf(
        "offset = %d, key = %s, value = %s\n",
        record.offset(), 
        record.key(), 
        record.value()
    );
        
    String[] valueArray = record.value().split(";");
    
    // Откладываем в топик `lab1-05-ivanov` все сообщения от страхователя "Ivanov"
    if (valueArray[0].equals("insurer=Ivanov")) {
        System.out.println("`Ivanov` is found"); 
        producer.send(new ProducerRecord<String, String>(tgtTopicNameIvanov, record.key(), record.value()));    
    }
    
    // Откладываем в топик `lab1-05-doubles` подряд идущие запросы от одного и того же страхователя.    
    if (prevInsurer.equals(valueArray[0])) {
        System.out.println("Duplicate is found"); 
        String duplicateMsg = record.value() + ";;previous=" + prevValue;        
        producer.send(new ProducerRecord<String, String>(tgtTopicNameDoubles, record.key(), duplicateMsg));    
    }
    
    prevInsurer = valueArray[0];
    prevValue = record.value();
}

consumer.close();
```

    offset = 0, key = null, value = insurer=mike;model=toyota;osago
    offset = 1, key = null, value = insurer=mike;model=volvo;kasko
    Duplicate is found
    offset = 2, key = null, value = insurer=pete;model=toyota;osago
    offset = 3, key = null, value = insurer=Ivanov;model=toyota;osago
    `Ivanov` is found
    offset = 4, key = null, value = insurer=mike;model=bmw;osago
    offset = 5, key = null, value = insurer=mike;model=ford;osago
    Duplicate is found
    offset = 6, key = null, value = insurer=Ivanov;model=toyota;osago
    `Ivanov` is found
    offset = 7, key = null, value = insurer=mike;model=nissan;kasko



```Java
// Проверяем что легло в топик tgtTopicNameIvanov
KafkaConsumer<String, String> consumerForCheck = new KafkaConsumer<String, String>(propsCs);
consumerForCheck.subscribe(Arrays.asList(tgtTopicNameIvanov));

ConsumerRecords<String, String> records = consumerForCheck.poll(Duration.ofMillis(1000L));

System.out.println("Messages `Ivanov`"); 
for (ConsumerRecord<String, String> record : records){
    System.out.printf(
        "offset = %d, key = %s, value = %s\n",
        record.offset(), record.key(), record.value()
    );
}

consumerForCheck.close();
```

    Messages `Ivanov`
    offset = 0, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 1, key = null, value = insurer=Ivanov;model=toyota;osago



```Java
// Проверяем что легло в топик tgtTopicNameDoubles
KafkaConsumer<String, String> consumerForCheck = new KafkaConsumer<String, String>(propsCs);
consumerForCheck.subscribe(Arrays.asList(tgtTopicNameDoubles));

ConsumerRecords<String, String> records = consumerForCheck.poll(Duration.ofMillis(1000L));

System.out.println("Messages `Duplicate`"); 
for (ConsumerRecord<String, String> record : records){
    System.out.printf(
        "offset = %d, key = %s, value = %s\n",
        record.offset(), record.key(), record.value()
    );
}

consumerForCheck.close();
```

    Messages `Duplicate`
    offset = 0, key = null, value = insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    offset = 1, key = null, value = insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago


**Небольшое усложнение**:

    seek()
    
сделайте вариант задачи 1 так, чтобы он всегда работал со всеми данными топика (т.е. работал со смещения 0). При этом топик (requests) может содержать более одной партиции. 


```Java
/**
 * Первое чтение данных
 */

String srcTopicName = "requests";
String tgtTopicName = "lab1-05-ivanov-seek";

// создаем Consumer
Properties propsCs = new Properties();
propsCs.put("bootstrap.servers", "localhost:9092");
propsCs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("group.id", "lab1-05-group-seek1");
propsCs.put("enable.auto.commit", "true");
propsCs.put("auto.offset.reset","earliest");

KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(propsCs);

// создаем Producer
Properties propsPr = new Properties();
propsPr.put("bootstrap.servers", "localhost:9092");
propsPr.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
propsPr.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

Producer<String, String> producer = new KafkaProducer<String, String>(propsPr);

// подписываемся на топик
consumer.subscribe(Arrays.asList(srcTopicName));

// читаем данные
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));

// обрабатываем прочитанные данные
for (ConsumerRecord<String, String> record : records){
    System.out.printf(
        "partition=%d, offset = %d, key = %s, value = %s\n",
        record.partition(), record.offset(), record.key(), record.value()
    );
        
    String[] valueArray = record.value().split(";");
    
    // Откладываем в топик `lab1-05-ivanov-seek` все сообщения от страхователя "Ivanov"
    if (valueArray[0].equals("insurer=Ivanov")) {
        System.out.println("`Ivanov` is found"); 
        producer.send(new ProducerRecord<String, String>(tgtTopicName, record.key(), record.value()));    
    }
}


```

    partition=0, offset = 0, key = null, value = insurer=mike;model=toyota;osago
    partition=0, offset = 1, key = null, value = insurer=mike;model=volvo;kasko
    partition=0, offset = 2, key = null, value = insurer=pete;model=toyota;osago
    partition=0, offset = 3, key = null, value = insurer=Ivanov;model=toyota;osago
    `Ivanov` is found
    partition=0, offset = 4, key = null, value = insurer=mike;model=bmw;osago
    partition=0, offset = 5, key = null, value = insurer=mike;model=ford;osago
    partition=0, offset = 6, key = null, value = insurer=Ivanov;model=toyota;osago
    `Ivanov` is found
    partition=0, offset = 7, key = null, value = insurer=mike;model=nissan;kasko



```Java
/**
 * Повторное чтение данных
 */
 
// прыгаем на нулевое сообщение в каждой из существующих партиций
for (TopicPartition partition: consumer.assignment()){
    System.out.println("Topic " + partition.topic() + ", seek in partition " + partition.partition());
    consumer.seek(partition, 0);
}

// читаем данные
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));

// обрабатываем прочитанные данные
for (ConsumerRecord<String, String> record : records){
    System.out.printf(
        "partition=%d, offset = %d, key = %s, value = %s\n",
        record.partition(), record.offset(), record.key(), record.value()
    );
        
    String[] valueArray = record.value().split(";");
    
    // Откладываем в топик `lab1-05-ivanov-seek` все сообщения от страхователя "Ivanov"
    if (valueArray[0].equals("insurer=Ivanov")) {
        System.out.println("`Ivanov` is found"); 
        producer.send(new ProducerRecord<String, String>(tgtTopicName, record.key(), record.value()));    
    }
}

```

    Topic requests, seek in partition 0
    partition=0, offset = 0, key = null, value = insurer=mike;model=toyota;osago
    partition=0, offset = 1, key = null, value = insurer=mike;model=volvo;kasko
    partition=0, offset = 2, key = null, value = insurer=pete;model=toyota;osago
    partition=0, offset = 3, key = null, value = insurer=Ivanov;model=toyota;osago
    `Ivanov` is found
    partition=0, offset = 4, key = null, value = insurer=mike;model=bmw;osago
    partition=0, offset = 5, key = null, value = insurer=mike;model=ford;osago
    partition=0, offset = 6, key = null, value = insurer=Ivanov;model=toyota;osago
    `Ivanov` is found
    partition=0, offset = 7, key = null, value = insurer=mike;model=nissan;kasko



```Java
consumer.close();
```

**Еще одно добавление**

    commitSync()
    
Сделайте еще один вариант задачи 1: отключите автокоммит (включен по умолчанию), проверьте, что повторный запуск Вашего кода корректно отрабатывает (не читает повторно уже обработанные записи из топика).


```Java
String srcTopicName = "requests";
String tgtTopicName = "lab1-05-ivanov-commit";

// создаем Consumer
Properties propsCs = new Properties();
propsCs.put("bootstrap.servers", "localhost:9092");
propsCs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("group.id", "lab1-05-group-commit");
propsCs.put("enable.auto.commit", "false");
propsCs.put("auto.offset.reset","earliest");

KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(propsCs);

// создаем Producer
Properties propsPr = new Properties();
propsPr.put("bootstrap.servers", "localhost:9092");
propsPr.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
propsPr.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

Producer<String, String> producer = new KafkaProducer<String, String>(propsPr);

// подписываемся на топик
consumer.subscribe(Arrays.asList(srcTopicName));

// читаем данные
ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));

// обрабатываем прочитанные данные
for (ConsumerRecord<String, String> record : records){
    System.out.printf(
        "offset = %d, key = %s, value = %s\n",
        record.offset(), 
        record.key(), 
        record.value()
    );
        
    String[] valueArray = record.value().split(";");
    
    // Откладываем в топик `lab1-05-ivanov-commit` все сообщения от страхователя "Ivanov"
    if (valueArray[0].equals("insurer=Ivanov")) {
        System.out.println("`Ivanov` is found"); 
        producer.send(new ProducerRecord<String, String>(tgtTopicName, record.key(), record.value()));    
    }
}
consumer.commitSync();
consumer.close();
```

    offset = 0, key = null, value = insurer=mike;model=toyota;osago
    offset = 1, key = null, value = insurer=mike;model=volvo;kasko
    offset = 2, key = null, value = insurer=pete;model=toyota;osago
    offset = 3, key = null, value = insurer=Ivanov;model=toyota;osago
    `Ivanov` is found
    offset = 4, key = null, value = insurer=mike;model=bmw;osago
    offset = 5, key = null, value = insurer=mike;model=ford;osago
    offset = 6, key = null, value = insurer=Ivanov;model=toyota;osago
    `Ivanov` is found
    offset = 7, key = null, value = insurer=mike;model=nissan;kasko



```Java
/**
 * Проверяем что при повторном чтении ничего не приходит
 */

KafkaConsumer<String, String> consumerForCheck = new KafkaConsumer<String, String>(propsCs);
consumerForCheck.subscribe(Arrays.asList(srcTopicName));

ConsumerRecords<String, String> records = consumerForCheck.poll(Duration.ofMillis(1000L));

System.out.println("Checking repeated running..."); 
for (ConsumerRecord<String, String> record : records){
    System.out.printf(
        "offset = %d, key = %s, value = %s\n",
        record.offset(), record.key(), record.value()
    );
}

consumerForCheck.close();
```

    Checking repeated running...

