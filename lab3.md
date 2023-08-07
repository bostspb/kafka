# Лабораторная работа 3: Processor API в Kafka Streams


```Java
%%loadFromPOM
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.UsePreviousTimeOnInvalidTimestamp;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;
import java.sql.Timestamp;
import java.util.Objects;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;
```

# Постановка задачи

Необходимо написать потоковое приложение, которое будет отслеживать "слабо меняющиеся" топики (из списка - подготовьте список входных топиков и данные в них самостоятельно) - такие, в которые не приходит сообщений в течение заданного времени (интервал 1). Приложение рапортует об этих топиках в специальный топик (lab3-studNN-traffic).

* Шаг1: построить приложение, которое будет "сливать" данные из всех топиков в "отчетный топик" (без обработки, но с процессором)
* Шаг2: доделать до конца (добавить состояние, логику и пунктуацию) 

**Тестовый сценарий**

Следим за топиками src1, src2, src3

Интервал 1 = 3 секунды

Публикации

* в топик src1 будем публиковать в цикле с задержкой 1 секунда
* в топик src2 будем публиковать вручную (т.е. будут интервалы, когда в нем были сообщения)
* в топик src3 не будем публиковать вообще

В отчетном топике должно оказаться

* в каждой отбивке должен быть топик src3
* ни в одной отбивке не должен быть топик src1
* топик src2 должен быть не во всех отбивках

# Решение


```Java
String srcTopicName01 = "lab3-05-src01";
String srcTopicName02 = "lab3-05-src02";
String srcTopicName03 = "lab3-05-src03";
String tgtTopicName   = "lab3-05-traffic";

String logStateStore  = "lab3-05-traffic-store";

String sourceNodeName     = "source_node";
String processorNodeName  = "processor_node";
String sinkNodeName       = "sink_node";

Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-05-app-processor-7");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);   

// подготовка
Serde<String> stringSerde = Serdes.String();
Deserializer<String> stringDeserializer = stringSerde.deserializer();
Serializer<String> stringSerializer = stringSerde.serializer();

Serde<Integer> integerSerde = Serdes.Integer();
Deserializer<Integer> integerDeserializer = integerSerde.deserializer();
Serializer<Integer> integerSerializer = integerSerde.serializer();
```


```Java
public class TrafficPunctuator implements Punctuator {

    private ProcessorContext context;
    private KeyValueStore<String, Integer> keyValueStore;

    public TrafficPunctuator(ProcessorContext context, KeyValueStore<String, Integer> keyValueStore) {
        this.context = context;
        this.keyValueStore = keyValueStore;
    }

    @Override
    public void punctuate(long timestamp) {
        for (Integer i = 1 ; i <= 3 ; i++){        
            Integer val = keyValueStore.get("lab3-05-src0"+ i.toString());
            if (val == null || val == 0) context.forward("", timestamp + "_lab3-05-src0" + i.toString(), "sink_node");        
            keyValueStore.put("lab3-05-src0" + i.toString(), 0);
        }        
    }
}
```


```Java
public class TrafficProcessor extends AbstractProcessor<String, String> {

    private KeyValueStore<String, Integer> keyValueStore;
    private String stateStoreName;

    public TrafficProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext processorContext) {
        super.init(processorContext);
        keyValueStore = (KeyValueStore) context().getStateStore(stateStoreName);
        TrafficPunctuator punctuator = new TrafficPunctuator(context(), keyValueStore);
        
        context().schedule(3000, PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(String key, String value) {
        Integer nMessages = keyValueStore.get(context().topic());
        if (nMessages == null) nMessages = 0;
        keyValueStore.put(context().topic(), nMessages+1);
    }
}
```


```Java
KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(logStateStore);
StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = 
    Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());


TrafficProcessor trafficProcessor = new TrafficProcessor(logStateStore);


Topology tpl = new Topology();
tpl.addSource(LATEST, sourceNodeName, stringDeserializer, stringDeserializer, srcTopicName01, srcTopicName02, srcTopicName03)
    .addProcessor(processorNodeName, () -> trafficProcessor, sourceNodeName)
    .addStateStore(storeBuilder, processorNodeName)
    .addSink(sinkNodeName, tgtTopicName, stringSerializer, stringSerializer, processorNodeName);

System.out.println(tpl.describe());
```

    Topologies:
       Sub-topology: 0
        Source: source_node (topics: [lab3-05-src03, lab3-05-src01, lab3-05-src02])
          --> processor_node
        Processor: processor_node (stores: [lab3-05-traffic-store])
          --> sink_node
          <-- source_node
        Sink: sink_node (topic: lab3-05-traffic)
          <-- processor_node
    
    



```Java
KafkaStreams kafkaStreams = new KafkaStreams(tpl, props);

System.out.println("Starting Traffic Log Application now");
kafkaStreams.start();
Thread.sleep(30000);
System.out.println("Shutting down Application  now");
kafkaStreams.close();

kafkaStreams.cleanUp();
```

    Starting Traffic Log Application now
    Shutting down Application  now



```Java
/**
 * Проверяем содержимое целевого топика tgtTopicName
 */
 
Properties propsCs = new Properties();
propsCs.put("bootstrap.servers", "localhost:9092");
propsCs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("group.id", "lab3-05-group1");
propsCs.put("enable.auto.commit", "true"); // "false" для отладки
propsCs.put("auto.offset.reset","earliest");

KafkaConsumer<String, String> consumerForCheck = new KafkaConsumer<String, String>(propsCs);
consumerForCheck.subscribe(Arrays.asList(tgtTopicName));

ConsumerRecords<String, String> records = consumerForCheck.poll(Duration.ofMillis(1000L));

System.out.println("Checking..."); 
for (ConsumerRecord<String, String> record : records){
    System.out.printf(
        "offset = %d, key = %s, value = %s\n",
        record.offset(), record.key(), record.value()
    );
}

consumerForCheck.close();
```

    Checking...
    offset = 0, key = , value = 1691413689077_lab3-05-src02
    offset = 1, key = , value = 1691413689077_lab3-05-src03
    offset = 2, key = , value = 1691413692078_lab3-05-src02
    offset = 3, key = , value = 1691413692078_lab3-05-src03
    offset = 4, key = , value = 1691413695078_lab3-05-src02
    offset = 5, key = , value = 1691413695078_lab3-05-src03
    offset = 6, key = , value = 1691413698079_lab3-05-src03
    offset = 7, key = , value = 1691413701080_lab3-05-src02
    offset = 8, key = , value = 1691413701080_lab3-05-src03
    offset = 9, key = , value = 1691413704081_lab3-05-src02
    offset = 10, key = , value = 1691413704081_lab3-05-src03
    offset = 11, key = , value = 1691413707083_lab3-05-src03
    offset = 12, key = , value = 1691413710083_lab3-05-src02
    offset = 13, key = , value = 1691413710083_lab3-05-src03
    offset = 14, key = , value = 1691413713085_lab3-05-src02
    offset = 15, key = , value = 1691413713085_lab3-05-src03


Как видим, в содержимом целевого топика нет источника `lab3-05-src01`, а источник `lab3-05-src02` идет с пропусками.
