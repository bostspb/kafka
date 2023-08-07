# Лабораторная работа 2: Stream DSL


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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Objects;
import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;
import java.sql.Timestamp;
```

# Постановка задачи

В топик requests поступают запросы на страхование - сообщения вида

    insurer=<имя>;model=<модель>;<другой текст>
    
**Задача 1** Откладывать в топик (lab2-`stud_code`-ivanov) все сообщения от страхователя "Ivanov".

**Задача 2** Откладывать в топик (lab2-`stud_code`-doubles) повторные запросы от одного и того же страхователя. Необходимо в сообщение добавлять текст

    ;;previous=<полный текст предыдущего сообщения>

**Задача 3** Выводить в отдельный топик (lab2-`stud_code`-counts) страхователей, от которых приходят повторные запросы с их количеством (т.е. страхователь, количество запросов)


Пример данных

    insurer=mike;model=toyota;osago
    insurer=mike;model=volvo;kasko
    insurer=pete;model=toyota;osago
    insurer=Ivanov;model=toyota;osago
    insurer=mike;model=bmw;osago
    insurer=mike;model=ford;osago
    insurer=Ivanov;model=toyota;osago    
    insurer=mike;model=nissan;kasko

В топик про Ivanov попадут 2 записи

    insurer=Ivanov;model=toyota;osago
    insurer=Ivanov;model=toyota;osago    

Топик с повторами будет содержать

    insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    insurer=mike;model=bmw;osago;;previous=insurer=mike;model=ford;osago
    insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    insurer=mike;model=nissan;kasko;;previous=insurer=mike;model=ford;osago

Топик с счетчиками будет содержать

    mike;2
    mike;3
    mike;4
    Ivanov;2
    mike;5
    

# Решение задачи


```Java
// Конфигурация
String srcTopicName = "requests";
String tgtTopicNameIvanov = "lab2-05-ivanov";
String tgtTopicNameDoubles = "lab2-05-doubles";
String tgtTopicNameCounts = "lab2-05-counts";
String storeNameDoubles = "lab2-05-store-doubles";
String storeNameCounts = "lab2-05-store-counts";

Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab2-05-app-stream-8");  // меняем для повторных запусков
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

// Подготовка билдера
Serde<String> stringSerde = Serdes.String();
StreamsBuilder builder = new StreamsBuilder();

// Подключаем два хранилища состояний
KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeNameDoubles);
StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
    storeSupplier, Serdes.String(), Serdes.String()
);
builder.addStateStore(storeBuilder);

KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeNameCounts);
StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(
    storeSupplier, Serdes.String(), Serdes.Integer()
);
builder.addStateStore(storeBuilder);
```




    org.apache.kafka.streams.StreamsBuilder@62faa06e




```Java
/**
 * Ветка захвата источника
 */
// SOURCE
KStream<String, String> srcExtractStream = builder.stream(srcTopicName, Consumed.with(stringSerde,stringSerde)
    .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)); 
// PRINT
srcExtractStream.print(Printed.<String, String>toSysOut().withLabel("SOURCE")); 
```


```Java
/**
 * Ответвление для слива Ивановых
 */
Predicate<String,String> isIvanov = new Predicate<String, String>() {
    @Override
    public boolean test(String key, String value) {
        String[] valueArray = value.split(";");
        return (valueArray[0].equals("insurer=Ivanov"));        
    }
};
// FILTER
KStream<String, String> onlyIvanovStream = srcExtractStream.filter(isIvanov);
// SINK
onlyIvanovStream.to( tgtTopicNameIvanov, Produced.with(stringSerde, stringSerde));
// PRINT
onlyIvanovStream.print(Printed.<String, String>toSysOut().withLabel("IVANOV"));

```


```Java
/**
 * Ответвление для слива дублей
 */
// Класс трансформации
// Сохраняем текущую запись в хранилище состояний.
// Результат трансформации: если страхователь повторяется, выдаем строку с текущим 
// сообщением и добавляем ";;previous=<полный текст предыдущего сообщения>", 
// иначе выдаем пустую строку.
public class addPreviousValueIfDublicate implements ValueTransformer<String,String> {

    private KeyValueStore<String, String> stateStore;
    private final String storeName;
    private ProcessorContext context;

    // реализация необходимых "стандартных" методов интерфейса...
    public addPreviousValueIfDublicate(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore) this.context.getStateStore(storeName);
    }

    @Override
    public void close() {
        //no-op
    }

    // Реализация самой трансформации
    // (по-хорошему нужно было делать класс)
    @Override
    public String transform(String value) {
        String duplicateMsg = "";
        String prevValue = stateStore.get("lab2_05_doubles_previous_value");
        String prevInsurer = stateStore.get("lab2_05_doubles_previous_insurer");

        if (prevValue == null) prevValue = "";        
        if (prevInsurer == null) prevInsurer = "";
                
        String[] valueArray = value.split(";");
        
        if (prevInsurer.equals(valueArray[0])) {            
            duplicateMsg = value + ";;previous=" + prevValue;                    
        }
        
        stateStore.put("lab2_05_doubles_previous_value", value);
        stateStore.put("lab2_05_doubles_previous_insurer", valueArray[0]);

        return duplicateMsg;
    }
}

Predicate<String,String> isEmptyValue = new Predicate<String, String>() {
    @Override
    public boolean test(String key, String value) {       
        return (value.equals(""));        
    }
};

// ACCUMULATE
KStream<String, String> catchDublicatesTransformation = srcExtractStream.transformValues(
    () -> new addPreviousValueIfDublicate(storeNameDoubles), storeNameDoubles
);
// FILTER
KStream<String, String> onlyDublicatesStream = catchDublicatesTransformation.filterNot(isEmptyValue);
// SINK
onlyDublicatesStream.to( tgtTopicNameDoubles, Produced.with(stringSerde, stringSerde));
// PRINT
onlyDublicatesStream.print(Printed.<String, String>toSysOut().withLabel("DOUBLES"));     
```


```Java
/**
 * Ответвление для слива счетчика повторных запросов в разрезе страхователей
 */
// Класс трансформации
// Сохраняем счетчик повторных запросов в разрезе страхователей.
// Результат трансформации: если страхователь повторяется в потоке, выдаем строку с именем страхователи и 
// числом повторных появлений в потоке, иначе выдаем пустую строку.
public class countRrepeatedMsgByInsurer implements ValueTransformer<String,String> {

    private KeyValueStore<String, Integer> stateStore;
    private final String storeName;
    private ProcessorContext context;

    // реализация необходимых "стандартных" методов интерфейса...
    public countRrepeatedMsgByInsurer(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore) this.context.getStateStore(storeName);
    }

    @Override
    public void close() {
        //no-op
    }

    // Реализация самой трансформации
    // (по-хорошему нужно было делать класс)
    @Override
    public String transform(String value) {
        String countsMsg = "";
        String[] valueArray = value.split(";");
        String[] insurerArray = valueArray[0].split("=");
        
        Integer insurerCounter = stateStore.get(insurerArray[1]);

        if (insurerCounter == null) {
            insurerCounter = 1;  
        } else {
            insurerCounter++;
        }
        
        if (insurerCounter > 1) {
            countsMsg = insurerArray[1] + ";" + String.valueOf(insurerCounter);                    
        }
        
        stateStore.put(insurerArray[1], insurerCounter);

        return countsMsg;
    }
}



// ACCUMULATE
KStream<String, String> catchRrepeatedMsgByInsurerTransformation = srcExtractStream.transformValues(
    () -> new countRrepeatedMsgByInsurer(storeNameCounts), storeNameCounts
);
// FILTER
KStream<String, String> onlyRrepeatedMsgCounterStream = catchRrepeatedMsgByInsurerTransformation.filterNot(isEmptyValue);
// SINK
onlyRrepeatedMsgCounterStream.to( tgtTopicNameCounts, Produced.with(stringSerde, stringSerde));
// PRINT
onlyRrepeatedMsgCounterStream.print(Printed.<String, String>toSysOut().withLabel("COUNTER"));     
```


```Java
// Фиксируем топологию
Topology tpl = builder.build();

// Выводим на печать описание топологии
System.out.println(tpl.describe());
```

    Topologies:
       Sub-topology: 0
        Source: KSTREAM-SOURCE-0000000000 (topics: [requests])
          --> KSTREAM-FILTER-0000000002, KSTREAM-TRANSFORMVALUES-0000000005, KSTREAM-TRANSFORMVALUES-0000000009, KSTREAM-PRINTER-0000000001
        Processor: KSTREAM-TRANSFORMVALUES-0000000005 (stores: [lab2-05-store-doubles])
          --> KSTREAM-FILTER-0000000006
          <-- KSTREAM-SOURCE-0000000000
        Processor: KSTREAM-TRANSFORMVALUES-0000000009 (stores: [lab2-05-store-counts])
          --> KSTREAM-FILTER-0000000010
          <-- KSTREAM-SOURCE-0000000000
        Processor: KSTREAM-FILTER-0000000002 (stores: [])
          --> KSTREAM-PRINTER-0000000004, KSTREAM-SINK-0000000003
          <-- KSTREAM-SOURCE-0000000000
        Processor: KSTREAM-FILTER-0000000006 (stores: [])
          --> KSTREAM-PRINTER-0000000008, KSTREAM-SINK-0000000007
          <-- KSTREAM-TRANSFORMVALUES-0000000005
        Processor: KSTREAM-FILTER-0000000010 (stores: [])
          --> KSTREAM-PRINTER-0000000012, KSTREAM-SINK-0000000011
          <-- KSTREAM-TRANSFORMVALUES-0000000009
        Processor: KSTREAM-PRINTER-0000000001 (stores: [])
          --> none
          <-- KSTREAM-SOURCE-0000000000
        Processor: KSTREAM-PRINTER-0000000004 (stores: [])
          --> none
          <-- KSTREAM-FILTER-0000000002
        Processor: KSTREAM-PRINTER-0000000008 (stores: [])
          --> none
          <-- KSTREAM-FILTER-0000000006
        Processor: KSTREAM-PRINTER-0000000012 (stores: [])
          --> none
          <-- KSTREAM-FILTER-0000000010
        Sink: KSTREAM-SINK-0000000003 (topic: lab2-05-ivanov)
          <-- KSTREAM-FILTER-0000000002
        Sink: KSTREAM-SINK-0000000007 (topic: lab2-05-doubles)
          <-- KSTREAM-FILTER-0000000006
        Sink: KSTREAM-SINK-0000000011 (topic: lab2-05-counts)
          <-- KSTREAM-FILTER-0000000010
    
    



```Java
// Запуск приложения
KafkaStreams kafkaStreams = new KafkaStreams(tpl, props);
System.out.println("LAB #2: Streaming App Started");
kafkaStreams.start();
Thread.sleep(3000);
System.out.println("Shutting down Streaming App");
kafkaStreams.close();

// чистим локальное хранилище
kafkaStreams.cleanUp(); 
```

    LAB #2: Streaming App Started
    [SOURCE]: null, insurer=mike;model=toyota;osago
    [SOURCE]: null, insurer=mike;model=volvo;kasko
    [DOUBLES]: null, insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    [COUNTER]: null, mike;2
    [SOURCE]: null, insurer=pete;model=toyota;osago
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [SOURCE]: null, insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;3
    [SOURCE]: null, insurer=mike;model=ford;osago
    [DOUBLES]: null, insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;4
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;2
    [SOURCE]: null, insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;5
    [SOURCE]: null, insurer=mike;model=toyota;osago
    [DOUBLES]: null, insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;6
    [SOURCE]: null, insurer=mike;model=volvo;kasko
    [DOUBLES]: null, insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    [COUNTER]: null, mike;7
    [SOURCE]: null, insurer=pete;model=toyota;osago
    [COUNTER]: null, pete;2
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;3
    [SOURCE]: null, insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;8
    [SOURCE]: null, insurer=mike;model=ford;osago
    [DOUBLES]: null, insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;9
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;4
    [SOURCE]: null, insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;10
    [SOURCE]: null, insurer=mike;model=toyota;osago
    [DOUBLES]: null, insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;11
    [SOURCE]: null, insurer=mike;model=volvo;kasko
    [DOUBLES]: null, insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    [COUNTER]: null, mike;12
    [SOURCE]: null, insurer=pete;model=toyota;osago
    [COUNTER]: null, pete;3
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;5
    [SOURCE]: null, insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;13
    [SOURCE]: null, insurer=mike;model=ford;osago
    [DOUBLES]: null, insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;14
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;6
    [SOURCE]: null, insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;15
    [SOURCE]: null, insurer=mike;model=toyota;osago
    [DOUBLES]: null, insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;16
    [SOURCE]: null, insurer=mike;model=volvo;kasko
    [DOUBLES]: null, insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    [COUNTER]: null, mike;17
    [SOURCE]: null, insurer=pete;model=toyota;osago
    [COUNTER]: null, pete;4
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;7
    [SOURCE]: null, insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;18
    [SOURCE]: null, insurer=mike;model=ford;osago
    [DOUBLES]: null, insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;19
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;8
    [SOURCE]: null, insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;20
    [SOURCE]: null, insurer=mike;model=toyota;osago
    [DOUBLES]: null, insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;21
    [SOURCE]: null, insurer=mike;model=volvo;kasko
    [DOUBLES]: null, insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    [COUNTER]: null, mike;22
    [SOURCE]: null, insurer=pete;model=toyota;osago
    [COUNTER]: null, pete;5
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;9
    [SOURCE]: null, insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;23
    [SOURCE]: null, insurer=mike;model=ford;osago
    [DOUBLES]: null, insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;24
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;10
    [SOURCE]: null, insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;25
    [SOURCE]: null, insurer=mike;model=toyota;osago
    [DOUBLES]: null, insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;26
    [SOURCE]: null, insurer=mike;model=volvo;kasko
    [DOUBLES]: null, insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    [COUNTER]: null, mike;27
    [SOURCE]: null, insurer=pete;model=toyota;osago
    [COUNTER]: null, pete;6
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;11
    [SOURCE]: null, insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;28
    [SOURCE]: null, insurer=mike;model=ford;osago
    [DOUBLES]: null, insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    [COUNTER]: null, mike;29
    [SOURCE]: null, insurer=Ivanov;model=toyota;osago
    [IVANOV]: null, insurer=Ivanov;model=toyota;osago
    [COUNTER]: null, Ivanov;12
    [SOURCE]: null, insurer=mike;model=nissan;kasko
    [COUNTER]: null, mike;30
    Shutting down Streaming App



```Java
Properties propsCs = new Properties();
propsCs.put("bootstrap.servers", "localhost:9092");
propsCs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
propsCs.put("group.id", "lab2-05-group1");
propsCs.put("enable.auto.commit", "false"); // "false" для отладки
propsCs.put("auto.offset.reset","earliest");
```


```Java
/**
 * Проверяем что лежало в топике-источнике
 */

KafkaConsumer<String, String> consumerForCheck = new KafkaConsumer<String, String>(propsCs);
consumerForCheck.subscribe(Arrays.asList(srcTopicName));

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
    offset = 0, key = null, value = insurer=mike;model=toyota;osago
    offset = 1, key = null, value = insurer=mike;model=volvo;kasko
    offset = 2, key = null, value = insurer=pete;model=toyota;osago
    offset = 3, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 4, key = null, value = insurer=mike;model=bmw;osago
    offset = 5, key = null, value = insurer=mike;model=ford;osago
    offset = 6, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 7, key = null, value = insurer=mike;model=nissan;kasko
    offset = 8, key = null, value = insurer=mike;model=toyota;osago
    offset = 9, key = null, value = insurer=mike;model=volvo;kasko
    offset = 10, key = null, value = insurer=pete;model=toyota;osago
    offset = 11, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 12, key = null, value = insurer=mike;model=bmw;osago
    offset = 13, key = null, value = insurer=mike;model=ford;osago
    offset = 14, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 15, key = null, value = insurer=mike;model=nissan;kasko
    offset = 16, key = null, value = insurer=mike;model=toyota;osago
    offset = 17, key = null, value = insurer=mike;model=volvo;kasko
    offset = 18, key = null, value = insurer=pete;model=toyota;osago
    offset = 19, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 20, key = null, value = insurer=mike;model=bmw;osago
    offset = 21, key = null, value = insurer=mike;model=ford;osago
    offset = 22, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 23, key = null, value = insurer=mike;model=nissan;kasko
    offset = 24, key = null, value = insurer=mike;model=toyota;osago
    offset = 25, key = null, value = insurer=mike;model=volvo;kasko
    offset = 26, key = null, value = insurer=pete;model=toyota;osago
    offset = 27, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 28, key = null, value = insurer=mike;model=bmw;osago
    offset = 29, key = null, value = insurer=mike;model=ford;osago
    offset = 30, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 31, key = null, value = insurer=mike;model=nissan;kasko
    offset = 32, key = null, value = insurer=mike;model=toyota;osago
    offset = 33, key = null, value = insurer=mike;model=volvo;kasko
    offset = 34, key = null, value = insurer=pete;model=toyota;osago
    offset = 35, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 36, key = null, value = insurer=mike;model=bmw;osago
    offset = 37, key = null, value = insurer=mike;model=ford;osago
    offset = 38, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 39, key = null, value = insurer=mike;model=nissan;kasko
    offset = 40, key = null, value = insurer=mike;model=toyota;osago
    offset = 41, key = null, value = insurer=mike;model=volvo;kasko
    offset = 42, key = null, value = insurer=pete;model=toyota;osago
    offset = 43, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 44, key = null, value = insurer=mike;model=bmw;osago
    offset = 45, key = null, value = insurer=mike;model=ford;osago
    offset = 46, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 47, key = null, value = insurer=mike;model=nissan;kasko



```Java
/**
 * Проверяем топик tgtTopicNameIvanov
 */

KafkaConsumer<String, String> consumerForCheck = new KafkaConsumer<String, String>(propsCs);
consumerForCheck.subscribe(Arrays.asList(tgtTopicNameIvanov));

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
    offset = 0, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 1, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 2, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 3, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 4, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 5, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 6, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 7, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 8, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 9, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 10, key = null, value = insurer=Ivanov;model=toyota;osago
    offset = 11, key = null, value = insurer=Ivanov;model=toyota;osago



```Java
/**
 * Проверяем топик tgtTopicNameDoubles
 */

KafkaConsumer<String, String> consumerForCheck = new KafkaConsumer<String, String>(propsCs);
consumerForCheck.subscribe(Arrays.asList(tgtTopicNameDoubles));

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
    offset = 0, key = null, value = insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    offset = 1, key = null, value = insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    offset = 2, key = null, value = insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    offset = 3, key = null, value = insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    offset = 4, key = null, value = insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    offset = 5, key = null, value = insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    offset = 6, key = null, value = insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    offset = 7, key = null, value = insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    offset = 8, key = null, value = insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    offset = 9, key = null, value = insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    offset = 10, key = null, value = insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    offset = 11, key = null, value = insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    offset = 12, key = null, value = insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    offset = 13, key = null, value = insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago
    offset = 14, key = null, value = insurer=mike;model=toyota;osago;;previous=insurer=mike;model=nissan;kasko
    offset = 15, key = null, value = insurer=mike;model=volvo;kasko;;previous=insurer=mike;model=toyota;osago
    offset = 16, key = null, value = insurer=mike;model=ford;osago;;previous=insurer=mike;model=bmw;osago



```Java
/**
 * Проверяем топик tgtTopicNameCounts
 */

KafkaConsumer<String, String> consumerForCheck = new KafkaConsumer<String, String>(propsCs);
consumerForCheck.subscribe(Arrays.asList(tgtTopicNameCounts));

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
    offset = 0, key = null, value = mike;2
    offset = 1, key = null, value = mike;3
    offset = 2, key = null, value = mike;4
    offset = 3, key = null, value = Ivanov;2
    offset = 4, key = null, value = mike;5
    offset = 5, key = null, value = mike;6
    offset = 6, key = null, value = mike;7
    offset = 7, key = null, value = pete;2
    offset = 8, key = null, value = Ivanov;3
    offset = 9, key = null, value = mike;8
    offset = 10, key = null, value = mike;9
    offset = 11, key = null, value = Ivanov;4
    offset = 12, key = null, value = mike;10
    offset = 13, key = null, value = mike;11
    offset = 14, key = null, value = mike;12
    offset = 15, key = null, value = pete;3
    offset = 16, key = null, value = Ivanov;5
    offset = 17, key = null, value = mike;13
    offset = 18, key = null, value = mike;14
    offset = 19, key = null, value = Ivanov;6
    offset = 20, key = null, value = mike;15
    offset = 21, key = null, value = mike;16
    offset = 22, key = null, value = mike;17
    offset = 23, key = null, value = pete;4
    offset = 24, key = null, value = Ivanov;7
    offset = 25, key = null, value = mike;18
    offset = 26, key = null, value = mike;19
    offset = 27, key = null, value = Ivanov;8
    offset = 28, key = null, value = mike;20
    offset = 29, key = null, value = mike;21
    offset = 30, key = null, value = mike;22
    offset = 31, key = null, value = pete;5
    offset = 32, key = null, value = Ivanov;9
    offset = 33, key = null, value = mike;23
    offset = 34, key = null, value = mike;24
    offset = 35, key = null, value = Ivanov;10
    offset = 36, key = null, value = mike;25
    offset = 37, key = null, value = mike;26
    offset = 38, key = null, value = mike;27
    offset = 39, key = null, value = pete;6
    offset = 40, key = null, value = Ivanov;11
    offset = 41, key = null, value = mike;28
    offset = 42, key = null, value = mike;29
    offset = 43, key = null, value = Ivanov;12
    offset = 44, key = null, value = mike;30

