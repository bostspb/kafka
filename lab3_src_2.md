# Источник №2 для лабораторной №3


```python
# настройки 
PRODUCER_CLI = "/home/ec2-user/local/kafka_2.13-2.6.0/bin/kafka-console-producer.sh --bootstrap-server localhost:9092"
!echo 'src2 msg' | {PRODUCER_CLI} --topic "lab3-05-src02"
```

```python
TOPICS_CLI = "/home/ec2-user/local/kafka_2.13-2.6.0/bin/kafka-topics.sh --bootstrap-server localhost:9092"

!{TOPICS_CLI} --delete --topic "lab3-05-src01"
!{TOPICS_CLI} --delete --topic "lab3-05-src02"
!{TOPICS_CLI} --delete --topic "lab3-05-src03"
!{TOPICS_CLI} --delete --topic "lab3-05-traffic"

!{TOPICS_CLI} --create --topic "lab3-05-src01"
!{TOPICS_CLI} --create --topic "lab3-05-src02"
!{TOPICS_CLI} --create --topic "lab3-05-src03"
!{TOPICS_CLI} --create --topic "lab3-05-traffic"
```

    Created topic lab3-05-src01.
    Created topic lab3-05-src02.
    Created topic lab3-05-src03.
    Created topic lab3-05-traffic.
