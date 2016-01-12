---
title: "Docker Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 6
sub-nav-title: Docker
---

A Docker container is provided with all the required configurations for test running the connectors of Apache Flink. The servers for the message queues will be running on the docker container while the example topology can be run on the user's computer.

#### Installing Docker
The official Docker installation guide can be found [here](https://docs.docker.com/installation/).
After installing Docker an image can be pulled for each connector. Containers can be started from these images where all the required configurations are set.

#### Creating a jar with all the dependencies
For the easiest setup, create a jar with all the dependencies of the *flink-streaming-connectors* project.

~~~bash
cd /PATH/TO/GIT/flink/flink-staging/flink-streaming-connectors
mvn assembly:assembly
~~~bash

This creates an assembly jar under *flink-streaming-connectors/target*.

#### RabbitMQ
Pull the docker image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-rabbitmq
~~~

To run the container, type:

~~~bash
sudo docker run -p 127.0.0.1:5672:5672 -t -i flinkstreaming/flink-connectors-rabbitmq
~~~

Now a terminal has started running from the image with all the necessary configurations to test run the RabbitMQ connector. The -p flag binds the localhost's and the Docker container's ports so RabbitMQ can communicate with the application through these.

To start the RabbitMQ server:

~~~bash
sudo /etc/init.d/rabbitmq-server start
~~~

To launch the example on the host computer, execute:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.rabbitmq.RMQTopology \
> log.txt 2> errorlog.txt
~~~

There are two connectors in the example. One that sends messages to RabbitMQ, and one that receives messages from the same queue. In the logger messages, the arriving messages can be observed in the following format:

~~~
<DATE> INFO rabbitmq.RMQTopology: String: <one> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <two> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <three> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <four> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <five> arrived from RMQ
~~~

#### Apache Kafka

Pull the image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-kafka
~~~

To run the container type:

~~~bash
sudo docker run -p 127.0.0.1:2181:2181 -p 127.0.0.1:9092:9092 -t -i \
flinkstreaming/flink-connectors-kafka
~~~

Now a terminal has started running from the image with all the necessary configurations to test run the Kafka connector. The -p flag binds the localhost's and the Docker container's ports so Kafka can communicate with the application through these.
First start a zookeeper in the background:

~~~bash
/kafka_2.9.2-0.8.1.1/bin/zookeeper-server-start.sh /kafka_2.9.2-0.8.1.1/config/zookeeper.properties \
> zookeeperlog.txt &
~~~

Then start the kafka server in the background:

~~~bash
/kafka_2.9.2-0.8.1.1/bin/kafka-server-start.sh /kafka_2.9.2-0.8.1.1/config/server.properties \
 > serverlog.txt 2> servererr.txt &
~~~

To launch the example on the host computer execute:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.kafka.KafkaTopology \
> log.txt 2> errorlog.txt
~~~


In the example there are two connectors. One that sends messages to Kafka, and one that receives messages from the same queue. In the logger messages, the arriving messages can be observed in the following format:

~~~
<DATE> INFO kafka.KafkaTopology: String: (0) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (1) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (2) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (3) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (4) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (5) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (6) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (7) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (8) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (9) arrived from Kafka
~~~
