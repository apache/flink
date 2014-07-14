/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.connectors.kafka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * This is a simple kafka producer that reads local file from disk and produces line streams.
 * To use the producer, a zookeeper server and a kafka server should be in service.
 * Run the following script to start a zookeeper server:
 * 		bin/zookeeper-server-start.sh config/zookeeper.properties
 * Run the following script to start a kafka server:
 * 		bin/kafka-server-start.sh config/server.properties
 * Run the following script to start the producer:
 * 		java -cp kafka-0.8/libs/*:yourJarFile.jar eu.stratosphere.streaming.kafka.KafkaProducer yourTopicID kafkaServerIp
 * As an example:
 * 		java -cp kafka-0.8/libs/*:stratosphere-streaming.jar eu.stratosphere.streaming.kafka.KafkaProducer test localhost:9092
 */
public class KafkaProducer {
	static kafka.javaapi.producer.Producer<Integer, String> producer;
	static Properties props = new Properties();

	public static void ProducerPrepare(String brokerAddr) {
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", brokerAddr);

		producer = new kafka.javaapi.producer.Producer<Integer, String>(
				new ProducerConfig(props));
	}

	public static void main(String[] args) throws Exception{
		if (args.length == 3) {
			String infilename=args[0];
			String topicId=args[1];
			String brokerAddr=args[2];
			ProducerPrepare(brokerAddr);
			BufferedReader reader = new BufferedReader(new FileReader(infilename));
			while (true) {
				String line=reader.readLine();
				if(line==null){
					reader.close();
					reader = new BufferedReader(new FileReader(infilename));
					continue;
				}
				producer.send(new KeyedMessage<Integer, String>(
						topicId, line));
			}
		}else{
			System.out.println("please set filename!");
			System.exit(-1);
		}
	}
}
