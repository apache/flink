/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rocketmq.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rocketmq.RocketMQSink;
import org.apache.flink.streaming.connectors.rocketmq.RocketMQSource;
import org.apache.flink.streaming.connectors.rocketmq.common.ConsumerConfig;
import org.apache.flink.streaming.connectors.rocketmq.common.ProducerConfig;
import org.apache.flink.streaming.connectors.rocketmq.common.RocketMQMessage;

/**
 * A quick start example for RocketMQ connector {@link Example}.
 */
public class Example {

	/**
	 * Main function entry for example
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//create source
		ConsumerConfig consumerConfig = new ConsumerConfig();
		consumerConfig.setConsumerGroupName("my_consumer_groupname");
		consumerConfig.setMessageModel("cluster");
		consumerConfig.setSubscribeTopics("my_topic");
		consumerConfig.setNameServerAddress("localhost:9876");
		DataStreamSource<RocketMQMessage> source = env.addSource(new RocketMQSource(consumerConfig));

		//create sink and add it to source
		ProducerConfig producerConfig = new ProducerConfig();
		producerConfig.setProducerGroupName("my_consumer_groupname");
		producerConfig.setNameServerAddress("localhost:9876");
		source.addSink(new RocketMQSink(producerConfig));

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

}
