/**
 *
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
 *
 */

package org.apache.flink.streaming.connectors.kafka;

import java.util.*;

import org.apache.flink.streaming.api.function.SinkFunction;

import org.apache.flink.api.java.tuple.Tuple;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public abstract class KafkaSink<IN extends Tuple, OUT> extends SinkFunction<IN>{
	private static final long serialVersionUID = 1L;
	
	private kafka.javaapi.producer.Producer<Integer, OUT> producer;
	static Properties props;
	private String topicId;
	private String brokerAddr;
	private boolean close = false;
	private boolean initDone = false;
	
	public KafkaSink(String topicId, String brokerAddr){
		this.topicId=topicId;
		this.brokerAddr=brokerAddr;
		
		
	}
	public void initialize() {
		props = new Properties();
		 
		props.put("metadata.broker.list", brokerAddr);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		 
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<Integer, OUT>(config);
		initDone = true;
	}
	
	@Override
	public void invoke(IN tuple) {
		if(!initDone) initialize();
		
		OUT out=serialize(tuple);
		KeyedMessage<Integer, OUT> data = new KeyedMessage<Integer, OUT>(topicId, out);
        producer.send(data);
        if(close){
        	producer.close();
        }
        
	}
	
	public abstract OUT serialize(IN tuple);
	
	public void close(){
		close=true;
	}

}
