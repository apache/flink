/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.SourceFunction;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

public class KafkaTopology {

	public static final class MySource extends SourceFunction<Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<Tuple1<String>> collector) throws Exception {
			// TODO Auto-generated method stub
			for(int i=0; i<10; i++){
				collector.collect(new Tuple1<String>(Integer.toString(i)));
			}
			collector.collect(new Tuple1<String>("q"));
			
		}
	}
	
	public static final class MyKafkaSource extends KafkaSource<Tuple1<String>, String>{

		public MyKafkaSource(String zkQuorum, String groupId, String topicId, int numThreads) {
			super(zkQuorum, groupId, topicId, numThreads);
			// TODO Auto-generated constructor stub
		}

		@Override
		public Tuple1<String> deserialize(byte[] msg) {
			// TODO Auto-generated method stub
			String s=new String(msg);
			if(s.equals("q")){
				close();
			}
			return new Tuple1<String>(s);
		}
		
	}
	public static final class MyKafkaSink extends KafkaSink<Tuple1<String>, String>{

		public MyKafkaSink(String topicId, String brokerAddr) {
			super(topicId, brokerAddr);
			// TODO Auto-generated constructor stub
		}

		@Override
		public String serialize(Tuple1<String> tuple) {
			// TODO Auto-generated method stub
			if(tuple.f0.equals("q")) close();
			return tuple.f0;
		}
		
	}
	
	private static final int SOURCE_PARALELISM = 1;

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		
		DataStream<Tuple1<String>> stream1 = env.addSource(new MyKafkaSource("localhost:2181", "group", "test", 1), SOURCE_PARALELISM)
				.print();
		
		DataStream<Tuple1<String>> stream2 = env
				.addSource(new MySource())
				.addSink(new MyKafkaSink("test", "localhost:9092"));
		env.execute();
	}
}