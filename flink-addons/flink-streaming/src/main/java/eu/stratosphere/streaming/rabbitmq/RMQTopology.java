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

package eu.stratosphere.streaming.rabbitmq;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.DataStream;
import eu.stratosphere.streaming.api.SinkFunction;
import eu.stratosphere.streaming.api.StreamExecutionEnvironment;

public class RMQTopology {

	public static final class MySink extends SinkFunction<Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<String> tuple) {
			result.add(tuple.f0);
		}

		
	}
	
	public static final class MyRMQSink extends RMQSink<Tuple1<String>> {
		public MyRMQSink(String HOST_NAME, String QUEUE_NAME) {
			super(HOST_NAME, QUEUE_NAME);
			// TODO Auto-generated constructor stub
		}

		private static final long serialVersionUID = 1L;

		@Override
		public byte[] serialize(Tuple t) {
			// TODO Auto-generated method stub
			return SerializationUtils.serialize((String)t.getField(0));
		}

		
	}
	
	public static final class MyRMQSource extends RMQSource<Tuple1<String>> {
		

		public MyRMQSource(String HOST_NAME, String QUEUE_NAME) {
			super(HOST_NAME, QUEUE_NAME);
			// TODO Auto-generated constructor stub
		}

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple1<String> deserialize(byte[] t) {
			String s = (String) SerializationUtils.deserialize(t);
			Tuple1<String> out=new Tuple1<String>();
			out.f0=s;
			if(s.equals("q")){
				close();
			}
			return out;
		}
		
	}
	
	private static Set<String> result = new HashSet<String>();
	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = new StreamExecutionEnvironment();

		DataStream<Tuple1<String>> dataStream1 = env
				.addSource(new MyRMQSource("localhost", "hello"), 1)
				.addSink(new MySink());
		
		DataStream<Tuple1<String>> dataStream2 = env
				.fromElements("one", "two", "three", "four", "five", "q")
				.addSink(new MyRMQSink("localhost", "hello"));

		env.execute();
		
	}
}