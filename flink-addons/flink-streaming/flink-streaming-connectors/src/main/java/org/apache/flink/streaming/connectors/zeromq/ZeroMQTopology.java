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

package org.apache.flink.streaming.connectors.zeromq;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.SinkFunction;
import org.apache.flink.streaming.api.function.SourceFunction;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

public class ZeroMQTopology {
	
	public static final class MyZeroMQSource extends ZeroMQSource<Tuple1<String>>{

		public MyZeroMQSource(String connection, String type) throws Exception {
			super(connection, type);
		}

		@Override
		public Tuple1<String> deserialize(byte[] t) {
			String out=new String(t);
			if(out.equals("q")) close();
			return new Tuple1<String>(out);
		}
		
	}
	
	public static final class MyZeroMQSource1 extends ZeroMQSource<Tuple1<String>>{

		private int counter=0;
		
		public MyZeroMQSource1(String connection, String type, String filter) throws Exception {
			super(connection, type, filter);
		}

		@Override
		public Tuple1<String> deserialize(byte[] t) {
			counter++;
			String out=new String(t);
			if(counter==2) close();
			return new Tuple1<String>(out);
		}
		
	}
	
	public static final class MyZeroMQSource2 extends ZeroMQSource<Tuple1<String>>{

		private int counter=0;
		
		public MyZeroMQSource2(String connection, String type, String filter) throws Exception {
			super(connection, type, filter);
		}

		@Override
		public Tuple1<String> deserialize(byte[] t) {
			counter++;
			String out=new String(t);
			if(counter==2) close();
			return new Tuple1<String>(out);
		}
		
	}
	
	public static final class MyZeroMQSink extends ZeroMQSink<Tuple1<String>>{

		public MyZeroMQSink(String connection, String type) throws Exception {
			super(connection, type);
		}

		@Override
		public byte[] serialize(Tuple t) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(t.getField(0).equals("q")) close();
			return ((String)t.getField(0)).getBytes();
		}

		
		
	}
	
	public static final class PrintSink1 extends SinkFunction<Tuple1<String>>{

		@Override
		public void invoke(Tuple1<String> tuple) {
			System.out.println("source1 " + tuple);
			
		}
	}
	
	public static final class PrintSink2 extends SinkFunction<Tuple1<String>>{

		@Override
		public void invoke(Tuple1<String> tuple) {
			System.out.println("source2 " + tuple);
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		//Push-Pull example
		DataStream<Tuple1<String>> dataStream1 = env
				.addSource(new MyZeroMQSource("tcp://localhost:5555", "pushpull"))
				.print();
		
		DataStream<Tuple1<String>> dataStream2 = env
				.fromElements("one", "two", "three", "four", "five", "q")
				.addSink(new MyZeroMQSink("tcp://*:5555", "pushpull"));
		
		//PubSub Example
		DataStream<Tuple1<String>> dataStream3 = env
				.fromElements("apple", "Hello", "World", "Hello", "World")
				.addSink(new MyZeroMQSink("tcp://*:5563", "pubsub"));
		
		DataStream<Tuple1<String>> dataStream4 = env
				.addSource(new MyZeroMQSource1("tcp://localhost:5563", "pubsub", "H"))
				.addSink(new PrintSink1());
		
		DataStream<Tuple1<String>> dataStream5 = env
				.addSource(new MyZeroMQSource2("tcp://localhost:5563", "pubsub", "W"))
				.addSink(new PrintSink2());

		env.execute();
		
	}
}
