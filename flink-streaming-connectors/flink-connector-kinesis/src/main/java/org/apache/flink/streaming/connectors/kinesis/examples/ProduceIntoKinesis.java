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
package org.apache.flink.streaming.connectors.kinesis.examples;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.KinesisPartitioner;
import org.apache.flink.streaming.connectors.kinesis.KinesisSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.nio.ByteBuffer;

/**
 * This is an example on how to produce data into Kinesis
 */
public class ProduceIntoKinesis {

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		DataStream<String> simpleStringStream = see.addSource(new EventsGenerator());

		FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(
				pt.getRequired("region"),
				pt.getRequired("accessKey"),
				pt.getRequired("secretKey"),
				new SimpleStringSchema());

		kinesis.setFailOnError(true);
		kinesis.setDefaultStream("flink-test");
		kinesis.setDefaultPartition("0");

		simpleStringStream.addSink(kinesis);

		see.execute();
	}

	public static class EventsGenerator implements SourceFunction<String> {
		private boolean running = true;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			long seq = 0;
			while(running) {
				Thread.sleep(10);
				ctx.collect((seq++) + "-" + RandomStringUtils.randomAlphabetic(12));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
