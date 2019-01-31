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

package org.apache.flink.streaming.connectors.flume.examples;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.flume.FlumeEventBuilder;
import org.apache.flink.streaming.connectors.flume.FlumeSink;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;

/**
 * An example FlumeSink that sends data to Flume service.
 */
public class FlumeSinkExample {
	private static String clientType = "thrift";
	private static String hostname = "localhost";
	private static int port = 9000;

	public static void main(String[] args) throws Exception {
		//FlumeSink send data
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		FlumeEventBuilder<String> flumeEventBuilder = new FlumeEventBuilder<String>() {
			@Override
			public Event createFlumeEvent(String value, RuntimeContext ctx) {
				return EventBuilder.withBody(value, Charset.defaultCharset());
			}
		};

		FlumeSink<String> flumeSink = new FlumeSink<>(clientType, hostname, port, flumeEventBuilder, 1, 1, 1);

		// Note: parallelisms and FlumeSink batchSize
		// if every parallelism not enough batchSize, this parallelism not word FlumeThriftService output
		DataStreamSink<String> dataStream = env.fromElements("one", "two", "three", "four", "five")
			.addSink(flumeSink);

		env.execute();
	}
}
