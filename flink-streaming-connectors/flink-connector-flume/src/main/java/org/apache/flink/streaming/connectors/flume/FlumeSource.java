///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.streaming.connectors.flume;
//
//import java.util.List;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.functions.source.ConnectorSource;
//import org.apache.flink.streaming.util.serialization.DeserializationSchema;
//import org.apache.flink.util.Collector;
//import org.apache.flume.Context;
//import org.apache.flume.channel.ChannelProcessor;
//import org.apache.flume.source.AvroSource;
//import org.apache.flume.source.avro.AvroFlumeEvent;
//import org.apache.flume.source.avro.Status;
//
//public class FlumeSource<OUT> extends ConnectorSource<OUT> {
//	private static final long serialVersionUID = 1L;
//
//	String host;
//	String port;
//	volatile boolean finished = false;
//
//	private volatile boolean isRunning = false;
//
//	FlumeSource(String host, int port, DeserializationSchema<OUT> deserializationSchema) {
//		super(deserializationSchema);
//		this.host = host;
//		this.port = Integer.toString(port);
//	}
//
//	public class MyAvroSource extends AvroSource {
//		Collector<OUT> output;
//
//		/**
//		 * Sends the AvroFlumeEvent from it's argument list to the Apache Flink
//		 * {@link DataStream}.
//		 *
//		 * @param avroEvent
//		 *            The event that should be sent to the dataStream
//		 * @return A {@link Status}.OK message if sending the event was
//		 *         successful.
//		 */
//		@Override
//		public Status append(AvroFlumeEvent avroEvent) {
//			collect(avroEvent);
//			return Status.OK;
//		}
//
//		/**
//		 * Sends the AvroFlumeEvents from it's argument list to the Apache Flink
//		 * {@link DataStream}.
//		 *
//		 * @param events
//		 *            The events that is sent to the dataStream
//		 * @return A Status.OK message if sending the events was successful.
//		 */
//		@Override
//		public Status appendBatch(List<AvroFlumeEvent> events) {
//			for (AvroFlumeEvent avroEvent : events) {
//				collect(avroEvent);
//			}
//
//			return Status.OK;
//		}
//
//		/**
//		 * Deserializes the AvroFlumeEvent before sending it to the Apache Flink
//		 * {@link DataStream}.
//		 *
//		 * @param avroEvent
//		 *            The event that is sent to the dataStream
//		 */
//		private void collect(AvroFlumeEvent avroEvent) {
//			byte[] b = avroEvent.getBody().array();
//			OUT out = FlumeSource.this.schema.deserialize(b);
//
//			if (schema.isEndOfStream(out)) {
//				FlumeSource.this.finished = true;
//				this.stop();
//				FlumeSource.this.notifyAll();
//			} else {
//				output.collect(out);
//			}
//
//		}
//
//	}
//
//	MyAvroSource avroSource;
//
//	/**
//	 * Configures the AvroSource. Also sets the output so the application can
//	 * use it from outside of the invoke function.
//	 *
//	 * @param output
//	 *            The output used in the invoke function
//	 */
//	public void configureAvroSource(Collector<OUT> output) {
//
//		avroSource = new MyAvroSource();
//		avroSource.output = output;
//		Context context = new Context();
//		context.put("port", port);
//		context.put("bind", host);
//		avroSource.configure(context);
//		// An instance of a ChannelProcessor is required for configuring the
//		// avroSource although it will not be used in this case.
//		ChannelProcessor cp = new ChannelProcessor(null);
//		avroSource.setChannelProcessor(cp);
//	}
//
//	/**
//	 * Configures the AvroSource and runs until the user calls a close function.
//	 *
//	 * @param output
//	 *            The Collector for sending data to the datastream
//	 */
//	@Override
//	public void run(Collector<OUT> output) throws Exception {
//		isRunning = true;
//		configureAvroSource(output);
//		avroSource.start();
//		while (!finished && isRunning) {
//			this.wait();
//		}
//	}
//
//	@Override
//	public void cancel() {
//		isRunning = false;
//	}
//
//}
