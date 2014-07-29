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

package org.apache.flink.streaming.connectors.flume;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.source.AvroSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.Status;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public abstract class FlumeSource<IN extends Tuple> extends SourceFunction<IN> {
	private static final long serialVersionUID = 1L;

	String host;
	String port;

	FlumeSource(String host, int port) {
		this.host = host;
		this.port = Integer.toString(port);
	}

	public class MyAvroSource extends AvroSource {
		Collector<IN> collector;

		@Override
		public Status append(AvroFlumeEvent avroEvent) {
			collect(avroEvent);
			return Status.OK;
		}

		@Override
		public Status appendBatch(List<AvroFlumeEvent> events) {
			for (AvroFlumeEvent avroEvent : events) {
				collect(avroEvent);
			}

			return Status.OK;
		}

		private void collect(AvroFlumeEvent avroEvent) {
			byte[] b = avroEvent.getBody().array();
			IN tuple = FlumeSource.this.deserialize(b);
			if (!closeWithoutSend) {
				collector.collect(tuple);
			}
			if (sendAndClose) {
				sendDone = true;
			}
		}

	}

	MyAvroSource avroSource;
	private volatile boolean closeWithoutSend = false;
	private boolean sendAndClose = false;
	private volatile boolean sendDone = false;

	public abstract IN deserialize(byte[] msg);

	public void configureAvroSource(Collector<IN> collector) {

		avroSource = new MyAvroSource();
		avroSource.collector = collector;
		Context context = new Context();
		context.put("port", port);
		context.put("bind", host);
		avroSource.configure(context);
		ChannelProcessor cp = new ChannelProcessor(null);
		avroSource.setChannelProcessor(cp);
	}

	@Override
	public void invoke(Collector<IN> collector) throws Exception {
		configureAvroSource(collector);
		avroSource.start();
		while (true) {
			if (closeWithoutSend || sendDone) {
				break;
			}
		}
		avroSource.stop();
	}

	public void sendAndClose() {
		sendAndClose = true;
	}

	public void closeWithoutSend() {
		closeWithoutSend = true;
	}

}
