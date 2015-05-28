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

package org.apache.flink.streaming.connectors.flume;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(FlumeSink.class);

	private transient FlinkRpcClientFacade client;
	boolean initDone = false;
	String host;
	int port;
	SerializationSchema<IN, byte[]> schema;

	public FlumeSink(String host, int port, SerializationSchema<IN, byte[]> schema) {
		this.host = host;
		this.port = port;
		this.schema = schema;
	}

	/**
	 * Receives tuples from the Apache Flink {@link DataStream} and forwards
	 * them to Apache Flume.
	 * 
	 * @param value
	 *            The tuple arriving from the datastream
	 */
	@Override
	public void invoke(IN value) {

		byte[] data = schema.serialize(value);
		client.sendDataToFlume(data);

	}

	private class FlinkRpcClientFacade {
		private RpcClient client;
		private String hostname;
		private int port;

		/**
		 * Initializes the connection to Apache Flume.
		 * 
		 * @param hostname
		 *            The host
		 * @param port
		 *            The port.
		 */
		public void init(String hostname, int port) {
			// Setup the RPC connection
			this.hostname = hostname;
			this.port = port;
			int initCounter = 0;
			while (true) {
				if (initCounter >= 90) {
					throw new RuntimeException("Cannot establish connection with" + port + " at "
							+ host);
				}
				try {
					this.client = RpcClientFactory.getDefaultInstance(hostname, port);
				} catch (FlumeException e) {
					// Wait one second if the connection failed before the next
					// try
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						if (LOG.isErrorEnabled()) {
							LOG.error("Interrupted while trying to connect {} at {}", port, host);
						}
					}
				}
				if (client != null) {
					break;
				}
				initCounter++;
			}
			initDone = true;
		}

		/**
		 * Sends byte arrays as {@link Event} series to Apache Flume.
		 * 
		 * @param data
		 *            The byte array to send to Apache FLume
		 */
		public void sendDataToFlume(byte[] data) {
			Event event = EventBuilder.withBody(data);

			try {
				client.append(event);

			} catch (EventDeliveryException e) {
				// clean up and recreate the client
				client.close();
				client = null;
				client = RpcClientFactory.getDefaultInstance(hostname, port);
			}
		}

	}

	@Override
	public void close() {
		client.client.close();
	}

	@Override
	public void open(Configuration config) {
		client = new FlinkRpcClientFacade();
		client.init(host, port);
	}
}
