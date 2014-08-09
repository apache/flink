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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

public abstract class FlumeSink<IN> implements SinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(FlumeSink.class);

	private transient FlinkRpcClientFacade client;
	boolean initDone = false;
	String host;
	int port;
	private boolean sendAndClose = false;
	private boolean closeWithoutSend = false;

	public FlumeSink(String host, int port) {
		this.host = host;
		this.port = port;
	}

	/**
	 * Receives tuples from the Apache Flink {@link DataStream} and forwards them to
	 * Apache Flume.
	 * 
	 * @param value
	 *            The tuple arriving from the datastream
	 */
	@Override
	public void invoke(IN value) {

		if (!initDone) {
			client = new FlinkRpcClientFacade();
			client.init(host, port);
		}

		byte[] data = serialize(value);
		if (!closeWithoutSend) {
			client.sendDataToFlume(data);
		}
		if (sendAndClose) {
			client.close();
		}

	}

	/**
	 * Serializes tuples into byte arrays.
	 * 
	 * @param value
	 *            The tuple used for the serialization
	 * @return The serialized byte array.
	 */
	public abstract byte[] serialize(IN value);

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
					throw new RuntimeException("Cannot establish connection with" + port + " at " + host);
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
							LOG.error("Interrupted while trying to connect " + port + " at " + host);
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

		/**
		 * Closes the RpcClient.
		 */
		public void close() {
			client.close();
		}

	}

	/**
	 * Closes the connection only when the next message is sent after this call.
	 */
	public void sendAndClose() {
		sendAndClose = true;
	}

	/**
	 * Closes the connection immediately and no further data will be sent.
	 */
	public void closeWithoutSend() {
		client.close();
		closeWithoutSend = true;
	}

}
