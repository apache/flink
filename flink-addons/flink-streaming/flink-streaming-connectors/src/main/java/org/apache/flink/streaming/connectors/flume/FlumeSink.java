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
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;

public abstract class FlumeSink<IN extends Tuple> extends SinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(RMQSource.class);
	
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

	@Override
	public void invoke(IN tuple) {

		if (!initDone) {
			client = new FlinkRpcClientFacade();
			client.init(host, port);
		}

		byte[] data = serialize(tuple);
		if (!closeWithoutSend) {
			client.sendDataToFlume(data);
		}
		if (sendAndClose) {
			client.close();
		}

	}

	public abstract byte[] serialize(IN tuple);

	private class FlinkRpcClientFacade {
		private RpcClient client;
		private String hostname;
		private int port;

		public void init(String hostname, int port) {
			// Setup the RPC connection
			this.hostname = hostname;
			this.port = port;
			int initCounter = 0;
			while (true) {
				if (initCounter >= 90) {
					System.exit(1);
				}
				try {
					this.client = RpcClientFactory.getDefaultInstance(hostname, port);
				} catch (FlumeException e) {
					// Wait one second if the connection failed before the next try
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

		public void close() {
			client.close();
		}

	}

	public void sendAndClose() {
		sendAndClose = true;
	}

	public void closeWithoutSend() {
		client.close();
		closeWithoutSend = true;
	}

}
