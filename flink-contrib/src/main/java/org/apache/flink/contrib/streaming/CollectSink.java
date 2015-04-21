/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.InetAddress;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A specialized data sink to be used by DataStreamUtils.collect.
 */
class CollectSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(SocketClientSink.class);

	private final InetAddress hostIp;
	private final int port;
	private final TypeSerializer<IN> serializer;
	private transient Socket client;
	private transient DataOutputStream dataOutputStream;
	private StreamWriterDataOutputView streamWriter;

	/**
	 * Creates a CollectSink that will send the data to the specified host.
	 *
	 * @param hostIp IP address of the Socket server.
	 * @param port Port of the Socket server.
	 * @param serializer A serializer for the data.
	 */
	public CollectSink(InetAddress hostIp, int port, TypeSerializer<IN> serializer) {
		this.hostIp = hostIp;
		this.port = port;
		this.serializer = serializer;
	}

	/**
	 * Initializes the connection to Socket.
	 */
	public void initializeConnection() {
		OutputStream outputStream;
		try {
			client = new Socket(hostIp, port);
			outputStream = client.getOutputStream();
			streamWriter = new StreamWriterDataOutputView(outputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		dataOutputStream = new DataOutputStream(outputStream);
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Socket.
	 *
	 * @param value
	 *			The incoming data
	 */
	@Override
	public void invoke(IN value) {
		try {
			serializer.serialize(value, streamWriter);
		} catch (IOException e) {
			if(LOG.isErrorEnabled()){
				LOG.error("Cannot send message to socket server at " + hostIp.toString() + ":" + port, e);
			}
		}
	}

	/**
	 * Closes the connection of the Socket client.
	 */
	private void closeConnection(){
		try {
			dataOutputStream.flush();
			client.close();
		} catch (IOException e) {
			throw new RuntimeException("Error while closing connection with socket server at "
					+ hostIp.toString() + ":" + port, e);
		} finally {
			if (client != null) {
				try {
					client.close();
				} catch (IOException e) {
					LOG.error("Cannot close connection with socket server at "
							+ hostIp.toString() + ":" + port, e);
				}
			}
		}
	}

	/**
	 * Initialize the connection with the Socket in the server.
	 * @param parameters Configuration.
	 */
	@Override
	public void open(Configuration parameters) {
		initializeConnection();
	}

	/**
	 * Closes the connection with the Socket server.
	 */
	@Override
	public void close() {
		closeConnection();
	}

	private static class StreamWriterDataOutputView extends DataOutputStream implements DataOutputView {

		public StreamWriterDataOutputView(OutputStream stream) {
			super(stream);
		}

		public void skipBytesToWrite(int numBytes) throws IOException {
			for (int i = 0; i < numBytes; i++) {
				write(0);
			}
		}

		public void write(DataInputView source, int numBytes) throws IOException {
			byte[] data = new byte[numBytes];
			source.readFully(data);
			write(data);
		}
	}
}
