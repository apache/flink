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

package org.apache.flink.streaming.api.functions.sink;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Socket client that acts as a streaming sink. The data is sent to a Socket as a byte array.
 *
 * @param <IN> data to be written into the Socket.
 */
public class SocketClientSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private final String hostName;
	private final int port;
	private final SerializationSchema<IN, byte[]> schema;
	private transient Socket client;
	private transient DataOutputStream dataOutputStream;

	/**
	 * Default constructor.
	 *
	 * @param hostName Host of the Socket server.
	 * @param port Port of the Socket.
	 * @param schema Schema of the data.
	 */
	public SocketClientSink(String hostName, int port, SerializationSchema<IN, byte[]> schema) {
		this.hostName = hostName;
		this.port = port;
		this.schema = schema;
	}

	/**
	 * Initializes the connection to Socket.
	 */
	public void intializeConnection() {
		OutputStream outputStream;
		try {
			client = new Socket(hostName, port);
			outputStream = client.getOutputStream();
		} catch (IOException e) {
			throw new RuntimeException("Cannot initialize connection to socket server at " + hostName + ":" + port, e);
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
		byte[] msg = schema.serialize(value);
		try {
			dataOutputStream.write(msg);
		} catch (IOException e) {
			throw new RuntimeException("Cannot send message " + value.toString() +
					" to socket server at " + hostName + ":" + port, e);
		}
	}

	/**
	 * Closes the connection of the Socket client.
	 */
	private void closeConnection(){
		try {
			dataOutputStream.close();
			client.close();
		} catch (IOException e) {
			throw new RuntimeException("Error while closing connection with socket server at "
					+ hostName + ":" + port, e);
		} finally {
			if (client != null) {
				try {
					client.close();
				} catch (IOException e) {
					throw new RuntimeException("Cannot close connection with socket server at "
							+ hostName + ":" + port, e);
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
		intializeConnection();
	}

	/**
	 * Closes the connection with the Socket server.
	 */
	@Override
	public void close() {
		closeConnection();
	}

}
