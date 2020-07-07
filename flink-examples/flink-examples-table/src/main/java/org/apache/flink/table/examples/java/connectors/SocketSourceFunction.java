/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * The {@link SocketSourceFunction} opens a socket and consumes bytes.
 *
 * <p>It splits records by the given byte delimiter (`\n` by default) and delegates the decoding to a
 * pluggable {@link DeserializationSchema}.
 *
 * <p>Note: This is only an example and should not be used in production. The source function is not
 * fault-tolerant and can only work with a parallelism of 1.
 */
public final class SocketSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

	private final String hostname;
	private final int port;
	private final byte byteDelimiter;
	private final DeserializationSchema<RowData> deserializer;

	private volatile boolean isRunning = true;
	private Socket currentSocket;

	public SocketSourceFunction(String hostname, int port, byte byteDelimiter, DeserializationSchema<RowData> deserializer) {
		this.hostname = hostname;
		this.port = port;
		this.byteDelimiter = byteDelimiter;
		this.deserializer = deserializer;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return deserializer.getProducedType();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		deserializer.open(() -> getRuntimeContext().getMetricGroup());
	}

	@Override
	public void run(SourceContext<RowData> ctx) throws Exception {
		while (isRunning) {
			// open and consume from socket
			try (final Socket socket = new Socket()) {
				currentSocket = socket;
				socket.connect(new InetSocketAddress(hostname, port), 0);
				try (InputStream stream = socket.getInputStream()) {
					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
					int b;
					while ((b = stream.read()) >= 0) {
						// buffer until delimiter
						if (b != byteDelimiter) {
							buffer.write(b);
						}
						// decode and emit record
						else {
							ctx.collect(deserializer.deserialize(buffer.toByteArray()));
							buffer.reset();
						}
					}
				}
			} catch (Throwable t) {
				t.printStackTrace(); // print and continue
			}
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
		try {
			currentSocket.close();
		} catch (Throwable t) {
			// ignore
		}
	}
}
