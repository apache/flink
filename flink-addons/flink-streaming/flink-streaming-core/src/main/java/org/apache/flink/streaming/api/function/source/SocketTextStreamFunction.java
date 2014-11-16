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

package org.apache.flink.streaming.api.function.source;

import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

public class SocketTextStreamFunction implements SourceFunction<String> {
	private static final long serialVersionUID = 1L;

	private String hostname;
	private int port;
	private char delimiter;

	public SocketTextStreamFunction(String hostname, int port, char delimiter) {
		this.hostname = hostname;
		this.port = port;
		this.delimiter = delimiter;
	}

	@Override
	public void invoke(Collector<String> collector) throws Exception {
		Socket socket = new Socket(hostname, port);
		while (!socket.isClosed() && socket.isConnected()) {
			streamFromSocket(collector, socket);
		}
	}

	public void streamFromSocket(Collector<String> collector, Socket socket) throws Exception {
		StringBuffer buffer = new StringBuffer();
		BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

		while (true) {
			int data = reader.read();
			if (!socket.isConnected() || socket.isClosed() || data == -1) {
				break;
			}

			if (data == delimiter) {
				collector.collect(buffer.toString());
				buffer = new StringBuffer();
			} else if (data != '\r') { // ignore carriage return
				buffer.append((char) data);
			}
		}

		if (buffer.length() > 0) {
			collector.collect(buffer.toString());
		}
	}
}
