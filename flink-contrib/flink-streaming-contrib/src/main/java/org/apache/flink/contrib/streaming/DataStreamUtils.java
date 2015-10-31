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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.runtime.net.ConnectionUtils;

public final class DataStreamUtils {

	/**
	 * Returns an iterator to iterate over the elements of the DataStream.
	 * @return The iterator
	 */
	public static <OUT> Iterator<OUT> collect(DataStream<OUT> stream) {
		TypeSerializer serializer = stream.getType().createSerializer(stream.getExecutionEnvironment().getConfig());
		DataStreamIterator<OUT> it = new DataStreamIterator<OUT>(serializer);

		//Find out what IP of us should be given to CollectSink, that it will be able to connect to
		StreamExecutionEnvironment env = stream.getExecutionEnvironment();
		InetAddress clientAddress;
		if(env instanceof RemoteStreamEnvironment) {
			String host = ((RemoteStreamEnvironment)env).getHost();
			int port = ((RemoteStreamEnvironment)env).getPort();
			try {
				clientAddress = ConnectionUtils.findConnectingAddress(new InetSocketAddress(host, port), 2000, 400);
			} catch (IOException e) {
				throw new RuntimeException("IOException while trying to connect to the master", e);
			}
		} else {
			try {
				clientAddress = InetAddress.getLocalHost();
			} catch (UnknownHostException e) {
				throw new RuntimeException("getLocalHost failed", e);
			}
		}

		DataStreamSink<OUT> sink = stream.addSink(new CollectSink<OUT>(clientAddress, it.getPort(), serializer));
		sink.setParallelism(1); // It would not work if multiple instances would connect to the same port

		(new CallExecute(stream)).start();

		return it;
	}

	private static class CallExecute<OUT> extends Thread {

		DataStream<OUT> stream;

		public CallExecute(DataStream<OUT> stream) {
			this.stream = stream;
		}

		@Override
		public void run(){
			try {
				stream.getExecutionEnvironment().execute();
			} catch (Exception e) {
				throw new RuntimeException("Exception in execute()", e);
			}
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private DataStreamUtils() {
		throw new RuntimeException();
	}
}
