/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.streaming.experimental.SocketStreamIterator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

/**
 * A collection of utilities for {@link DataStream DataStreams}.
 */
@Experimental
public final class DataStreamUtils {

	/**
	 * Returns an iterator to iterate over the elements of the DataStream.
	 * @return The iterator
	 */
	public static <OUT> Iterator<OUT> collect(DataStream<OUT> stream) throws IOException {

		TypeSerializer<OUT> serializer = stream.getType().createSerializer(
				stream.getExecutionEnvironment().getConfig());

		SocketStreamIterator<OUT> iter = new SocketStreamIterator<OUT>(serializer);

		//Find out what IP of us should be given to CollectSink, that it will be able to connect to
		StreamExecutionEnvironment env = stream.getExecutionEnvironment();
		InetAddress clientAddress;

		if (env instanceof RemoteStreamEnvironment) {
			String host = ((RemoteStreamEnvironment) env).getHost();
			int port = ((RemoteStreamEnvironment) env).getPort();
			try {
				clientAddress = ConnectionUtils.findConnectingAddress(new InetSocketAddress(host, port), 2000, 400);
			}
			catch (Exception e) {
				throw new IOException("Could not determine an suitable network address to " +
						"receive back data from the streaming program.", e);
			}
		} else if (env instanceof LocalStreamEnvironment) {
			clientAddress = InetAddress.getLoopbackAddress();
		} else {
			try {
				clientAddress = InetAddress.getLocalHost();
			} catch (UnknownHostException e) {
				throw new IOException("Could not determine this machines own local address to " +
						"receive back data from the streaming program.", e);
			}
		}

		DataStreamSink<OUT> sink = stream.addSink(new CollectSink<OUT>(clientAddress, iter.getPort(), serializer));
		sink.setParallelism(1); // It would not work if multiple instances would connect to the same port

		(new CallExecute(env, iter)).start();

		return iter;
	}

	/**
	 * Reinterprets the given {@link DataStream} as a {@link KeyedStream}, which extracts keys with the given
	 * {@link KeySelector}.
	 *
	 * <p>IMPORTANT: For every partition of the base stream, the keys of events in the base stream must be
	 * partitioned exactly in the same way as if it was created through a {@link DataStream#keyBy(KeySelector)}.
	 *
	 * @param stream      The data stream to reinterpret. For every partition, this stream must be partitioned exactly
	 *                    in the same way as if it was created through a {@link DataStream#keyBy(KeySelector)}.
	 * @param keySelector Function that defines how keys are extracted from the data stream.
	 * @param <T>         Type of events in the data stream.
	 * @param <K>         Type of the extracted keys.
	 * @return The reinterpretation of the {@link DataStream} as a {@link KeyedStream}.
	 */
	public static <T, K> KeyedStream<T, K> reinterpretAsKeyedStream(
		DataStream<T> stream,
		KeySelector<T, K> keySelector) {

		return reinterpretAsKeyedStream(
			stream,
			keySelector,
			TypeExtractor.getKeySelectorTypes(keySelector, stream.getType()));
	}

	/**
	 * Reinterprets the given {@link DataStream} as a {@link KeyedStream}, which extracts keys with the given
	 * {@link KeySelector}.
	 *
	 * <p>IMPORTANT: For every partition of the base stream, the keys of events in the base stream must be
	 * partitioned exactly in the same way as if it was created through a {@link DataStream#keyBy(KeySelector)}.
	 *
	 * @param stream      The data stream to reinterpret. For every partition, this stream must be partitioned exactly
	 *                    in the same way as if it was created through a {@link DataStream#keyBy(KeySelector)}.
	 * @param keySelector Function that defines how keys are extracted from the data stream.
	 * @param typeInfo    Explicit type information about the key type.
	 * @param <T>         Type of events in the data stream.
	 * @param <K>         Type of the extracted keys.
	 * @return The reinterpretation of the {@link DataStream} as a {@link KeyedStream}.
	 */
	public static <T, K> KeyedStream<T, K> reinterpretAsKeyedStream(
		DataStream<T> stream,
		KeySelector<T, K> keySelector,
		TypeInformation<K> typeInfo) {

		PartitionTransformation<T> partitionTransformation = new PartitionTransformation<>(
			stream.getTransformation(),
			new ForwardPartitioner<>());

		return new KeyedStream<>(
			stream,
			partitionTransformation,
			keySelector,
			typeInfo);
	}

	private static class CallExecute extends Thread {

		private final StreamExecutionEnvironment toTrigger;
		private final SocketStreamIterator<?> toNotify;

		private CallExecute(StreamExecutionEnvironment toTrigger, SocketStreamIterator<?> toNotify) {
			this.toTrigger = toTrigger;
			this.toNotify = toNotify;
		}

		@Override
		public void run(){
			try {
				toTrigger.execute();
			}
			catch (Throwable t) {
				toNotify.notifyOfError(t);
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Private constructor to prevent instantiation.
	 */
	private DataStreamUtils() {}
}
