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
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.streaming.util.FieldAccessor;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class DataStreamUtils {

	/**
	 * Returns an iterator to iterate over the elements of the DataStream.
	 * @return The iterator
	 */
	public static <OUT> Iterator<OUT> collect(DataStream<OUT> stream) {
		TypeSerializer<OUT> serializer = stream.getType().createSerializer(stream.getExecutionEnvironment().getConfig());
		DataStreamIterator<OUT> it = new DataStreamIterator<OUT>(serializer);

		//Find out what IP of us should be given to CollectSink, that it will be able to connect to
		StreamExecutionEnvironment env = stream.getExecutionEnvironment();
		InetAddress clientAddress;
		if(env instanceof RemoteStreamEnvironment) {
			String host = ((RemoteStreamEnvironment)env).getHost();
			int port = ((RemoteStreamEnvironment)env).getPort();
			try {
				clientAddress = NetUtils.findConnectingAddress(new InetSocketAddress(host, port), 2000, 400);
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

		(new CallExecute<OUT>(stream)).start();

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


	public static class Statistics {

		/**
		 * Approximately counts the distinct values in a data stream.
		 *
		 * The precision can be set by the rsd parameter.
		 *
		 * @param rsd
		 *            The relative standard deviation of the result from
		 *            the exact result. Smaller values create counters
		 *            that require more space.
		 * @param stream
		 *            The {@link DataStream} to work with
		 * @return The transformed {@link DataStream}.
		 */
		public static <T> DataStream<Long> countDistinctApprox(double rsd, DataStream<T> stream) {
			return stream.map(new CountDistinctMapFunction<T>(new CountDistinctApprox<T>(rsd))).setParallelism(1);
		}

		/**
		 * Approximately counts the distinct values in a data stream at the
		 * specified field position, and writes the result to another
		 * (or back to the same) field.
		 *
		 * The precision can be set by the rsd parameter.
		 *
		 * The output field must be of type Integer.
		 *
		 * @param inPos
		 *            The input position in the tuple/array
		 * @param outPos
		 *            The output position in the tuple/array
		 * @param rsd
		 *            The relative standard deviation of the result from
		 *            the exact result. Smaller values create counters
		 *            that require more space.
		 * @param stream
		 *            The {@link DataStream} to work with
		 * @return The transformed {@link DataStream}.
		 */
		public static <T> DataStream<T> countDistinctApprox(int inPos, int outPos, double rsd,
															DataStream<T> stream) {
			return stream.map(new CountDistinctFieldMapFunction<T, Object>(
					FieldAccessor.create(inPos, stream.getType(), stream.getExecutionConfig()),
					FieldAccessor.<T, Long>create(outPos, stream.getType(), stream.getExecutionConfig()),
					new CountDistinctApprox<Object>(rsd)
			)).setParallelism(1);
		}

		/**
		 * Approximately counts the distinct values in a data stream at the
		 * specified field, and writes the result to another
		 * (or back to the same) field.
		 *
		 * The precision can be set by the rsd parameter.
		 *
		 * The output field must be of type Integer.
		 *
		 * The fields can be specified by a field expression that can be either
		 * the name of a public field or a getter method with parentheses of the
		 * stream's underlying type. A dot can be used to drill down into objects,
		 * as in {@code "field1.getInnerField2()" }.
		 *
		 * @param inField
		 *            The input position in the tuple/array
		 * @param outField
		 *            The output position in the tuple/array
		 * @param rsd
		 *            The relative standard deviation of the result from
		 *            the exact result. Smaller values create counters
		 *            that require more space.
		 * @param stream
		 *            The {@link DataStream} to work with
		 * @return The transformed {@link DataStream}.
		 */
		public static <T> DataStream<T> countDistinctApprox(String inField, String outField, double rsd,
															DataStream<T> stream) {
			return stream.map(new CountDistinctFieldMapFunction<T, Object>(
					FieldAccessor.create(inField, stream.getType(), stream.getExecutionConfig()),
					FieldAccessor.<T, Long>create(outField, stream.getType(), stream.getExecutionConfig()),
					new CountDistinctApprox<Object>(rsd)
			)).setParallelism(1);
		}

		protected interface CountDistinct<T> {
			public Long offer(T elem);
		}

		/**
		 * Calculates count distinct using the specified implementation,
		 */
		protected static class CountDistinctMapFunction<R> implements MapFunction<R, Long> {
			CountDistinct<R> countDistinctImpl;

			public CountDistinctMapFunction(CountDistinct<R> countDistinctImpl) {
				this.countDistinctImpl = countDistinctImpl;
			}

			@Override
			public Long map(R record) throws Exception {
				return countDistinctImpl.offer(record);
			}
		}

		/**
		 * Calculates count distinct of one field using the specified implementation,
		 * and writes the result to an other field.
		 */
		protected static class CountDistinctFieldMapFunction<R, F> implements MapFunction<R, R> {
			FieldAccessor<R, F> inAccessor;
			FieldAccessor<R, Long> outAccessor;

			CountDistinct<F> countDistinctImpl;

			public CountDistinctFieldMapFunction(FieldAccessor<R, F> inAccessor, FieldAccessor<R, Long> outAccessor,
										CountDistinct<F> countDistinctImpl) {
				this.inAccessor = inAccessor;
				this.outAccessor = outAccessor;
				this.countDistinctImpl = countDistinctImpl;
			}

			@Override
			public R map(R record) throws Exception {
				F fieldVal = inAccessor.get(record);
				return outAccessor.set(record, countDistinctImpl.offer(fieldVal));
			}
		}

		protected static class CountDistinctApprox<T> implements CountDistinct<T>, Serializable {

			private static final long serialVersionUID = 1L;

			ICardinality card;

			public CountDistinctApprox(double rsd) {
				card = new HyperLogLog(rsd);
			}

			public Long offer(T elem) {
				card.offer(elem);
				return card.cardinality();
			}
		}
	}
}
