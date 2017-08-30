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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.windows.WindowTypeInformation;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A collection of utilities for {@link DataStream DataStreams}.
 */
public final class DataStreamUtils {

	/**
	 * Setups a aggregation with pre aggregation stop on a given {@link WindowedStream} using a
	 * {@link PreAggregationOperator}. Please check {@link PreAggregationOperator} for more specific information
	 * (including limitations).
	 *
	 * <p>Supplied {@link AggregateFunction} and {@link WindowAssigner} will be decomposed into two separate steps,
	 * one for pre aggregation and second for final aggregation. In final aggregation step only merging of previously
	 * created windows and accumulators will happen.
	 */
	public static <K, IN, ACC, OUT, W extends Window> SingleOutputStreamOperator<OUT> aggregateWithPreAggregation(
			DataStream<IN> input,
			KeySelector<IN, K> keySelector,
			AggregateFunction<IN, ACC, OUT> aggregateFunction,
			WindowAssigner<? super IN, W> windowAssigner,
			boolean flushAllOnWatermarks) {

		TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keySelector, input.getType());
		TypeInformation<W> windowType = new WindowTypeInformation<>(windowAssigner);
		TypeInformation<ACC> accumulatorType = TypeExtractor.getAggregateFunctionAccumulatorType(
			aggregateFunction, input.getType(), null, false);
		TupleTypeInfo<Tuple3<K, W, ACC>> intermediateResultType = new TupleTypeInfo<>(
			keyType,
			windowType,
			accumulatorType);
		TypeInformation<OUT> resultType = TypeExtractor.getAggregateFunctionReturnType(
			aggregateFunction, input.getType(), null, false);

		PreAggregationOperator<K, IN, ACC, W> preAggregationOperator = new PreAggregationOperator<>(
			aggregateFunction,
			keySelector,
			keyType,
			accumulatorType,
			windowAssigner,
			flushAllOnWatermarks);

		return input
			.transform(preAggregationOperator.toString(), intermediateResultType, preAggregationOperator)
			.keyBy(0)
			.window(new FinalAggregationWindowAssigner<>(windowAssigner))
			.aggregate(new FinalAggregationFunction<>(aggregateFunction), accumulatorType, resultType);
	}

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

	/**
	 * This wraps the supplied {@link WindowAssigner}, so that it can be used on final aggregation step after
	 * {@link PreAggregationOperator}.
	 */
	@PublicEvolving
	public static class FinalAggregationWindowAssigner<W extends Window> extends WindowAssigner<Tuple3<?, W, ?>, W> {
		private WindowAssigner<?, W> preAggregationWindowAssigner;

		public FinalAggregationWindowAssigner(WindowAssigner<?, W> preAggregationWindowAssigner) {
			this.preAggregationWindowAssigner = preAggregationWindowAssigner;
		}

		@Override
		public Collection<W> assignWindows(Tuple3<?, W, ?> element, long timestamp, WindowAssignerContext context) {
			return Collections.singleton(element.f1);
		}

		@Override
		public Trigger<Tuple3<?, W, ?>, W> getDefaultTrigger(StreamExecutionEnvironment env) {
			return (Trigger<Tuple3<?, W, ?>, W>) preAggregationWindowAssigner.getDefaultTrigger(env);
		}

		@Override
		public TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig) {
			return preAggregationWindowAssigner.getWindowSerializer(executionConfig);
		}

		@Override
		public boolean isEventTime() {
			return preAggregationWindowAssigner.isEventTime();
		}
	}

	/**
	 * This wraps the supplied {@link AggregateFunction}, so that it can be used on final aggregation step after
	 * {@link PreAggregationOperator}.
	 */
	@PublicEvolving
	public static class FinalAggregationFunction<K, W extends Window, ACC, OUT>
			implements AggregateFunction<Tuple3<K, W, ACC>, ACC, OUT> {
		private AggregateFunction<?, ACC, OUT> aggregateFunction;

		public FinalAggregationFunction(AggregateFunction<?, ACC, OUT> aggregateFunction) {
			this.aggregateFunction = checkNotNull(aggregateFunction);
		}

		@Override
		public ACC createAccumulator() {
			return aggregateFunction.createAccumulator();
		}

		@Override
		public ACC add(Tuple3<K, W, ACC> value, ACC accumulator) {
			return aggregateFunction.merge(value.f2, accumulator);
		}

		@Override
		public OUT getResult(ACC accumulator) {
			return aggregateFunction.getResult(accumulator);
		}

		@Override
		public ACC merge(ACC a, ACC b) {
			return aggregateFunction.merge(a, b);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Private constructor to prevent instantiation.
	 */
	private DataStreamUtils() {}
}
