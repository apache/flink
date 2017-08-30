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

package org.apache.flink.contrib.streaming;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This operator perform preliminary aggregation of the input values on non-keyed stream. This means that the output
 * is not fully aggregated, but only partially. It should be placed before keyBy of the final aggregation.
 * {@link DataStreamUtils#aggregateWithPreAggregation(DataStream, KeySelector, AggregateFunction, WindowAssigner)}
 * provides a useful wrapper that automatically creates a matching final aggregation step.
 *
 * <p>Pre aggregation can be useful in couple of scenarios:
 * <ol>
 * 		<li>Performing keyBy operation with low number of distinct values in the key. In such case
 * 		{@link PreAggregationOperator} can reduce both CPU usage and network usage, by pre-aggregating most of the
 * 		values before shuffling them over the network.</li>
 * 		<li>Increasing the parallelism above the number of distinct values for the task preceding the keyBy operation.</li>
 * 		<li>Handling the data skew of some of the key values. Normally if there is a data skew, a lot of work can be
 * 		dumped onto one single CPU core in the cluster. With pre aggregation some of that work can be performed in more
 * 		distributed fashion by the {@link PreAggregationOperator}.</li>
 * 		<li>Output partitioning of the data source is correlated with keyBy partitioning. For example when data source
 * 		is partitioned by day and keyBy function shuffles the data based by day and hour.</li>
 * </ol>
 *
 * <p>Because this operator performs only pre aggregation, it doesn't output the result of {@link AggregateFunction}
 * but rather it outputs a tuple containing the Key, Window, and Accumulator, where Accumulator is a partially
 * aggregated result {@link AggregateFunction}.
 *
 * <p>Keep in mind that {@link PreAggregationOperator} can have significant higher memory consumption compared to
 * normal aggregation. If the input data are either not partitioned or the input partitioning is not correlated with
 * the {@code keySelector}, each instance {@link PreAggregationOperator} can end up having each own accumulators entry
 * per each key. In other words in that case memory consumption is expected to be {@code parallelism} times larger
 * compared to what {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator} would have.
 *
 * <p>It is expected that this operator should be followed by keyBy operation based on {@code tuple.f0} and after that
 * followed by {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator} which perform
 * {@link AggregateFunction#merge(ACC, ACC)}.
 *
 * <p>Because currently {@link PreAggregationOperator} does not use {@link org.apache.flink.streaming.api.TimerService}
 * only two elements triggering policies are supported:
 * <ol>
 * 		<li>Flush everything on any watermark.</li>
 * 		<li>Iterate over each element in the state on each watermark and emit it if watermark's timestamp exceeds
 * 		{@link Window#maxTimestamp()}.</li>
 * </ol>
 * The first option has a drawback that it will often unnecessary emit elements, that could potentially be further
 * aggregated. The second one is quite CPU intensive if watermarks are emitted relatively often to the number of pre
 * aggregated key values.
 *
 * <p>Other limitations and notes:
 * <ol>
 * 		<li>{@link MergingWindowAssigner} is not supported.</li>
 * 		<li>{@link PreAggregationOperator} emits all of its data on each received watermark.</li>
 * 		<li>On restoring from checkpoint keys can be randomly shuffled between {@link PreAggregationOperator}
 * 		instances</li>.
 * </ol>
 */
@PublicEvolving
public class PreAggregationOperator<K, IN, ACC, W extends Window>
	extends AbstractStreamOperator<Tuple3<K, W, ACC>>
	implements OneInputStreamOperator<IN, Tuple3<K, W, ACC>>, Serializable {

	protected final AggregateFunction<IN, ACC, ?> aggregateFunction;
	protected final KeySelector<IN, K> keySelector;
	protected final WindowAssigner<? super IN, W> windowAssigner;
	protected final TypeInformation<K> keyTypeInformation;
	protected final TypeInformation<ACC> accumulatorTypeInformation;
	protected final boolean flushAllOnWatermark;
	protected final Map<Tuple2<K, W>, ACC> aggregates = new HashMap<>();

	protected transient WindowAssigner.WindowAssignerContext windowAssignerContext;
	protected transient ListState<Tuple3<K, W, ACC>> aggregatesState;

	/**
	 * Creates {@link PreAggregationOperator}.
	 *
	 * @param aggregateFunction function used for aggregation. Note, {@link AggregateFunction#getResult(Object)} will
	 *                          not be used.
	 * @param keySelector
	 * @param keyTypeInformation
	 * @param accumulatorTypeInformation
	 * @param windowAssigner
	 * @param flushAllOnWatermark flag to control whether all elements should be emitted on any watermark. Check more
	 *                            information in {@link PreAggregationOperator}.
	 */
	public PreAggregationOperator(
			AggregateFunction<IN, ACC, ?> aggregateFunction,
			KeySelector<IN, K> keySelector,
			TypeInformation<K> keyTypeInformation,
			TypeInformation<ACC> accumulatorTypeInformation,
			WindowAssigner<? super IN, W> windowAssigner,
			boolean flushAllOnWatermark) {
		this.aggregateFunction = checkNotNull(aggregateFunction, "aggregateFunction is null");
		this.keySelector = checkNotNull(keySelector, "keySelector is null");
		this.windowAssigner = checkNotNull(windowAssigner, "windowAssigner is null");
		this.keyTypeInformation = checkNotNull(keyTypeInformation, "keyTypeInformation is null");
		this.accumulatorTypeInformation = checkNotNull(accumulatorTypeInformation, "accumulatorTypeInformation is null");
		this.flushAllOnWatermark = flushAllOnWatermark;

		checkNotNull(keyTypeInformation, "keyTypeInformation is null");
		checkNotNull(accumulatorTypeInformation, "accumulatorTypeInformation is null");

		checkArgument(!(windowAssigner instanceof MergingWindowAssigner),
			"MergingWindowAssigner is not supported by the PreAggregationOperator");
	}

	@Override
	public void open() throws Exception {
		windowAssignerContext = new WindowAssigner.WindowAssignerContext() {
			@Override
			public long getCurrentProcessingTime() {
				throw new UnsupportedOperationException(
					"Using processing time with PreAggregationOperator would affect the results.");
			}
		};
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		TypeSerializer<Tuple3<K, W, ACC>> typeSerializer = new TupleSerializer<>(
			(Class<Tuple3<K, W, ACC>>) (Class<?>) Tuple3.class,
			new TypeSerializer<?>[] {
				keyTypeInformation.createSerializer(getExecutionConfig()),
				windowAssigner.getWindowSerializer(getExecutionConfig()),
				accumulatorTypeInformation.createSerializer(getExecutionConfig())
			});

		ListStateDescriptor<Tuple3<K, W, ACC>> mapStateDescriptor = new ListStateDescriptor<>("map-state-descriptor", typeSerializer);

		aggregatesState = context.getOperatorStateStore().getListState(mapStateDescriptor);

		if (context.isRestored()) {
			aggregates.clear();

			for (Tuple3<K, W, ACC> tuple : aggregatesState.get()) {
				Tuple2<K, W> key = new Tuple2<>(tuple.f0, tuple.f1);
				ACC accumulator = aggregates.get(key);
				if (accumulator != null) {
					accumulator = aggregateFunction.merge(accumulator, tuple.f2);
				}
				else {
					accumulator = tuple.f2;
				}
				aggregates.put(key, checkNotNull(accumulator, "accumulator is null"));
			}
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		aggregatesState.clear();
		for (Map.Entry<Tuple2<K, W>, ACC> entry : aggregates.entrySet()) {
			aggregatesState.add(new Tuple3<>(entry.getKey().f0, entry.getKey().f1, entry.getValue()));
		}
	}

	@Override
	public void processElement(StreamRecord<IN> streamRecord) throws Exception {
		Collection<W> windows = windowAssigner.assignWindows(streamRecord.getValue(), streamRecord.getTimestamp(), windowAssignerContext);
		K key = keySelector.getKey(streamRecord.getValue());

		for (W window : windows) {
			Tuple2<K, W> tuple = new Tuple2<>(key, window);
			ACC accumulator = aggregates.get(tuple);

			if (accumulator == null) {
				accumulator = aggregateFunction.createAccumulator();
			}

			accumulator = aggregateFunction.add(streamRecord.getValue(), accumulator);
			aggregates.put(tuple, accumulator);
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		if (flushAllOnWatermark) {
			flush();
		}
		else {
			trigger(mark.getTimestamp());
		}
		super.processWatermark(mark);
	}

	private void trigger(long timestamp) {
		for (Iterator<Map.Entry<Tuple2<K, W>, ACC>> it = aggregates.entrySet().iterator(); it.hasNext();) {
			Map.Entry<Tuple2<K, W>, ACC> entry = it.next();
			Tuple2<K, W> key = entry.getKey();
			ACC value = entry.getValue();
			if (key.f1.maxTimestamp() <= timestamp) {
				collect(key, value);
				it.remove();
			}
		}
	}

	protected void flush() {
		aggregates.forEach((key, value) -> collect(key, value));
		aggregates.clear();
	}

	protected void collect(Tuple2<K, W> key, ACC value) {
		output.collect(new StreamRecord<>(Tuple3.of(key.f0, key.f1, value), key.f1.maxTimestamp()));
	}

	@Override
	public String toString() {
		return String.format(
			"%s(%s)",
			this.getClass().getSimpleName(),
			windowAssigner);
	}
}
