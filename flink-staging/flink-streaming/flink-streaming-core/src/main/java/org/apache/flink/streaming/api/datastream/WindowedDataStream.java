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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.streaming.api.function.RichWindowMapFunction;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.function.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.function.aggregation.SumAggregator;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.windowing.GroupedStreamDiscretizer;
import org.apache.flink.streaming.api.invokable.operator.windowing.GroupedTimeDiscretizer;
import org.apache.flink.streaming.api.invokable.operator.windowing.StreamDiscretizer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.StreamWindowTypeInfo;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBuffer;
import org.apache.flink.streaming.api.windowing.windowbuffer.CompletePreAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.TumblingGroupedPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.TumblingPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;

/**
 * A {@link WindowedDataStream} represents a data stream that has been
 * discretised into windows. User defined function such as
 * {@link #reduceWindow(ReduceFunction)}, {@link #mapWindow()} or aggregations
 * can be applied to the windows. The results of these transformations are also
 * WindowedDataStreams of the same discretisation unit.
 * 
 * @param <OUT>
 *            The output type of the {@link WindowedDataStream}
 */
public class WindowedDataStream<OUT> {

	protected enum WindowTransformation {
		REDUCEWINDOW, MAPWINDOW, NONE;
		private Function UDF;

		public WindowTransformation with(Function UDF) {
			this.UDF = UDF;
			return this;
		}
	}

	protected DataStream<OUT> dataStream;

	protected boolean isLocal = false;

	protected KeySelector<OUT, ?> discretizerKey;
	protected KeySelector<OUT, ?> groupByKey;

	protected WindowingHelper<OUT> triggerHelper;
	protected WindowingHelper<OUT> evictionHelper;

	protected TriggerPolicy<OUT> userTrigger;
	protected EvictionPolicy<OUT> userEvicter;

	protected WindowedDataStream(DataStream<OUT> dataStream, WindowingHelper<OUT> policyHelper) {
		this.dataStream = dataStream.copy();
		this.triggerHelper = policyHelper;

		if (dataStream instanceof GroupedDataStream) {
			this.discretizerKey = ((GroupedDataStream<OUT>) dataStream).keySelector;
		}
	}

	protected WindowedDataStream(DataStream<OUT> dataStream, TriggerPolicy<OUT> trigger,
			EvictionPolicy<OUT> evicter) {
		this.dataStream = dataStream.copy();

		this.userTrigger = trigger;
		this.userEvicter = evicter;

		if (dataStream instanceof GroupedDataStream) {
			this.discretizerKey = ((GroupedDataStream<OUT>) dataStream).keySelector;
		}
	}

	protected WindowedDataStream(WindowedDataStream<OUT> windowedDataStream) {
		this.dataStream = windowedDataStream.dataStream.copy();
		this.discretizerKey = windowedDataStream.discretizerKey;
		this.groupByKey = windowedDataStream.groupByKey;
		this.triggerHelper = windowedDataStream.triggerHelper;
		this.evictionHelper = windowedDataStream.evictionHelper;
		this.userTrigger = windowedDataStream.userTrigger;
		this.userEvicter = windowedDataStream.userEvicter;
		this.isLocal = windowedDataStream.isLocal;
	}

	public WindowedDataStream() {
	}

	/**
	 * Defines the slide size (trigger frequency) for the windowed data stream.
	 * This controls how often the user defined function will be triggered on
	 * the window. </br></br> For example to get a window of 5 elements with a
	 * slide of 2 seconds use: </br></br>
	 * {@code ds.window(Count.of(5)).every(Time.of(2,TimeUnit.SECONDS))}
	 * </br></br> The user function in this case will be called on the 5 most
	 * recent elements every 2 seconds
	 * 
	 * @param policyHelper
	 *            The policy that define the triggering frequency
	 * 
	 * @return The windowed data stream with triggering set
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public WindowedDataStream<OUT> every(WindowingHelper policyHelper) {
		WindowedDataStream<OUT> ret = this.copy();
		if (ret.evictionHelper == null) {
			ret.evictionHelper = ret.triggerHelper;
			ret.triggerHelper = policyHelper;
		}

		return ret;
	}

	/**
	 * Groups the elements of the {@link WindowedDataStream} by the given key
	 * positions. The window sizes (evictions) and slide sizes (triggers) will
	 * be calculated on the whole stream (in a global fashion), but the user
	 * defined functions will be applied on a per group basis. </br></br> To get
	 * windows and triggers on a per group basis apply the
	 * {@link DataStream#window} operator on an already grouped data stream.
	 * 
	 * @param fields
	 *            The position of the fields to group by.
	 * @return The grouped {@link WindowedDataStream}
	 */
	public WindowedDataStream<OUT> groupBy(int... fields) {
		if (getType() instanceof BasicArrayTypeInfo || getType() instanceof PrimitiveArrayTypeInfo) {
			return groupBy(new KeySelectorUtil.ArrayKeySelector<OUT>(fields));
		} else {
			return groupBy(new Keys.ExpressionKeys<OUT>(fields, getType()));
		}
	}

	/**
	 * Groups the elements of the {@link WindowedDataStream} by the given field
	 * expressions. The window sizes (evictions) and slide sizes (triggers) will
	 * be calculated on the whole stream (in a global fashion), but the user
	 * defined functions will be applied on a per group basis. </br></br> To get
	 * windows and triggers on a per group basis apply the
	 * {@link DataStream#window} operator on an already grouped data stream.
	 * </br></br> A field expression is either the name of a public field or a
	 * getter method with parentheses of the stream's underlying type. A dot can
	 * be used to drill down into objects, as in
	 * {@code "field1.getInnerField2()" }.
	 * 
	 * @param fields
	 *            The fields to group by
	 * @return The grouped {@link WindowedDataStream}
	 */
	public WindowedDataStream<OUT> groupBy(String... fields) {
		return groupBy(new Keys.ExpressionKeys<OUT>(fields, getType()));
	}

	/**
	 * Groups the elements of the {@link WindowedDataStream} using the given
	 * {@link KeySelector}. The window sizes (evictions) and slide sizes
	 * (triggers) will be calculated on the whole stream (in a global fashion),
	 * but the user defined functions will be applied on a per group basis.
	 * </br></br> To get windows and triggers on a per group basis apply the
	 * {@link DataStream#window} operator on an already grouped data stream.
	 * 
	 * @param keySelector
	 *            The keySelector used to extract the key for grouping.
	 * @return The grouped {@link WindowedDataStream}
	 */
	public WindowedDataStream<OUT> groupBy(KeySelector<OUT, ?> keySelector) {
		WindowedDataStream<OUT> ret = this.copy();
		ret.groupByKey = keySelector;
		return ret;
	}

	private WindowedDataStream<OUT> groupBy(Keys<OUT> keys) {
		return groupBy(dataStream.clean(KeySelectorUtil.getSelectorForKeys(keys, getType(),
				getExecutionConfig())));
	}

	/**
	 * Sets the window discretisation local, meaning that windows will be
	 * created in parallel at environment parallelism.
	 * 
	 * @return The WindowedDataStream with local discretisation
	 */
	public WindowedDataStream<OUT> local() {
		WindowedDataStream<OUT> out = copy();
		out.isLocal = true;
		return out;
	}

	/**
	 * Returns the {@link DataStream} of {@link StreamWindow}s which represent
	 * the discretised stream. There is no ordering guarantee for the received
	 * windows.
	 * 
	 * @return The discretised stream
	 */
	public DataStream<StreamWindow<OUT>> getDiscretizedStream() {
		return discretize(WindowTransformation.NONE, new BasicWindowBuffer<OUT>())
				.getDiscretizedStream();
	}

	/**
	 * Flattens the results of the window computations and streams out the
	 * window elements.
	 * 
	 * @return The data stream consisting of the individual records.
	 */
	public DataStream<OUT> flatten() {
		return dataStream;
	}

	/**
	 * Applies a reduce transformation on the windowed data stream by reducing
	 * the current window at every trigger.The user can also extend the
	 * {@link RichReduceFunction} to gain access to other features provided by
	 * the {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param reduceFunction
	 *            The reduce function that will be applied to the windows.
	 * @return The transformed DataStream
	 */
	public DiscretizedStream<OUT> reduceWindow(ReduceFunction<OUT> reduceFunction) {

		WindowTransformation transformation = WindowTransformation.REDUCEWINDOW
				.with(reduceFunction);

		WindowBuffer<OUT> windowBuffer = getWindowBuffer(transformation, getTrigger(),
				getEviction(), discretizerKey);

		if (windowBuffer instanceof CompletePreAggregator) {
			return discretize(transformation, windowBuffer);
		} else {
			return discretize(transformation, windowBuffer).reduceWindow(reduceFunction);
		}
	}

	/**
	 * Applies a mapWindow transformation on the windowed data stream by calling
	 * the mapWindow function on the window at every trigger. In contrast with
	 * the standard binary reducer, with mapWindow allows the user to access all
	 * elements of the window at the same time through the iterable interface.
	 * The user can also extend the {@link RichWindowMapFunction} to gain access
	 * to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param windowMapFunction
	 *            The function that will be applied to the windows.
	 * @return The transformed DataStream
	 */
	public <R> WindowedDataStream<R> mapWindow(WindowMapFunction<OUT, R> windowMapFunction) {
		return discretize(WindowTransformation.MAPWINDOW.with(windowMapFunction),
				new BasicWindowBuffer<OUT>()).mapWindow(windowMapFunction);
	}

	/**
	 * Applies a mapWindow transformation on the windowed data stream by calling
	 * the mapWindow function on the window at every trigger. In contrast with
	 * the standard binary reducer, with mapWindow allows the user to access all
	 * elements of the window at the same time through the iterable interface.
	 * The user can also extend the {@link RichWindowMapFunction} to gain access
	 * to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * </br> </br> This version of mapWindow uses user supplied typeinformation
	 * for serializaton. Use this only when the system is unable to detect type
	 * information.
	 * 
	 * @param windowMapFunction
	 *            The function that will be applied to the windows.
	 * @param outType
	 *            The output type of the operator.
	 * @return The transformed DataStream
	 */
	public <R> WindowedDataStream<R> mapWindow(WindowMapFunction<OUT, R> windowMapFunction,
			TypeInformation<R> outType) {

		return discretize(WindowTransformation.MAPWINDOW.with(windowMapFunction),
				new BasicWindowBuffer<OUT>()).mapWindow(windowMapFunction, outType);
	}

	private DiscretizedStream<OUT> discretize(WindowTransformation transformation,
			WindowBuffer<OUT> windowBuffer) {

		StreamInvokable<OUT, StreamWindow<OUT>> discretizer = getDiscretizer(transformation,
				windowBuffer, getTrigger(), getEviction(), discretizerKey);

		int parallelism = getDiscretizerParallelism();

		return new DiscretizedStream<OUT>(dataStream.transform("Stream Discretizer",
				new StreamWindowTypeInfo<OUT>(getType()), discretizer).setParallelism(parallelism),
				groupByKey, transformation);

	}

	private int getDiscretizerParallelism() {
		return isLocal || (discretizerKey != null) ? dataStream.environment
				.getDegreeOfParallelism() : 1;
	}

	private StreamInvokable<OUT, StreamWindow<OUT>> getDiscretizer(
			WindowTransformation transformation, WindowBuffer<OUT> windowBuffer,
			TriggerPolicy<OUT> trigger, EvictionPolicy<OUT> eviction,
			KeySelector<OUT, ?> discretizerKey) {

		if (discretizerKey == null) {
			return new StreamDiscretizer<OUT>(trigger, eviction, windowBuffer);
		} else if (trigger instanceof TimeTriggerPolicy
				&& ((TimeTriggerPolicy<OUT>) trigger).timestampWrapper.isDefaultTimestamp()) {
			return new GroupedTimeDiscretizer<OUT>(discretizerKey,
					(TimeTriggerPolicy<OUT>) trigger, (CloneableEvictionPolicy<OUT>) eviction,
					windowBuffer);
		} else {
			return new GroupedStreamDiscretizer<OUT>(discretizerKey,
					(CloneableTriggerPolicy<OUT>) trigger, (CloneableEvictionPolicy<OUT>) eviction,
					windowBuffer);
		}

	}

	@SuppressWarnings("unchecked")
	private WindowBuffer<OUT> getWindowBuffer(WindowTransformation transformation,
			TriggerPolicy<OUT> trigger, EvictionPolicy<OUT> eviction,
			KeySelector<OUT, ?> discretizerKey) {

		if (transformation == WindowTransformation.REDUCEWINDOW
				&& eviction instanceof TumblingEvictionPolicy) {
			if (groupByKey == null) {
				return new TumblingPreReducer<OUT>(
						dataStream.clean((ReduceFunction<OUT>) transformation.UDF), getType()
								.createSerializer(getExecutionConfig()));
			} else {
				return new TumblingGroupedPreReducer<OUT>(
						dataStream.clean((ReduceFunction<OUT>) transformation.UDF), groupByKey,
						getType().createSerializer(getExecutionConfig()));
			}
		}
		return new BasicWindowBuffer<OUT>();
	}

	/**
	 * Applies an aggregation that sums every window of the data stream at the
	 * given position.
	 * 
	 * @param positionToSum
	 *            The position in the tuple/array to sum
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> sum(int positionToSum) {
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(positionToSum,
				getClassAtPos(positionToSum), getType()));
	}

	/**
	 * Applies an aggregation that sums every window of the pojo data stream at
	 * the given field for every window. </br></br> A field expression is either
	 * the name of a public field or a getter method with parentheses of the
	 * stream's underlying type. A dot can be used to drill down into objects,
	 * as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param positionToSum
	 *            The field to sum
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> sum(String field) {
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(field, getType(),
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the minimum value of every window
	 * of the data stream at the given position.
	 * 
	 * @param positionToMin
	 *            The position to minimize
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> min(int positionToMin) {
		return aggregate(ComparableAggregator.getAggregator(positionToMin, getType(),
				AggregationType.MIN));
	}

	/**
	 * Applies an aggregation that that gives the minimum value of the pojo data
	 * stream at the given field expression for every window. </br></br>A field
	 * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> min(String field) {
		return aggregate(ComparableAggregator.getAggregator(field, getType(), AggregationType.MIN,
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * minimum value the operator returns the first element by default.
	 * 
	 * @param positionToMinBy
	 *            The position to minimize by
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> minBy(int positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * minimum value the operator returns the first element by default.
	 * 
	 * @param positionToMinBy
	 *            The position to minimize by
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> minBy(String positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that gives the minimum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * minimum value the operator returns either the first or last one depending
	 * on the parameter setting.
	 * 
	 * @param positionToMinBy
	 *            The position to minimize
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            minimum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> minBy(int positionToMinBy, boolean first) {
		return aggregate(ComparableAggregator.getAggregator(positionToMinBy, getType(),
				AggregationType.MINBY, first));
	}

	/**
	 * Applies an aggregation that that gives the minimum element of the pojo
	 * data stream by the given field expression for every window. A field
	 * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @param first
	 *            If True then in case of field equality the first object will
	 *            be returned
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> minBy(String field, boolean first) {
		return aggregate(ComparableAggregator.getAggregator(field, getType(),
				AggregationType.MINBY, first, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the maximum value of every window of
	 * the data stream at the given position.
	 * 
	 * @param positionToMax
	 *            The position to maximize
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> max(int positionToMax) {
		return aggregate(ComparableAggregator.getAggregator(positionToMax, getType(),
				AggregationType.MAX));
	}

	/**
	 * Applies an aggregation that that gives the maximum value of the pojo data
	 * stream at the given field expression for every window. A field expression
	 * is either the name of a public field or a getter method with parentheses
	 * of the {@link DataStream}S underlying type. A dot can be used to drill
	 * down into objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> max(String field) {
		return aggregate(ComparableAggregator.getAggregator(field, getType(), AggregationType.MAX,
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * maximum value the operator returns the first by default.
	 * 
	 * @param positionToMaxBy
	 *            The position to maximize by
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> maxBy(int positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * maximum value the operator returns the first by default.
	 * 
	 * @param positionToMaxBy
	 *            The position to maximize by
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> maxBy(String positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that gives the maximum element of every window of
	 * the data stream by the given position. If more elements have the same
	 * maximum value the operator returns either the first or last one depending
	 * on the parameter setting.
	 * 
	 * @param positionToMaxBy
	 *            The position to maximize by
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            maximum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> maxBy(int positionToMaxBy, boolean first) {
		return aggregate(ComparableAggregator.getAggregator(positionToMaxBy, getType(),
				AggregationType.MAXBY, first));
	}

	/**
	 * Applies an aggregation that that gives the maximum element of the pojo
	 * data stream by the given field expression for every window. A field
	 * expression is either the name of a public field or a getter method with
	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @param first
	 *            If True then in case of field equality the first object will
	 *            be returned
	 * @return The transformed DataStream.
	 */
	public WindowedDataStream<OUT> maxBy(String field, boolean first) {
		return aggregate(ComparableAggregator.getAggregator(field, getType(),
				AggregationType.MAXBY, first, getExecutionConfig()));
	}

	private WindowedDataStream<OUT> aggregate(AggregationFunction<OUT> aggregator) {
		return reduceWindow(aggregator);
	}

	protected TriggerPolicy<OUT> getTrigger() {

		if (triggerHelper != null) {
			return triggerHelper.toTrigger();
		} else if (userTrigger != null) {
			return userTrigger;
		} else {
			throw new RuntimeException("Trigger must not be null");
		}

	}

	protected EvictionPolicy<OUT> getEviction() {

		if (evictionHelper != null) {
			return evictionHelper.toEvict();
		} else if (userEvicter == null) {
			if (triggerHelper instanceof Time) {
				return triggerHelper.toEvict();
			} else {
				return new TumblingEvictionPolicy<OUT>();
			}
		} else {
			return userEvicter;
		}

	}

	protected boolean isGrouped() {
		return groupByKey != null;
	}

	/**
	 * Gets the output type.
	 * 
	 * @return The output type.
	 */
	public TypeInformation<OUT> getType() {
		return dataStream.getType();
	}

	public ExecutionConfig getExecutionConfig() {
		return dataStream.getExecutionConfig();
	}

	protected Class<?> getClassAtPos(int pos) {
		return dataStream.getClassAtPos(pos);
	}

	protected WindowedDataStream<OUT> copy() {
		return new WindowedDataStream<OUT>(this);
	}

	protected static class WindowKey<R> implements KeySelector<StreamWindow<R>, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer getKey(StreamWindow<R> value) throws Exception {
			return value.windowID;
		}

	}
}
