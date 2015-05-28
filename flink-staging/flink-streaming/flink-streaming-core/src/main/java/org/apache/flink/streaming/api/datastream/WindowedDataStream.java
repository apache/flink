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
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFoldFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.RichWindowMapFunction;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.windowing.GroupedActiveDiscretizer;
import org.apache.flink.streaming.api.operators.windowing.GroupedStreamDiscretizer;
import org.apache.flink.streaming.api.operators.windowing.GroupedWindowBuffer;
import org.apache.flink.streaming.api.operators.windowing.StreamDiscretizer;
import org.apache.flink.streaming.api.operators.windowing.StreamWindowBuffer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.StreamWindowTypeInfo;
import org.apache.flink.streaming.api.windowing.WindowEvent;
import org.apache.flink.streaming.api.windowing.WindowUtils;
import org.apache.flink.streaming.api.windowing.WindowUtils.WindowTransformation;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.CentralActiveTrigger;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.KeepAllEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBuffer;
import org.apache.flink.streaming.api.windowing.windowbuffer.JumpingCountGroupedPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.JumpingCountPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.JumpingTimeGroupedPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.JumpingTimePreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.PreAggregator;
import org.apache.flink.streaming.api.windowing.windowbuffer.SlidingCountGroupedPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.SlidingCountPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.SlidingTimeGroupedPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.SlidingTimePreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.TumblingGroupedPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.TumblingPreReducer;
import org.apache.flink.streaming.api.windowing.windowbuffer.WindowBuffer;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;

/**
 * A {@link WindowedDataStream} represents a data stream that has been
 * discretised into windows. User defined function such as
 * {@link #reduceWindow(ReduceFunction)}, {@link #mapWindow(WindowMapFunction)} or aggregations
 * can be applied to the windows. The results of these transformations are also
 * WindowedDataStreams of the same discretisation unit.
 * 
 * @param <OUT>
 *            The output type of the {@link WindowedDataStream}
 */
public class WindowedDataStream<OUT> {

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
		policyHelper.setExecutionConfig(getExecutionConfig());
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
		return groupBy(clean(KeySelectorUtil.getSelectorForKeys(keys, getType(),
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
		if (getEviction() instanceof KeepAllEvictionPolicy) {
			throw new RuntimeException("Cannot get discretized stream for full stream window");
		}
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

		// We check whether we should apply parallel time discretization, which
		// is a more complex exploiting the monotonic properties of time
		// policies
		if (WindowUtils.isTimeOnly(getTrigger(), getEviction()) && discretizerKey == null
				&& dataStream.getParallelism() > 1) {
			return timeReduce(reduceFunction);
		} else {
			WindowTransformation transformation = WindowTransformation.REDUCEWINDOW
					.with(clean(reduceFunction));

			WindowBuffer<OUT> windowBuffer = getWindowBuffer(transformation);

			DiscretizedStream<OUT> discretized = discretize(transformation, windowBuffer);

			if (windowBuffer instanceof PreAggregator) {
				return discretized;
			} else {
				return discretized.reduceWindow(reduceFunction);
			}
		}
	}

	/**
	 * Applies a fold transformation on the windowed data stream by folding the
	 * current window at every trigger.The user can also extend the
	 * {@link RichFoldFunction} to gain access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * This version of foldWindow uses user supplied typeinformation for
	 * serializaton. Use this only when the system is unable to detect type
	 * information.
	 * 
	 * @param foldFunction
	 *            The fold function that will be applied to the windows.
	 * @param initialValue
	 *            Initial value given to foldFunction
	 * @param outType
	 *            The output type of the operator
	 * @return The transformed DataStream
	 */
	public <R> DiscretizedStream<R> foldWindow(R initialValue, FoldFunction<OUT, R> foldFunction,
			TypeInformation<R> outType) {

		return discretize(WindowTransformation.FOLDWINDOW.with(clean(foldFunction)),
				new BasicWindowBuffer<OUT>()).foldWindow(initialValue, foldFunction, outType);

	}

	/**
	 * Applies a fold transformation on the windowed data stream by folding the
	 * current window at every trigger.The user can also extend the
	 * {@link RichFoldFunction} to gain access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param foldFunction
	 *            The fold function that will be applied to the windows.
	 * @param initialValue
	 *            Initial value given to foldFunction
	 * @return The transformed DataStream
	 */
	public <R> DiscretizedStream<R> foldWindow(R initialValue, FoldFunction<OUT, R> foldFunction) {

		TypeInformation<R> outType = TypeExtractor.getFoldReturnTypes(clean(foldFunction),
				getType());
		return foldWindow(initialValue, foldFunction, outType);
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
	public <R> DiscretizedStream<R> mapWindow(WindowMapFunction<OUT, R> windowMapFunction) {
		return discretize(WindowTransformation.MAPWINDOW.with(clean(windowMapFunction)),
				getWindowBuffer(WindowTransformation.MAPWINDOW)).mapWindow(windowMapFunction);
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
	public <R> DiscretizedStream<R> mapWindow(WindowMapFunction<OUT, R> windowMapFunction,
			TypeInformation<R> outType) {

		return discretize(WindowTransformation.MAPWINDOW.with(windowMapFunction),
				getWindowBuffer(WindowTransformation.MAPWINDOW)).mapWindow(windowMapFunction,
				outType);
	}

	private DiscretizedStream<OUT> discretize(WindowTransformation transformation,
			WindowBuffer<OUT> windowBuffer) {

		OneInputStreamOperator<OUT, WindowEvent<OUT>> discretizer = getDiscretizer();

		OneInputStreamOperator<WindowEvent<OUT>, StreamWindow<OUT>> bufferOperator = getBufferOperator(windowBuffer);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		TypeInformation<WindowEvent<OUT>> bufferEventType = new TupleTypeInfo(WindowEvent.class,
				getType(), BasicTypeInfo.INT_TYPE_INFO);

		int parallelism = getDiscretizerParallelism(transformation);

		return new DiscretizedStream<OUT>(dataStream
				.transform(discretizer.getClass().getSimpleName(), bufferEventType, discretizer)
				.setParallelism(parallelism)
				.transform(windowBuffer.getClass().getSimpleName(),
						new StreamWindowTypeInfo<OUT>(getType()), bufferOperator)
				.setParallelism(parallelism), groupByKey, transformation, false);

	}

	/**
	 * Returns the parallelism for the stream discretizer. The returned
	 * parallelism is either 1 for for non-parallel global policies (or when the
	 * input stream is non-parallel), environment parallelism for the policies
	 * that can run in parallel (such as, any ditributed policy, reduce by count
	 * or time).
	 * 
	 * @param transformation
	 *            The applied transformation
	 * @return The parallelism for the stream discretizer
	 */
	private int getDiscretizerParallelism(WindowTransformation transformation) {
		return isLocal
				|| (transformation == WindowTransformation.REDUCEWINDOW && WindowUtils
						.isParallelPolicy(getTrigger(), getEviction(), dataStream.getParallelism()))
				|| (discretizerKey != null) ? dataStream.environment.getParallelism() : 1;

	}

	/**
	 * Dedicated method for applying parallel time reduce transformations on
	 * windows
	 * 
	 * @param reduceFunction
	 *            Reduce function to apply
	 * @return The transformed stream
	 */
	protected DiscretizedStream<OUT> timeReduce(ReduceFunction<OUT> reduceFunction) {

		WindowTransformation transformation = WindowTransformation.REDUCEWINDOW
				.with(clean(reduceFunction));

		// We get the windowbuffer and set it to emit empty windows with
		// sequential IDs. This logic is necessarry to merge windows created in
		// parallel.
		WindowBuffer<OUT> windowBuffer = getWindowBuffer(transformation).emitEmpty().sequentialID();

		// If there is a groupby for the reduce operation we apply it before the
		// discretizers, because we will forward everything afterwards to
		// exploit task chaining
		if (groupByKey != null) {
			dataStream = dataStream.groupBy(groupByKey);
		}

		// We discretize the stream and call the timeReduce function of the
		// discretized stream, we also pass the type of the windowbuffer
		DiscretizedStream<OUT> discretized = discretize(transformation, windowBuffer);

		if (getEviction() instanceof KeepAllEvictionPolicy
				&& !(windowBuffer instanceof PreAggregator)) {
			throw new RuntimeException(
					"Error in preaggregator logic, parallel time reduce should always be preaggregated");
		}

		return discretized.timeReduce(reduceFunction);

	}

	/**
	 * Based on the defined policies, returns the stream discretizer to be used
	 */
	private OneInputStreamOperator<OUT, WindowEvent<OUT>> getDiscretizer() {
		if (discretizerKey == null) {
			return new StreamDiscretizer<OUT>(getTrigger(), getEviction());
		} else if (getTrigger() instanceof CentralActiveTrigger) {
			return new GroupedActiveDiscretizer<OUT>(discretizerKey,
					(CentralActiveTrigger<OUT>) getTrigger(),
					(CloneableEvictionPolicy<OUT>) getEviction());
		} else {
			return new GroupedStreamDiscretizer<OUT>(discretizerKey,
					(CloneableTriggerPolicy<OUT>) getTrigger(),
					(CloneableEvictionPolicy<OUT>) getEviction());
		}

	}

	private OneInputStreamOperator<WindowEvent<OUT>, StreamWindow<OUT>> getBufferOperator(
			WindowBuffer<OUT> windowBuffer) {
		if (discretizerKey == null) {
			return new StreamWindowBuffer<OUT>(windowBuffer);
		} else {
			return new GroupedWindowBuffer<OUT>(windowBuffer, discretizerKey);
		}
	}

	/**
	 * Based on the given policies returns the WindowBuffer used to store the
	 * elements in the window. This is the module that also encapsulates the
	 * pre-aggregator logic when it is applicable, reducing the space cost, and
	 * trigger latency.
	 * 
	 */
	@SuppressWarnings("unchecked")
	private WindowBuffer<OUT> getWindowBuffer(WindowTransformation transformation) {
		TriggerPolicy<OUT> trigger = getTrigger();
		EvictionPolicy<OUT> eviction = getEviction();

		if (transformation == WindowTransformation.REDUCEWINDOW) {
			if (WindowUtils.isTumblingPolicy(trigger, eviction)) {
				if (eviction instanceof KeepAllEvictionPolicy) {
					if (groupByKey == null) {
						return new TumblingPreReducer<OUT>(
								(ReduceFunction<OUT>) transformation.getUDF(), getType()
										.createSerializer(getExecutionConfig())).noEvict();
					} else {
						return new TumblingGroupedPreReducer<OUT>(
								(ReduceFunction<OUT>) transformation.getUDF(), groupByKey,
								getType().createSerializer(getExecutionConfig())).noEvict();
					}
				} else {
					if (groupByKey == null) {
						return new TumblingPreReducer<OUT>(
								(ReduceFunction<OUT>) transformation.getUDF(), getType()
										.createSerializer(getExecutionConfig()));
					} else {
						return new TumblingGroupedPreReducer<OUT>(
								(ReduceFunction<OUT>) transformation.getUDF(), groupByKey,
								getType().createSerializer(getExecutionConfig()));
					}
				}
			} else if (WindowUtils.isSlidingCountPolicy(trigger, eviction)) {
				if (groupByKey == null) {
					return new SlidingCountPreReducer<OUT>(
							clean((ReduceFunction<OUT>) transformation.getUDF()), dataStream
									.getType().createSerializer(getExecutionConfig()),
							WindowUtils.getWindowSize(eviction), WindowUtils.getSlideSize(trigger),
							((CountTriggerPolicy<?>) trigger).getStart());
				} else {
					return new SlidingCountGroupedPreReducer<OUT>(
							clean((ReduceFunction<OUT>) transformation.getUDF()), dataStream
									.getType().createSerializer(getExecutionConfig()), groupByKey,
							WindowUtils.getWindowSize(eviction), WindowUtils.getSlideSize(trigger),
							((CountTriggerPolicy<?>) trigger).getStart());
				}

			} else if (WindowUtils.isSlidingTimePolicy(trigger, eviction)) {
				if (groupByKey == null) {
					return new SlidingTimePreReducer<OUT>(
							(ReduceFunction<OUT>) transformation.getUDF(), dataStream.getType()
									.createSerializer(getExecutionConfig()),
							WindowUtils.getWindowSize(eviction), WindowUtils.getSlideSize(trigger),
							WindowUtils.getTimeStampWrapper(trigger));
				} else {
					return new SlidingTimeGroupedPreReducer<OUT>(
							(ReduceFunction<OUT>) transformation.getUDF(), dataStream.getType()
									.createSerializer(getExecutionConfig()), groupByKey,
							WindowUtils.getWindowSize(eviction), WindowUtils.getSlideSize(trigger),
							WindowUtils.getTimeStampWrapper(trigger));
				}

			} else if (WindowUtils.isJumpingCountPolicy(trigger, eviction)) {
				if (groupByKey == null) {
					return new JumpingCountPreReducer<OUT>(
							(ReduceFunction<OUT>) transformation.getUDF(), getType()
									.createSerializer(getExecutionConfig()),
							WindowUtils.getSlideSize(trigger) - WindowUtils.getWindowSize(eviction));
				} else {
					return new JumpingCountGroupedPreReducer<OUT>(
							(ReduceFunction<OUT>) transformation.getUDF(), groupByKey, getType()
									.createSerializer(getExecutionConfig()),
							WindowUtils.getSlideSize(trigger) - WindowUtils.getWindowSize(eviction));
				}
			} else if (WindowUtils.isJumpingTimePolicy(trigger, eviction)) {
				if (groupByKey == null) {
					return new JumpingTimePreReducer<OUT>(
							(ReduceFunction<OUT>) transformation.getUDF(), getType()
									.createSerializer(getExecutionConfig()),
							WindowUtils.getSlideSize(trigger), WindowUtils.getWindowSize(eviction),
							WindowUtils.getTimeStampWrapper(trigger));
				} else {
					return new JumpingTimeGroupedPreReducer<OUT>(
							(ReduceFunction<OUT>) transformation.getUDF(), groupByKey, getType()
									.createSerializer(getExecutionConfig()),
							WindowUtils.getSlideSize(trigger), WindowUtils.getWindowSize(eviction),
							WindowUtils.getTimeStampWrapper(trigger));
				}
			}
		}

		if (eviction instanceof KeepAllEvictionPolicy) {
			throw new RuntimeException(
					"Full stream policy can only be used with operations that support preaggregations, such as reduce or aggregations");
		} else {
			return new BasicWindowBuffer<OUT>();
		}
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
	 * @param field
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
		} else if (userEvicter == null || userEvicter instanceof TumblingEvictionPolicy) {
			if (triggerHelper instanceof Time) {
				return triggerHelper.toEvict();
			} else {
				return new TumblingEvictionPolicy<OUT>();
			}
		} else {
			return userEvicter;
		}

	}

	public <F> F clean(F f) {
		if (getExecutionConfig().isClosureCleanerEnabled()) {
			ClosureCleaner.clean(f, true);
		}
		ClosureCleaner.ensureSerializable(f);
		return f;
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
}
