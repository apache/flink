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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.function.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.function.aggregation.SumAggregator;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.windowing.GroupedStreamDiscretizer;
import org.apache.flink.streaming.api.invokable.operator.windowing.StreamDiscretizer;
import org.apache.flink.streaming.api.invokable.operator.windowing.StreamWindow;
import org.apache.flink.streaming.api.invokable.operator.windowing.StreamWindowTypeInfo;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;

/**
 * A {@link WindowedDataStream} represents a data stream that has been divided
 * into windows (predefined chunks). User defined function such as
 * {@link #reduceWindow(ReduceFunction)},
 * {@link #mapWindow(GroupReduceFunction)} or aggregations can be applied to the
 * windows.
 * 
 * @param <OUT>
 *            The output type of the {@link WindowedDataStream}
 */
public class WindowedDataStream<OUT> {

	protected DataStream<OUT> dataStream;

	protected boolean isLocal = false;
	protected boolean isCentral = true;

	protected KeySelector<OUT, ?> discretizerKey;
	protected KeySelector<OUT, ?> groupByKey;

	protected List<WindowingHelper<OUT>> triggerHelpers;
	protected List<WindowingHelper<OUT>> evictionHelpers;

	protected LinkedList<TriggerPolicy<OUT>> userTriggers;
	protected LinkedList<EvictionPolicy<OUT>> userEvicters;

	protected WindowedDataStream() {

	}

	protected WindowedDataStream(DataStream<OUT> dataStream, WindowingHelper<OUT>... policyHelpers) {
		this.dataStream = dataStream.copy();
		this.triggerHelpers = new ArrayList<WindowingHelper<OUT>>();
		for (WindowingHelper<OUT> helper : policyHelpers) {
			this.triggerHelpers.add(helper);
		}

		if (dataStream instanceof GroupedDataStream) {
			this.discretizerKey = ((GroupedDataStream<OUT>) dataStream).keySelector;
			// set all policies distributed
			this.isCentral = false;
		}
	}

	protected WindowedDataStream(DataStream<OUT> dataStream, List<TriggerPolicy<OUT>> triggers,
			List<EvictionPolicy<OUT>> evicters) {
		this.dataStream = dataStream.copy();

		if (triggers != null) {
			this.userTriggers = new LinkedList<TriggerPolicy<OUT>>();
			this.userTriggers.addAll(triggers);
		}

		if (evicters != null) {
			this.userEvicters = new LinkedList<EvictionPolicy<OUT>>();
			this.userEvicters.addAll(evicters);
		}

		if (dataStream instanceof GroupedDataStream) {
			this.discretizerKey = ((GroupedDataStream<OUT>) dataStream).keySelector;
			// set all policies distributed
			this.isCentral = false;

		}
	}

	protected WindowedDataStream(WindowedDataStream<OUT> windowedDataStream) {
		this.dataStream = windowedDataStream.dataStream.copy();
		this.discretizerKey = windowedDataStream.discretizerKey;
		this.groupByKey = windowedDataStream.groupByKey;
		this.triggerHelpers = windowedDataStream.triggerHelpers;
		this.evictionHelpers = windowedDataStream.evictionHelpers;
		this.userTriggers = windowedDataStream.userTriggers;
		this.userEvicters = windowedDataStream.userEvicters;
		this.isCentral = windowedDataStream.isCentral;
		this.isLocal = windowedDataStream.isLocal;
	}

	public <F> F clean(F f) {
		return dataStream.clean(f);
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
	 * @param policyHelpers
	 *            The policies that define the triggering frequency
	 * 
	 * @return The windowed data stream with triggering set
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public WindowedDataStream<OUT> every(WindowingHelper... policyHelpers) {
		WindowedDataStream<OUT> ret = this.copy();
		if (ret.evictionHelpers == null) {
			ret.evictionHelpers = ret.triggerHelpers;
			ret.triggerHelpers = new ArrayList<WindowingHelper<OUT>>();
		}
		for (WindowingHelper<OUT> helper : policyHelpers) {
			ret.triggerHelpers.add(helper);
		}
		return ret;
	}

	/**
	 * Groups the elements of the {@link WindowedDataStream} by the given key
	 * positions. The window sizes (evictions) and slide sizes (triggers) will
	 * be calculated on the whole stream (in a central fashion), but the user
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
	 * be calculated on the whole stream (in a central fashion), but the user
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
	 * (triggers) will be calculated on the whole stream (in a central fashion),
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
		return groupBy(clean(KeySelectorUtil.getSelectorForKeys(keys, getType())));
	}

	/**
	 * Sets the windowed computations local, so that the windowing and reduce or
	 * aggregation logic will be computed for each parallel instance of this
	 * operator
	 * 
	 * @return The local windowed data stream
	 */
	public WindowedDataStream<OUT> local() {
		WindowedDataStream<OUT> out = copy();
		out.isLocal = true;
		return out;
	}

	private DiscretizedStream<OUT> discretize(boolean isMap) {

		StreamInvokable<OUT, StreamWindow<OUT>> discretizer;

		if (discretizerKey == null) {
			discretizer = new StreamDiscretizer<OUT>(getTriggers(), getEvicters());
		} else {
			discretizer = new GroupedStreamDiscretizer<OUT>(discretizerKey,
					getDistributedTriggers(), getDistributedEvicters(), getCentralTriggers(),
					getCentralEvicters());
		}

		int parallelism = isLocal || (discretizerKey != null) ? dataStream.environment
				.getDegreeOfParallelism() : 1;

		return new DiscretizedStream<OUT>(dataStream.transform("Stream Discretizer",
				new StreamWindowTypeInfo<OUT>(getType()), discretizer).setParallelism(parallelism),
				groupByKey);

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

		return discretize(false).reduceWindow(reduceFunction);
	}

	/**
	 * Applies a reduceGroup transformation on the windowed data stream by
	 * reducing the current window at every trigger. In contrast with the
	 * standard binary reducer, with reduceGroup the user can access all
	 * elements of the window at the same time through the iterable interface.
	 * The user can also extend the {@link RichGroupReduceFunction} to gain
	 * access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param reduceFunction
	 *            The reduce function that will be applied to the windows.
	 * @return The transformed DataStream
	 */
	public <R> WindowedDataStream<R> mapWindow(GroupReduceFunction<OUT, R> reduceFunction) {
		return discretize(true).mapWindow(reduceFunction);
	}

	public DataStream<OUT> flatten() {
		return dataStream;
	}

	/**
	 * Applies a reduceGroup transformation on the windowed data stream by
	 * reducing the current window at every trigger. In contrast with the
	 * standard binary reducer, with reduceGroup the user can access all
	 * elements of the window at the same time through the iterable interface.
	 * The user can also extend the {@link RichGroupReduceFunction} to gain
	 * access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * </br> </br> This version of reduceGroup uses user supplied
	 * typeinformation for serializaton. Use this only when the system is unable
	 * to detect type information using: {@link #mapWindow(GroupReduceFunction)}
	 * 
	 * @param reduceFunction
	 *            The reduce function that will be applied to the windows.
	 * @return The transformed DataStream
	 */
	public <R> SingleOutputStreamOperator<R, ?> mapWindow(
			GroupReduceFunction<OUT, R> reduceFunction, TypeInformation<R> outType) {

		throw new RuntimeException("Not implemented yet");
	}

	protected Class<?> getClassAtPos(int pos) {
		return dataStream.getClassAtPos(pos);
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
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(field, getType()));
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
				false));
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
				AggregationType.MINBY, first));
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
				false));
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
				AggregationType.MAXBY, first));
	}

	private WindowedDataStream<OUT> aggregate(AggregationFunction<OUT> aggregator) {

		return reduceWindow(aggregator);
	}

	protected LinkedList<TriggerPolicy<OUT>> getTriggers() {

		LinkedList<TriggerPolicy<OUT>> triggers = new LinkedList<TriggerPolicy<OUT>>();

		if (triggerHelpers != null) {
			for (WindowingHelper<OUT> helper : triggerHelpers) {
				triggers.add(helper.toTrigger());
			}
		}

		if (userTriggers != null) {
			triggers.addAll(userTriggers);
		}

		return triggers;

	}

	protected LinkedList<EvictionPolicy<OUT>> getEvicters() {

		LinkedList<EvictionPolicy<OUT>> evicters = new LinkedList<EvictionPolicy<OUT>>();

		if (evictionHelpers != null) {
			for (WindowingHelper<OUT> helper : evictionHelpers) {
				evicters.add(helper.toEvict());
			}
		} else {
			if (userEvicters == null) {
				boolean notOnlyTime = false;
				for (WindowingHelper<OUT> helper : triggerHelpers) {
					if (helper instanceof Time<?>) {
						evicters.add(helper.toEvict());
					} else {
						notOnlyTime = true;
					}
				}
				if (notOnlyTime) {
					evicters.add(new TumblingEvictionPolicy<OUT>());
				}
			}
		}

		if (userEvicters != null) {
			evicters.addAll(userEvicters);
		}

		return evicters;
	}

	protected LinkedList<TriggerPolicy<OUT>> getCentralTriggers() {
		LinkedList<TriggerPolicy<OUT>> cTriggers = new LinkedList<TriggerPolicy<OUT>>();
		if (isCentral) {
			cTriggers.addAll(getTriggers());
		} else {
			for (TriggerPolicy<OUT> trigger : getTriggers()) {
				if (trigger instanceof TimeTriggerPolicy) {
					cTriggers.add(trigger);
				}
			}
		}
		return cTriggers;
	}

	protected LinkedList<CloneableTriggerPolicy<OUT>> getDistributedTriggers() {
		LinkedList<CloneableTriggerPolicy<OUT>> dTriggers = null;

		if (!isCentral) {
			dTriggers = new LinkedList<CloneableTriggerPolicy<OUT>>();
			for (TriggerPolicy<OUT> trigger : getTriggers()) {
				if (!(trigger instanceof TimeTriggerPolicy)) {
					dTriggers.add((CloneableTriggerPolicy<OUT>) trigger);
				}
			}
		}

		return dTriggers;
	}

	protected LinkedList<CloneableEvictionPolicy<OUT>> getDistributedEvicters() {
		LinkedList<CloneableEvictionPolicy<OUT>> evicters = null;

		if (!isCentral) {
			evicters = new LinkedList<CloneableEvictionPolicy<OUT>>();
			for (EvictionPolicy<OUT> evicter : getEvicters()) {
				evicters.add((CloneableEvictionPolicy<OUT>) evicter);
			}
		}

		return evicters;
	}

	protected LinkedList<EvictionPolicy<OUT>> getCentralEvicters() {
		if (isCentral) {
			return getEvicters();
		} else {
			return null;
		}
	}

	/**
	 * Gets the output type.
	 * 
	 * @return The output type.
	 */
	public TypeInformation<OUT> getType() {
		return dataStream.getType();
	}

	protected DataStream<OUT> getDataStream() {
		return dataStream;
	}

	protected WindowedDataStream<OUT> copy() {
		return new WindowedDataStream<OUT>(this);
	}

	protected boolean isGrouped() {
		return groupByKey != null;
	}

	public DataStream<StreamWindow<OUT>> getDiscretizedStream() {
		return discretize(true).getDiscretizedStream();
	}

	protected static class WindowKey<R> implements KeySelector<StreamWindow<R>, Integer> {

		private static final long serialVersionUID = 1L;
		
		@Override
		public Integer getKey(StreamWindow<R> value) throws Exception {
			return value.windowID;
		}

	}
}
