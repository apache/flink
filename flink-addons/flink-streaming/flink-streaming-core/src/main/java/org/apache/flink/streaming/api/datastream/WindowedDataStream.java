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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.function.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.function.aggregation.SumAggregator;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.GroupedWindowInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowReduceInvokable;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;

/**
 * A {@link WindowedDataStream} represents a data stream that has been divided
 * into windows (predefined chunks). User defined function such as
 * {@link #reduce(ReduceFunction)}, {@link #reduceGroup(GroupReduceFunction)} or
 * aggregations can be applied to the windows.
 * 
 * @param <OUT>
 *            The output type of the {@link WindowedDataStream}
 */
public class WindowedDataStream<OUT> {

	protected DataStream<OUT> dataStream;
	protected boolean isGrouped;
	protected boolean allCentral;
	protected KeySelector<OUT, ?> keySelector;

	protected List<WindowingHelper<OUT>> triggerHelpers;
	protected List<WindowingHelper<OUT>> evictionHelpers;

	protected LinkedList<TriggerPolicy<OUT>> userTriggers;
	protected LinkedList<EvictionPolicy<OUT>> userEvicters;

	protected WindowedDataStream(DataStream<OUT> dataStream, WindowingHelper<OUT>... policyHelpers) {
		this.dataStream = dataStream.copy();
		this.triggerHelpers = new ArrayList<WindowingHelper<OUT>>();
		for (WindowingHelper<OUT> helper : policyHelpers) {
			this.triggerHelpers.add(helper);
		}

		if (dataStream instanceof GroupedDataStream) {
			this.isGrouped = true;
			this.keySelector = ((GroupedDataStream<OUT>) dataStream).keySelector;
			// set all policies distributed
			this.allCentral = false;

		} else {
			this.isGrouped = false;
			// set all policies central
			this.allCentral = true;
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
			this.isGrouped = true;
			this.keySelector = ((GroupedDataStream<OUT>) dataStream).keySelector;
			// set all policies distributed
			this.allCentral = false;

		} else {
			this.isGrouped = false;
			// set all policies central
			this.allCentral = true;
		}
	}

	protected WindowedDataStream(WindowedDataStream<OUT> windowedDataStream) {
		this.dataStream = windowedDataStream.dataStream.copy();
		this.isGrouped = windowedDataStream.isGrouped;
		this.keySelector = windowedDataStream.keySelector;
		this.triggerHelpers = windowedDataStream.triggerHelpers;
		this.evictionHelpers = windowedDataStream.evictionHelpers;
		this.userTriggers = windowedDataStream.userTriggers;
		this.userEvicters = windowedDataStream.userEvicters;
		this.allCentral = windowedDataStream.allCentral;
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
		WindowedDataStream<OUT> ret = this.copy();
		ret.dataStream = ret.dataStream.groupBy(fields);
		ret.isGrouped = true;
		ret.keySelector = ((GroupedDataStream<OUT>) ret.dataStream).keySelector;
		return ret;
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
		WindowedDataStream<OUT> ret = this.copy();
		ret.dataStream = ret.dataStream.groupBy(fields);
		ret.isGrouped = true;
		ret.keySelector = ((GroupedDataStream<OUT>) ret.dataStream).keySelector;
		return ret;
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
		ret.dataStream = ret.dataStream.groupBy(keySelector);
		ret.isGrouped = true;
		ret.keySelector = ((GroupedDataStream<OUT>) ret.dataStream).keySelector;
		return ret;
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
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reduceFunction) {
		return dataStream.transform("WindowReduce", getType(),
				getReduceInvokable(reduceFunction));
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
	public <R> SingleOutputStreamOperator<R, ?> reduceGroup(
			GroupReduceFunction<OUT, R> reduceFunction) {

		TypeInformation<OUT> inType = getType();
		TypeInformation<R> outType = TypeExtractor
				.getGroupReduceReturnTypes(reduceFunction, inType);

		return dataStream.transform("WindowReduce", outType,
				getReduceGroupInvokable(reduceFunction));
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
	 * to detect type information using:
	 * {@link #reduceGroup(GroupReduceFunction)}
	 * 
	 * @param reduceFunction
	 *            The reduce function that will be applied to the windows.
	 * @return The transformed DataStream
	 */
	public <R> SingleOutputStreamOperator<R, ?> reduceGroup(
			GroupReduceFunction<OUT, R> reduceFunction, TypeInformation<R> outType) {

		return dataStream.transform("WindowReduce", outType,
				getReduceGroupInvokable(reduceFunction));
	}

	/**
	 * Applies an aggregation that sums every window of the data stream at the
	 * given position.
	 * 
	 * @param positionToSum
	 *            The position in the tuple/array to sum
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> sum(int positionToSum) {
		dataStream.checkFieldRange(positionToSum);
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(positionToSum,
				dataStream.getClassAtPos(positionToSum), getType()));
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
	public SingleOutputStreamOperator<OUT, ?> sum(String field) {
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
	public SingleOutputStreamOperator<OUT, ?> min(int positionToMin) {
		dataStream.checkFieldRange(positionToMin);
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
	public SingleOutputStreamOperator<OUT, ?> min(String field) {
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
	public SingleOutputStreamOperator<OUT, ?> minBy(int positionToMinBy) {
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
	public SingleOutputStreamOperator<OUT, ?> minBy(String positionToMinBy) {
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
	public SingleOutputStreamOperator<OUT, ?> minBy(int positionToMinBy, boolean first) {
		dataStream.checkFieldRange(positionToMinBy);
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
	public SingleOutputStreamOperator<OUT, ?> minBy(String field, boolean first) {
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
	public SingleOutputStreamOperator<OUT, ?> max(int positionToMax) {
		dataStream.checkFieldRange(positionToMax);
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
	public SingleOutputStreamOperator<OUT, ?> max(String field) {
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
	public SingleOutputStreamOperator<OUT, ?> maxBy(int positionToMaxBy) {
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
	public SingleOutputStreamOperator<OUT, ?> maxBy(String positionToMaxBy) {
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
	public SingleOutputStreamOperator<OUT, ?> maxBy(int positionToMaxBy, boolean first) {
		dataStream.checkFieldRange(positionToMaxBy);
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
	public SingleOutputStreamOperator<OUT, ?> maxBy(String field, boolean first) {
		return aggregate(ComparableAggregator.getAggregator(field, getType(),
				AggregationType.MAXBY, first));
	}

	private SingleOutputStreamOperator<OUT, ?> aggregate(AggregationFunction<OUT> aggregator) {
		StreamInvokable<OUT, OUT> invokable = getReduceInvokable(aggregator);

		SingleOutputStreamOperator<OUT, ?> returnStream = dataStream.transform("windowReduce",
				getType(), invokable);

		return returnStream;
	}

	private LinkedList<TriggerPolicy<OUT>> getTriggers() {

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

	private LinkedList<EvictionPolicy<OUT>> getEvicters() {

		LinkedList<EvictionPolicy<OUT>> evicters = new LinkedList<EvictionPolicy<OUT>>();

		if (evictionHelpers != null) {
			for (WindowingHelper<OUT> helper : evictionHelpers) {
				evicters.add(helper.toEvict());
			}
		} else {
			if (userEvicters == null) {
				boolean notOnlyTime=false;
				for (WindowingHelper<OUT> helper : triggerHelpers){
					if (helper instanceof Time<?>){
						evicters.add(helper.toEvict());
					} else {
						notOnlyTime=true;
					}
				}
				if (notOnlyTime){
					evicters.add(new TumblingEvictionPolicy<OUT>());
				}
			}
		}

		if (userEvicters != null) {
			evicters.addAll(userEvicters);
		}

		return evicters;
	}

	private LinkedList<TriggerPolicy<OUT>> getCentralTriggers() {
		LinkedList<TriggerPolicy<OUT>> cTriggers = new LinkedList<TriggerPolicy<OUT>>();
		if (allCentral) {
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

	private LinkedList<CloneableTriggerPolicy<OUT>> getDistributedTriggers() {
		LinkedList<CloneableTriggerPolicy<OUT>> dTriggers = null;

		if (!allCentral) {
			dTriggers = new LinkedList<CloneableTriggerPolicy<OUT>>();
			for (TriggerPolicy<OUT> trigger : getTriggers()) {
				if (!(trigger instanceof TimeTriggerPolicy)) {
					dTriggers.add((CloneableTriggerPolicy<OUT>) trigger);
				}
			}
		}

		return dTriggers;
	}

	private LinkedList<CloneableEvictionPolicy<OUT>> getDistributedEvicters() {
		LinkedList<CloneableEvictionPolicy<OUT>> evicters = null;

		if (!allCentral) {
			evicters = new LinkedList<CloneableEvictionPolicy<OUT>>();
			for (EvictionPolicy<OUT> evicter : getEvicters()) {
				evicters.add((CloneableEvictionPolicy<OUT>) evicter);
			}
		}

		return evicters;
	}

	private LinkedList<EvictionPolicy<OUT>> getCentralEvicters() {
		if (allCentral) {
			return getEvicters();
		} else {
			return null;
		}
	}

	private <R> StreamInvokable<OUT, R> getReduceGroupInvokable(GroupReduceFunction<OUT, R> reducer) {
		StreamInvokable<OUT, R> invokable;
		if (isGrouped) {
			invokable = new GroupedWindowInvokable<OUT, R>(clean(reducer), keySelector,
					getDistributedTriggers(), getDistributedEvicters(), getCentralTriggers(),
					getCentralEvicters());

		} else {
			invokable = new WindowGroupReduceInvokable<OUT, R>(clean(reducer), getTriggers(),
					getEvicters());
		}
		return invokable;
	}

	private StreamInvokable<OUT, OUT> getReduceInvokable(ReduceFunction<OUT> reducer) {
		StreamInvokable<OUT, OUT> invokable;
		if (isGrouped) {
			invokable = new GroupedWindowInvokable<OUT, OUT>(clean(reducer), keySelector,
					getDistributedTriggers(), getDistributedEvicters(), getCentralTriggers(),
					getCentralEvicters());

		} else {
			invokable = new WindowReduceInvokable<OUT>(clean(reducer), getTriggers(), getEvicters());
		}
		return invokable;
	}

	/**
	 * Gets the output type.
	 * 
	 * @return The output type.
	 */
	public TypeInformation<OUT> getType() {
		return dataStream.getType();
	}

	public DataStream<OUT> getDataStream() {
		return dataStream;
	}

	protected WindowedDataStream<OUT> copy() {
		return new WindowedDataStream<OUT>(this);
	}
}
