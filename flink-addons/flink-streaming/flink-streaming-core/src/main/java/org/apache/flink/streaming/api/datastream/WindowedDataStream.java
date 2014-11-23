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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.function.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.function.aggregation.SumAggregator;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.GroupedWindowingInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowingGroupInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowingReduceInvokable;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.TimeTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CloneableTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.TumblingEvictionPolicy;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;

/**
 * A {@link WindowedDataStream} represents a data stream whose elements are
 * batched together in a sliding batch. operations like
 * {@link #reduce(ReduceFunction)} or {@link #reduceGroup(GroupReduceFunction)}
 * are applied for each batch and the batch is slid afterwards.
 *
 * @param <OUT>
 *            The output type of the {@link WindowedDataStream}
 */
public class WindowedDataStream<OUT> {

	protected DataStream<OUT> dataStream;
	protected boolean isGrouped;
	protected KeySelector<OUT, ?> keySelector;

	protected List<WindowingHelper<OUT>> triggerPolicies;
	protected List<WindowingHelper<OUT>> evictionPolicies;

	protected WindowedDataStream(DataStream<OUT> dataStream, WindowingHelper<OUT>... policyHelpers) {
		this.dataStream = dataStream.copy();
		this.triggerPolicies = new ArrayList<WindowingHelper<OUT>>();
		for (WindowingHelper<OUT> helper : policyHelpers) {
			this.triggerPolicies.add(helper);
		}

		if (dataStream instanceof GroupedDataStream) {
			this.isGrouped = true;
			this.keySelector = ((GroupedDataStream<OUT>) dataStream).keySelector;

		} else {
			this.isGrouped = false;
		}
	}

	protected WindowedDataStream(WindowedDataStream<OUT> windowedDataStream) {
		this.dataStream = windowedDataStream.dataStream.copy();
		this.isGrouped = windowedDataStream.isGrouped;
		this.keySelector = windowedDataStream.keySelector;
		this.triggerPolicies = windowedDataStream.triggerPolicies;
		this.evictionPolicies = windowedDataStream.evictionPolicies;
	}

	protected LinkedList<TriggerPolicy<OUT>> getTriggers() {
		LinkedList<TriggerPolicy<OUT>> triggerPolicyList = new LinkedList<TriggerPolicy<OUT>>();

		for (WindowingHelper<OUT> helper : triggerPolicies) {
			triggerPolicyList.add(helper.toTrigger());
		}

		return triggerPolicyList;
	}

	protected LinkedList<EvictionPolicy<OUT>> getEvicters() {
		LinkedList<EvictionPolicy<OUT>> evictionPolicyList = new LinkedList<EvictionPolicy<OUT>>();

		if (evictionPolicies != null) {
			for (WindowingHelper<OUT> helper : evictionPolicies) {
				evictionPolicyList.add(helper.toEvict());
			}
		} else {
			evictionPolicyList.add(new TumblingEvictionPolicy<OUT>());
		}

		return evictionPolicyList;
	}

	protected LinkedList<TriggerPolicy<OUT>> getCentralTriggers() {
		LinkedList<TriggerPolicy<OUT>> cTriggers = new LinkedList<TriggerPolicy<OUT>>();
		// Add Time triggers to central triggers
		for (TriggerPolicy<OUT> trigger : getTriggers()) {
			if (trigger instanceof TimeTriggerPolicy) {
				cTriggers.add(trigger);
			}
		}
		return cTriggers;
	}

	protected LinkedList<CloneableTriggerPolicy<OUT>> getDistributedTriggers() {
		LinkedList<CloneableTriggerPolicy<OUT>> dTriggers = new LinkedList<CloneableTriggerPolicy<OUT>>();

		// Everything except Time triggers are distributed
		for (TriggerPolicy<OUT> trigger : getTriggers()) {
			if (!(trigger instanceof TimeTriggerPolicy)) {
				dTriggers.add((CloneableTriggerPolicy<OUT>) trigger);
			}
		}

		return dTriggers;
	}

	protected LinkedList<CloneableEvictionPolicy<OUT>> getDistributedEvicters() {
		LinkedList<CloneableEvictionPolicy<OUT>> evicters = new LinkedList<CloneableEvictionPolicy<OUT>>();

		for (EvictionPolicy<OUT> evicter : getEvicters()) {
			evicters.add((CloneableEvictionPolicy<OUT>) evicter);
		}

		return evicters;
	}

	/**
	 * Groups the elements of the {@link WindowedDataStream} by the given
	 * {@link KeySelector} to be used with grouped operators.
	 * 
	 * @param keySelector
	 *            The specification of the key on which the
	 *            {@link WindowedDataStream} will be grouped.
	 * @return The transformed {@link WindowedDataStream}
	 */
	public WindowedDataStream<OUT> groupBy(KeySelector<OUT, ?> keySelector) {
		WindowedDataStream<OUT> ret = this.copy();
		ret.dataStream = ret.dataStream.groupBy(keySelector);
		ret.isGrouped = true;
		ret.keySelector = ((GroupedDataStream<OUT>) ret.dataStream).keySelector;
		return ret;
	}

	/**
	 * Groups the elements of the {@link WindowedDataStream} by the given key
	 * positions to be used with grouped operators.
	 * 
	 * @param fields
	 *            The position of the fields to group by.
	 * @return The transformed {@link WindowedDataStream}
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
	 * expressions to be used with grouped operators.
	 * 
	 * @param fields
	 *            The position of the fields to group by.
	 * @return The transformed {@link WindowedDataStream}
	 */
	public WindowedDataStream<OUT> groupBy(String... fields) {
		WindowedDataStream<OUT> ret = this.copy();
		ret.dataStream = ret.dataStream.groupBy(fields);
		ret.isGrouped = true;
		ret.keySelector = ((GroupedDataStream<OUT>) ret.dataStream).keySelector;
		return ret;
	}

	/**
	 * This is a prototype implementation for new windowing features based on
	 * trigger and eviction policies
	 * 
	 * @param triggerPolicies
	 *            A list of trigger policies
	 * @param evictionPolicies
	 *            A list of eviction policies
	 * @param sample
	 *            A sample of the OUT data type required to gather type
	 *            information
	 * @return The single output operator
	 */
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reduceFunction) {
		return dataStream.addFunction("NextGenWindowReduce", reduceFunction,
				dataStream.outTypeWrapper, dataStream.outTypeWrapper,
				getReduceInvokable(reduceFunction));
	}

	public <R> SingleOutputStreamOperator<R, ?> reduceGroup(
			GroupReduceFunction<OUT, R> reduceFunction) {
		return dataStream.addFunction("NextGenWindowReduce", reduceFunction,
				dataStream.outTypeWrapper, new FunctionTypeWrapper<R>(reduceFunction,
						GroupReduceFunction.class, 1), getReduceGroupInvokable(reduceFunction));
	}

	/**
	 * Applies an aggregation that sums every sliding batch/window of the data
	 * stream at the given position.
	 * 
	 * @param positionToSum
	 *            The position in the data point to sum
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> sum(int positionToSum) {
		dataStream.checkFieldRange(positionToSum);
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(positionToSum,
				dataStream.getClassAtPos(positionToSum), dataStream.getOutputType()));
	}

	/**
	 * Syntactic sugar for sum(0)
	 * 
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> sum() {
		return sum(0);
	}

	/**
	 * Applies an aggregation that that gives the sum of the pojo data stream at
	 * the given field expression. A field expression is either the name of a
	 * public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> sum(String field) {
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(field,
				getOutputType()));
	}

	/**
	 * Applies an aggregation that that gives the minimum of every sliding
	 * batch/window of the data stream at the given position.
	 * 
	 * @param positionToMin
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> min(int positionToMin) {
		dataStream.checkFieldRange(positionToMin);
		return aggregate(ComparableAggregator.getAggregator(positionToMin, getOutputType(),
				AggregationType.MIN));
	}

	/**
	 * Applies an aggregation that gives the minimum element of every sliding
	 * batch/window of the data stream by the given position. If more elements
	 * have the same minimum value the operator returns the first element by
	 * default.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> minBy(int positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that gives the minimum element of every sliding
	 * batch/window of the data stream by the given position. If more elements
	 * have the same minimum value the operator returns either the first or last
	 * one depending on the parameter setting.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            minimum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> minBy(int positionToMinBy, boolean first) {
		dataStream.checkFieldRange(positionToMinBy);
		return aggregate(ComparableAggregator.getAggregator(positionToMinBy, getOutputType(),
				AggregationType.MINBY, first));
	}

	/**
	 * Syntactic sugar for min(0)
	 * 
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> min() {
		return min(0);
	}

	/**
	 * Applies an aggregation that gives the maximum of every sliding
	 * batch/window of the data stream at the given position.
	 * 
	 * @param positionToMax
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> max(int positionToMax) {
		dataStream.checkFieldRange(positionToMax);
		return aggregate(ComparableAggregator.getAggregator(positionToMax, getOutputType(),
				AggregationType.MAX));
	}

	/**
	 * Applies an aggregation that gives the maximum element of every sliding
	 * batch/window of the data stream by the given position. If more elements
	 * have the same maximum value the operator returns the first by default.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> maxBy(int positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that gives the maximum element of every sliding
	 * batch/window of the data stream by the given position. If more elements
	 * have the same maximum value the operator returns either the first or last
	 * one depending on the parameter setting.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            maximum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> maxBy(int positionToMaxBy, boolean first) {
		dataStream.checkFieldRange(positionToMaxBy);
		return aggregate(ComparableAggregator.getAggregator(positionToMaxBy, getOutputType(),
				AggregationType.MAXBY, first));
	}

	/**
	 * Syntactic sugar for max(0)
	 * 
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> max() {
		return max(0);
	}

	/**
	 * Applies an aggregation that that gives the minimum of the pojo data
	 * stream at the given field expression. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> min(String field) {
		return aggregate(ComparableAggregator.getAggregator(field, getOutputType(),
				AggregationType.MIN, false));
	}

	/**
	 * Applies an aggregation that that gives the maximum of the pojo data
	 * stream at the given field expression. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> max(String field) {
		return aggregate(ComparableAggregator.getAggregator(field, getOutputType(),
				AggregationType.MAX, false));
	}

	/**
	 * Applies an aggregation that that gives the minimum element of the pojo
	 * data stream by the given field expression. A field expression is either
	 * the name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
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
		return aggregate(ComparableAggregator.getAggregator(field, getOutputType(),
				AggregationType.MINBY, first));
	}

	/**
	 * Applies an aggregation that that gives the maximum element of the pojo
	 * data stream by the given field expression. A field expression is either
	 * the name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
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
		return aggregate(ComparableAggregator.getAggregator(field, getOutputType(),
				AggregationType.MAXBY, first));
	}

	/**
	 * Gets the output type.
	 * 
	 * @return The output type.
	 */
	public TypeInformation<OUT> getOutputType() {
		return dataStream.getOutputType();
	}

	protected WindowedDataStream<OUT> copy() {
		return new WindowedDataStream<OUT>(this);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public WindowedDataStream<OUT> every(WindowingHelper... policyHelpers) {
		WindowedDataStream<OUT> ret = this.copy();
		if (ret.evictionPolicies == null) {
			ret.evictionPolicies = ret.triggerPolicies;
			ret.triggerPolicies = new ArrayList<WindowingHelper<OUT>>();
		}
		for (WindowingHelper<OUT> helper : policyHelpers) {
			ret.triggerPolicies.add(helper);
		}
		return ret;
	}

	private SingleOutputStreamOperator<OUT, ?> aggregate(AggregationFunction<OUT> aggregator) {
		StreamInvokable<OUT, OUT> invokable = getReduceInvokable(aggregator);

		SingleOutputStreamOperator<OUT, ?> returnStream = dataStream.addFunction("windowReduce",
				aggregator, dataStream.outTypeWrapper, dataStream.outTypeWrapper, invokable);

		return returnStream;
	}

	protected <R> StreamInvokable<OUT, R> getReduceGroupInvokable(
			GroupReduceFunction<OUT, R> reducer) {
		StreamInvokable<OUT, R> invokable;
		if (isGrouped) {
			invokable = new GroupedWindowingInvokable<OUT, R>(reducer, keySelector,
					getDistributedTriggers(), getDistributedEvicters(), getCentralTriggers());

		} else {
			invokable = new WindowingGroupInvokable<OUT, R>(reducer, getTriggers(), getEvicters());
		}
		return invokable;
	}

	protected StreamInvokable<OUT, OUT> getReduceInvokable(ReduceFunction<OUT> reducer) {
		StreamInvokable<OUT, OUT> invokable;
		if (isGrouped) {
			invokable = new GroupedWindowingInvokable<OUT, OUT>(reducer, keySelector,
					getDistributedTriggers(), getDistributedEvicters(), getCentralTriggers());

		} else {
			invokable = new WindowingReduceInvokable<OUT>(reducer, getTriggers(), getEvicters());
		}
		return invokable;
	}
}
