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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.invokable.operator.GroupedReduceInvokable;
import org.apache.flink.streaming.partitioner.StreamPartitioner;

/**
 * A GroupedDataStream represents a {@link DataStream} which has been
 * partitioned by the given {@link KeySelector}. Operators like {@link #reduce},
 * {@link #batchReduce} etc. can be applied on the {@link GroupedDataStream} to
 * get additional functionality by the grouping.
 * 
 * @param <OUT>
 *            The output type of the {@link GroupedDataStream}.
 */
public class GroupedDataStream<OUT> extends DataStream<OUT> {

	KeySelector<OUT, ?> keySelector;

	protected GroupedDataStream(DataStream<OUT> dataStream, KeySelector<OUT, ?> keySelector) {
		super(dataStream.partitionBy(keySelector));
		this.keySelector = keySelector;
	}

	protected GroupedDataStream(GroupedDataStream<OUT> dataStream) {
		super(dataStream);
		this.keySelector = dataStream.keySelector;
	}

	public KeySelector<OUT, ?> getKeySelector() {
		return this.keySelector;
	}

	/**
	 * Applies a reduce transformation on the grouped data stream grouped on by
	 * the given key position. The {@link ReduceFunction} will receive input
	 * values based on the key value. Only input values with the same key will
	 * go to the same reducer.The user can also extend
	 * {@link RichReduceFunction} to gain access to other features provided by
	 * the {@link RichFuntion} interface.
	 * 
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values with the same key.
	 * @return The transformed DataStream.
	 */
	@Override
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reducer) {
		return transform("groupReduce", getType(), new GroupedReduceInvokable<OUT>(clean(reducer),
				keySelector));
	}

	/**
	 * Applies an aggregation that sums the grouped data stream at the given
	 * position, grouped by the given key position. Input values with the same
	 * key will be summed.
	 * 
	 * @param positionToSum
	 *            The position in the data point to sum
	 * @return The transformed DataStream.
	 */
	@Override
	public SingleOutputStreamOperator<OUT, ?> sum(final int positionToSum) {
		return super.sum(positionToSum);
	}

	/**
	 * Applies an aggregation that gives the minimum of the grouped data stream
	 * at the given position, grouped by the given key position. Input values
	 * with the same key will be minimized.
	 * 
	 * @param positionToMin
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	@Override
	public SingleOutputStreamOperator<OUT, ?> min(final int positionToMin) {
		return super.min(positionToMin);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position for each group on a grouped data
	 * stream. If more elements have the minimum value at the given position,
	 * the operator returns the first one by default.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	@Override
	public SingleOutputStreamOperator<OUT, ?> minBy(int positionToMinBy) {
		return super.minBy(positionToMinBy);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position for each group on a grouped data
	 * stream. If more elements have the minimum value at the given position,
	 * the operator returns either the first or last one depending on the
	 * parameters.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            maximum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	@Override
	public SingleOutputStreamOperator<OUT, ?> minBy(int positionToMinBy, boolean first) {
		return super.minBy(positionToMinBy, first);
	}

	/**
	 * Applies an aggregation that gives the maximum of the grouped data stream
	 * at the given position, grouped by the given key position. Input values
	 * with the same key will be maximized.
	 * 
	 * @param positionToMax
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	@Override
	public SingleOutputStreamOperator<OUT, ?> max(final int positionToMax) {
		return super.max(positionToMax);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position for each group on a grouped data
	 * stream. If more elements have the maximum value at the given position,
	 * the operator returns the first one by default.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	@Override
	public SingleOutputStreamOperator<OUT, ?> maxBy(int positionToMaxBy) {
		return super.maxBy(positionToMaxBy);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position for each group on a grouped data
	 * stream. If more elements have the maximum value at the given position,
	 * the operator returns either the first or last one depending on the
	 * parameters.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            maximum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	@Override
	public SingleOutputStreamOperator<OUT, ?> maxBy(int positionToMaxBy, boolean first) {
		return super.maxBy(positionToMaxBy, first);
	}

	@Override
	protected SingleOutputStreamOperator<OUT, ?> aggregate(AggregationFunction<OUT> aggregate) {

		GroupedReduceInvokable<OUT> invokable = new GroupedReduceInvokable<OUT>(clean(aggregate),
				keySelector);

		SingleOutputStreamOperator<OUT, ?> returnStream = transform("groupReduce", getType(),
				invokable);

		return returnStream;
	}

	@Override
	protected DataStream<OUT> setConnectionType(StreamPartitioner<OUT> partitioner) {
		System.out.println("Setting the partitioning after groupBy can affect the grouping");
		return super.setConnectionType(partitioner);
	}

	@Override
	protected GroupedDataStream<OUT> copy() {
		return new GroupedDataStream<OUT>(this);
	}
}
