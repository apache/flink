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
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.invokable.operator.GroupedReduceInvokable;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;

/**
 * A GroupedDataStream represents a {@link DataStream} which has been
 * partitioned by the given key in the values. Operators like {@link #reduce},
 * {@link #batchReduce} etc. can be applied on the {@link GroupedDataStream} to
 * get additional functionality by the grouping.
 *
 * @param <OUT>
 *            The output type of the {@link GroupedDataStream}.
 */
public class GroupedDataStream<OUT> extends DataStream<OUT> {

	int keyPosition;

	protected GroupedDataStream(DataStream<OUT> dataStream, int keyPosition) {
		super(dataStream.partitionBy(keyPosition));
		this.keyPosition = keyPosition;
	}

	protected GroupedDataStream(GroupedDataStream<OUT> dataStream) {
		super(dataStream);
		this.keyPosition = dataStream.keyPosition;
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
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reducer) {
		return addFunction("groupReduce", reducer, new FunctionTypeWrapper<OUT>(reducer,
				ReduceFunction.class, 0), new FunctionTypeWrapper<OUT>(reducer,
				ReduceFunction.class, 0), new GroupedReduceInvokable<OUT>(reducer, keyPosition));
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
	public SingleOutputStreamOperator<OUT, ?> min(final int positionToMin) {
		return super.min(positionToMin);
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
	public SingleOutputStreamOperator<OUT, ?> max(final int positionToMax) {
		return super.max(positionToMax);
	}

	@Override
	protected SingleOutputStreamOperator<OUT, ?> aggregate(AggregationFunction<OUT> aggregate) {

		GroupedReduceInvokable<OUT> invokable = new GroupedReduceInvokable<OUT>(aggregate,
				keyPosition);

		SingleOutputStreamOperator<OUT, ?> returnStream = addFunction("groupReduce", aggregate,
				outTypeWrapper, outTypeWrapper, invokable);

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
