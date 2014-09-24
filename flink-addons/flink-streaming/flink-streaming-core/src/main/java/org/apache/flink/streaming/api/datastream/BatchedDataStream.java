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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.MaxAggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.MaxByAggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.MinAggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.MinByAggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.SumAggregationFunction;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.BatchGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.BatchReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.GroupedBatchGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.GroupedBatchReduceInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;

/**
 * A {@link BatchedDataStream} represents a data stream whose elements are
 * batched together in a sliding batch. operations like
 * {@link #reduce(ReduceFunction)} or {@link #reduceGroup(GroupReduceFunction)}
 * are applied for each batch and the batch is slid afterwards.
 *
 * @param <OUT>
 *            The output type of the {@link BatchedDataStream}
 */
public class BatchedDataStream<OUT> {

	protected DataStream<OUT> dataStream;
	protected boolean isGrouped;
	protected int keyPosition;
	protected long batchSize;
	protected long slideSize;

	protected BatchedDataStream(DataStream<OUT> dataStream, long batchSize, long slideSize) {
		if (dataStream instanceof GroupedDataStream) {
			this.isGrouped = true;
			this.keyPosition = ((GroupedDataStream<OUT>) dataStream).keyPosition;
		} else {
			this.isGrouped = false;
		}
		this.dataStream = dataStream.copy();
		this.batchSize = batchSize;
		this.slideSize = slideSize;
	}

	protected BatchedDataStream(BatchedDataStream<OUT> batchedDataStream) {
		this.dataStream = batchedDataStream.dataStream.copy();
		this.isGrouped = batchedDataStream.isGrouped;
		this.keyPosition = batchedDataStream.keyPosition;
		this.batchSize = batchedDataStream.batchSize;
		this.slideSize = batchedDataStream.slideSize;
	}

	/**
	 * Groups the elements of the {@link BatchedDataStream} by the given key
	 * position to be used with grouped operators.
	 * 
	 * @param keyPosition
	 *            The position of the field on which the
	 *            {@link BatchedDataStream} will be grouped.
	 * @return The transformed {@link BatchedDataStream}
	 */
	public BatchedDataStream<OUT> groupBy(int keyPosition) {
		return new BatchedDataStream<OUT>(dataStream.groupBy(keyPosition), batchSize, slideSize);
	}

	/**
	 * Applies a reduce transformation on every sliding batch/window of the data
	 * stream. If the data stream is grouped then the reducer is applied on
	 * every group of elements sharing the same key. This type of reduce is much
	 * faster than reduceGroup since the reduce function can be applied
	 * incrementally. The user can also extend the {@link RichReduceFunction} to
	 * gain access to other features provided by the {@link RichFuntion}
	 * interface.
	 * 
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values in the batch/window.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reducer) {
		return dataStream.addFunction("batchReduce", reducer, new FunctionTypeWrapper<OUT>(reducer,
				ReduceFunction.class, 0), new FunctionTypeWrapper<OUT>(reducer,
				ReduceFunction.class, 0), getReduceInvokable(reducer));
	}

	/**
	 * Applies a reduceGroup transformation on preset batches/windows of the
	 * DataStream. The transformation calls a {@link GroupReduceFunction} for
	 * each batch/window. Each GroupReduceFunction call can return any number of
	 * elements including none. The user can also extend
	 * {@link RichGroupReduceFunction} to gain access to other features provided
	 * by the {@link RichFuntion} interface.
	 * 
	 * @param reducer
	 *            The {@link GroupReduceFunction} that will be called for every
	 *            batch/window.
	 * @return The transformed DataStream.
	 */
	public <R> SingleOutputStreamOperator<R, ?> reduceGroup(GroupReduceFunction<OUT, R> reducer) {
		return dataStream.addFunction("batchReduce", reducer, new FunctionTypeWrapper<OUT>(reducer,
				GroupReduceFunction.class, 0), new FunctionTypeWrapper<R>(reducer,
				GroupReduceFunction.class, 1), getGroupReduceInvokable(reducer));
	}

	/**
	 * Applies an aggregation that sums every sliding batch/window of the data
	 * stream at the given position.
	 * 
	 * @param positionToSum
	 *            The position in the data point to sum
	 * @return The transformed DataStream.
	 */
	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, ?> sum(int positionToSum) {
		dataStream.checkFieldRange(positionToSum);
		return aggregate((AggregationFunction<OUT>) SumAggregationFunction.getSumFunction(
				positionToSum, dataStream.getClassAtPos(positionToSum)));
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
	 * Applies an aggregation that that gives the minimum of every sliding
	 * batch/window of the data stream at the given position.
	 * 
	 * @param positionToMin
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> min(int positionToMin) {
		dataStream.checkFieldRange(positionToMin);
		return aggregate(new MinAggregationFunction<OUT>(positionToMin));
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
		return aggregate(new MinByAggregationFunction<OUT>(positionToMinBy, first));
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
		return aggregate(new MaxAggregationFunction<OUT>(positionToMax));
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
		return aggregate(new MaxByAggregationFunction<OUT>(positionToMaxBy, first));
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
	 * Gets the output type.
	 * 
	 * @return The output type.
	 */
	public TypeInformation<OUT> getOutputType() {
		return dataStream.getOutputType();
	}

	private SingleOutputStreamOperator<OUT, ?> aggregate(AggregationFunction<OUT> aggregate) {
		StreamInvokable<OUT, OUT> invokable = getReduceInvokable(aggregate);

		SingleOutputStreamOperator<OUT, ?> returnStream = dataStream.addFunction("batchReduce",
				aggregate, dataStream.outTypeWrapper, dataStream.outTypeWrapper, invokable);

		return returnStream;
	}

	protected BatchReduceInvokable<OUT> getReduceInvokable(ReduceFunction<OUT> reducer) {
		BatchReduceInvokable<OUT> invokable;
		if (isGrouped) {
			invokable = new GroupedBatchReduceInvokable<OUT>(reducer, batchSize, slideSize,
					keyPosition);
		} else {
			invokable = new BatchReduceInvokable<OUT>(reducer, batchSize, slideSize);
		}
		return invokable;
	}

	protected <R> BatchGroupReduceInvokable<OUT, R> getGroupReduceInvokable(
			GroupReduceFunction<OUT, R> reducer) {
		BatchGroupReduceInvokable<OUT, R> invokable;
		if (isGrouped) {
			invokable = new GroupedBatchGroupReduceInvokable<OUT, R>(reducer, batchSize, slideSize,
					keyPosition);
		} else {
			invokable = new BatchGroupReduceInvokable<OUT, R>(reducer, batchSize, slideSize);
		}
		return invokable;
	}

	protected BatchedDataStream<OUT> copy() {
		return new BatchedDataStream<OUT>(this);
	}

}
