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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.operators.StreamGroupedFold;
import org.apache.flink.streaming.api.operators.StreamGroupedReduce;

/**
 * A GroupedDataStream represents a {@link DataStream} which has been
 * partitioned by the given {@link KeySelector}. Operators like {@link #reduce},
 * {@link #fold} etc. can be applied on the {@link GroupedDataStream} to
 * get additional functionality by the grouping.
 *
 * @param <T> The type of the elements in the Grouped Stream.
 * @param <KEY> The type of the key in the Keyed Stream.
 */
public class GroupedDataStream<T, KEY> extends KeyedStream<T, KEY> {

	/**
	 * Creates a new {@link GroupedDataStream}, group inclusion is determined using
	 * a {@link KeySelector} on the elements of the {@link DataStream}.
	 *
	 * @param dataStream Base stream of data
	 * @param keySelector Function for determining group inclusion
	 */
	public GroupedDataStream(DataStream<T> dataStream, KeySelector<T, KEY> keySelector) {
		super(dataStream, keySelector);
	}


	/**
	 * Applies a reduce transformation on the grouped data stream grouped on by
	 * the given key position. The {@link ReduceFunction} will receive input
	 * values based on the key value. Only input values with the same key will
	 * go to the same reducer.
	 * 
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values with the same key.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> reduce(ReduceFunction<T> reducer) {
		return transform("Grouped Reduce", getType(), new StreamGroupedReduce<T>(
				clean(reducer), keySelector));
	}

	/**
	 * Applies a fold transformation on the grouped data stream grouped on by
	 * the given key position. The {@link FoldFunction} will receive input
	 * values based on the key value. Only input values with the same key will
	 * go to the same folder.
	 * 
	 * @param folder
	 *            The {@link FoldFunction} that will be called for every element
	 *            of the input values with the same key.
	 * @param initialValue
	 *            The initialValue passed to the folders for each key.
	 * @return The transformed DataStream.
	 */
	public <R> SingleOutputStreamOperator<R, ?> fold(R initialValue, FoldFunction<T, R> folder) {

		TypeInformation<R> outType = TypeExtractor.getFoldReturnTypes(clean(folder), getType(),
				Utils.getCallLocationName(), true);

		return transform("Grouped Fold", outType, new StreamGroupedFold<T, R>(clean(folder),
				keySelector, initialValue));
	}

	/**
	 * Applies an aggregation that gives a rolling sum of the data stream at the
	 * given position grouped by the given key. An independent aggregate is kept
	 * per key.
	 * 
	 * @param positionToSum
	 *            The position in the data point to sum
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> sum(int positionToSum) {
		return aggregate(new SumAggregator<T>(positionToSum, getType(), getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current sum of the pojo data
	 * stream at the given field expressionby the given key. An independent
	 * aggregate is kept per key. A field expression is either the name of a
	 * public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> sum(String field) {
		return aggregate(new SumAggregator<T>(field, getType(), getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current minimum of the data
	 * stream at the given position by the given key. An independent aggregate
	 * is kept per key.
	 * 
	 * @param positionToMin
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> min(int positionToMin) {
		return aggregate(new ComparableAggregator<T>(positionToMin, getType(), AggregationType.MIN,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current minimum of the pojo
	 * data stream at the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> min(String field) {
		return aggregate(new ComparableAggregator<T>(field, getType(), AggregationType.MIN,
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current maximum of the data stream
	 * at the given position by the given key. An independent aggregate is kept
	 * per key.
	 * 
	 * @param positionToMax
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> max(int positionToMax) {
		return aggregate(new ComparableAggregator<T>(positionToMax, getType(), AggregationType.MAX,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current maximum of the pojo
	 * data stream at the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
	 * {@link DataStream}S underlying type. A dot can be used to drill down into
	 * objects, as in {@code "field1.getInnerField2()" }.
	 * 
	 * @param field
	 *            The field expression based on which the aggregation will be
	 *            applied.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> max(String field) {
		return aggregate(new ComparableAggregator<T>(field, getType(), AggregationType.MAX,
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current minimum element of the
	 * pojo data stream by the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
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
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public SingleOutputStreamOperator<T, ?> minBy(String field, boolean first) {
		return aggregate(new ComparableAggregator(field, getType(), AggregationType.MINBY,
				first, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current maximum element of the
	 * pojo data stream by the given field expression by the given key. An
	 * independent aggregate is kept per key. A field expression is either the
	 * name of a public field or a getter method with parentheses of the
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
	public SingleOutputStreamOperator<T, ?> maxBy(String field, boolean first) {
		return aggregate(new ComparableAggregator<T>(field, getType(), AggregationType.MAXBY,
				first, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns the first one by default.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> minBy(int positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns the first one by default.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> minBy(String positionToMinBy) {
		return this.minBy(positionToMinBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the minimum value at the
	 * given position, the operator returns either the first or last one,
	 * depending on the parameter set.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            minimal value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> minBy(int positionToMinBy, boolean first) {
		return aggregate(new ComparableAggregator<T>(positionToMinBy, getType(), AggregationType.MINBY, first,
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns the first one by default.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> maxBy(int positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns the first one by default.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> maxBy(String positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position by the given key. An independent
	 * aggregate is kept per key. If more elements have the maximum value at the
	 * given position, the operator returns either the first or last one,
	 * depending on the parameter set.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize.
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            maximum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<T, ?> maxBy(int positionToMaxBy, boolean first) {
		return aggregate(new ComparableAggregator<T>(positionToMaxBy, getType(), AggregationType.MAXBY, first,
				getExecutionConfig()));
	}

	protected SingleOutputStreamOperator<T, ?> aggregate(AggregationFunction<T> aggregate) {
		StreamGroupedReduce<T> operator = new StreamGroupedReduce<T>(clean(aggregate), keySelector);
		return transform("Grouped Aggregation", getType(), operator);
	}
}
