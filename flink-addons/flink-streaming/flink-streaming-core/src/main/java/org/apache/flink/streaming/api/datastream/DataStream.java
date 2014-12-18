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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.function.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.function.aggregation.SumAggregator;
import org.apache.flink.streaming.api.function.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.sink.WriteFormatAsCsv;
import org.apache.flink.streaming.api.function.sink.WriteFormatAsText;
import org.apache.flink.streaming.api.function.sink.WriteSinkFunctionByBatches;
import org.apache.flink.streaming.api.function.sink.WriteSinkFunctionByMillis;
import org.apache.flink.streaming.api.invokable.SinkInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.invokable.operator.CounterInvokable;
import org.apache.flink.streaming.api.invokable.operator.FilterInvokable;
import org.apache.flink.streaming.api.invokable.operator.FlatMapInvokable;
import org.apache.flink.streaming.api.invokable.operator.MapInvokable;
import org.apache.flink.streaming.api.invokable.operator.StreamReduceInvokable;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Delta;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.partitioner.DistributePartitioner;
import org.apache.flink.streaming.partitioner.FieldsPartitioner;
import org.apache.flink.streaming.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.keys.FieldsKeySelector;
import org.apache.flink.streaming.util.keys.PojoKeySelector;

/**
 * A DataStream represents a stream of elements of the same type. A DataStream
 * can be transformed into another DataStream by applying a transformation as
 * for example
 * <ul>
 * <li>{@link DataStream#map},</li>
 * <li>{@link DataStream#filter}, or</li>
 * <li>{@link DataStream#aggregate}.</li>
 * </ul>
 * 
 * @param <OUT>
 *            The type of the DataStream, i.e., the type of the elements of the
 *            DataStream.
 */
public class DataStream<OUT> {

	protected static Integer counter = 0;
	protected final StreamExecutionEnvironment environment;
	protected final String id;
	protected int degreeOfParallelism;
	protected List<String> userDefinedNames;
	protected boolean selectAll;
	protected StreamPartitioner<OUT> partitioner;
	protected final TypeInformation<OUT> typeInfo;
	protected List<DataStream<OUT>> mergedStreams;

	protected final JobGraphBuilder jobGraphBuilder;

	/**
	 * Create a new {@link DataStream} in the given execution environment with
	 * partitioning set to forward by default.
	 * 
	 * @param environment
	 *            StreamExecutionEnvironment
	 * @param operatorType
	 *            The type of the operator in the component
	 * @param typeInfo
	 *            Type of the datastream
	 */
	public DataStream(StreamExecutionEnvironment environment, String operatorType,
			TypeInformation<OUT> typeInfo) {
		if (environment == null) {
			throw new NullPointerException("context is null");
		}

		counter++;
		this.id = operatorType + "-" + counter.toString();
		this.environment = environment;
		this.degreeOfParallelism = environment.getDegreeOfParallelism();
		this.jobGraphBuilder = environment.getJobGraphBuilder();
		this.userDefinedNames = new ArrayList<String>();
		this.selectAll = false;
		this.partitioner = new DistributePartitioner<OUT>(true);
		this.typeInfo = typeInfo;
		this.mergedStreams = new ArrayList<DataStream<OUT>>();
		this.mergedStreams.add(this);
	}

	/**
	 * Create a new DataStream by creating a copy of another DataStream
	 * 
	 * @param dataStream
	 *            The DataStream that will be copied.
	 */
	public DataStream(DataStream<OUT> dataStream) {
		this.environment = dataStream.environment;
		this.id = dataStream.id;
		this.degreeOfParallelism = dataStream.degreeOfParallelism;
		this.userDefinedNames = new ArrayList<String>(dataStream.userDefinedNames);
		this.selectAll = dataStream.selectAll;
		this.partitioner = dataStream.partitioner;
		this.jobGraphBuilder = dataStream.jobGraphBuilder;
		this.typeInfo = dataStream.typeInfo;
		this.mergedStreams = new ArrayList<DataStream<OUT>>();
		this.mergedStreams.add(this);
		if (dataStream.mergedStreams.size() > 1) {
			for (int i = 1; i < dataStream.mergedStreams.size(); i++) {
				this.mergedStreams.add(new DataStream<OUT>(dataStream.mergedStreams.get(i)));
			}
		}

	}

	/**
	 * Returns the ID of the {@link DataStream}.
	 * 
	 * @return ID of the DataStream
	 */
	public String getId() {
		return id;
	}

	/**
	 * Gets the degree of parallelism for this operator.
	 * 
	 * @return The parallelism set for this operator.
	 */
	public int getParallelism() {
		return this.degreeOfParallelism;
	}

	/**
	 * Gets the type of the stream.
	 * 
	 * @return The type of the datastream.
	 */
	public TypeInformation<OUT> getType() {
		return this.typeInfo;
	}

	public <F> F clean(F f) {
		if (getExecutionEnvironment().getConfig().isClosureCleanerEnabled()) {
			ClosureCleaner.clean(f, true);
		}
		ClosureCleaner.ensureSerializable(f);
		return f;
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	/**
	 * Creates a new {@link DataStream} by merging {@link DataStream} outputs of
	 * the same type with each other. The DataStreams merged using this operator
	 * will be transformed simultaneously.
	 * 
	 * @param streams
	 *            The DataStreams to merge output with.
	 * @return The {@link DataStream}.
	 */
	public DataStream<OUT> merge(DataStream<OUT>... streams) {
		DataStream<OUT> returnStream = this.copy();

		for (DataStream<OUT> stream : streams) {
			for (DataStream<OUT> ds : stream.mergedStreams) {
				validateMerge(ds.getId());
				returnStream.mergedStreams.add(ds.copy());
			}
		}
		return returnStream;
	}

	/**
	 * Creates a new {@link ConnectedDataStream} by connecting
	 * {@link DataStream} outputs of different type with each other. The
	 * DataStreams connected using this operators can be used with CoFunctions.
	 * 
	 * @param dataStream
	 *            The DataStream with which this stream will be joined.
	 * @return The {@link ConnectedDataStream}.
	 */
	public <R> ConnectedDataStream<OUT, R> connect(DataStream<R> dataStream) {
		return new ConnectedDataStream<OUT, R>(this, dataStream);
	}

	/**
	 * Groups the elements of a {@link DataStream} by the given key positions to
	 * be used with grouped operators like
	 * {@link GroupedDataStream#reduce(ReduceFunction)}
	 * 
	 * @param fields
	 *            The position of the fields on which the {@link DataStream}
	 *            will be grouped.
	 * @return The grouped {@link DataStream}
	 */
	public GroupedDataStream<OUT> groupBy(int... fields) {

		return groupBy(FieldsKeySelector.getSelector(getType(), fields));

	}

	/**
	 * Groups a {@link DataStream} using field expressions. A field expression
	 * is either the name of a public field or a getter method with parentheses
	 * of the {@link DataStream}S underlying type. A dot can be used to drill
	 * down into objects, as in {@code "field1.getInnerField2()" }. This method
	 * returns an {@link GroupedDataStream}.
	 * 
	 * @param fields
	 *            One or more field expressions on which the DataStream will be
	 *            grouped.
	 * @return The grouped {@link DataStream}
	 **/
	public GroupedDataStream<OUT> groupBy(String... fields) {

		return groupBy(new PojoKeySelector<OUT>(getType(), fields));

	}

	/**
	 * Groups the elements of a {@link DataStream} by the key extracted by the
	 * {@link KeySelector} to be used with grouped operators like
	 * {@link GroupedDataStream#reduce(ReduceFunction)}
	 * 
	 * @param keySelector
	 *            The {@link KeySelector} that will be used to extract keys for
	 *            the values
	 * @return The grouped {@link DataStream}
	 */
	public GroupedDataStream<OUT> groupBy(KeySelector<OUT, ?> keySelector) {
		return new GroupedDataStream<OUT>(this, clean(keySelector));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output is
	 * partitioned by the selected fields.
	 * 
	 * @param fields
	 *            The fields to partition by.
	 * @return The DataStream with fields partitioning set.
	 */
	public DataStream<OUT> partitionBy(int... fields) {

		return setConnectionType(new FieldsPartitioner<OUT>(FieldsKeySelector.getSelector(
				getType(), fields)));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output is
	 * partitioned by the given field expressions.
	 * 
	 * @param fields
	 *            The fields expressions to partition by.
	 * @return The DataStream with fields partitioning set.
	 */
	public DataStream<OUT> partitionBy(String... fields) {

		return setConnectionType(new FieldsPartitioner<OUT>(new PojoKeySelector<OUT>(getType(),
				fields)));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output is
	 * partitioned using the given {@link KeySelector}.
	 * 
	 * @param keySelector
	 * @return
	 */
	public DataStream<OUT> partitionBy(KeySelector<OUT, ?> keySelector) {
		return setConnectionType(new FieldsPartitioner<OUT>(clean(keySelector)));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are broadcasted to every parallel instance of the next component.
	 * 
	 * @return The DataStream with broadcast partitioning set.
	 */
	public DataStream<OUT> broadcast() {
		return setConnectionType(new BroadcastPartitioner<OUT>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are shuffled to the next component.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> shuffle() {
		return setConnectionType(new ShufflePartitioner<OUT>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are forwarded to the local subtask of the next component. This is the
	 * default partitioner setting.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> forward() {
		return setConnectionType(new DistributePartitioner<OUT>(true));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are distributed evenly to the next component.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> distribute() {
		return setConnectionType(new DistributePartitioner<OUT>(false));
	}

	/**
	 * Initiates an iterative part of the program that feeds back data streams.
	 * The iterative part needs to be closed by calling
	 * {@link IterativeDataStream#closeWith(DataStream)}. The transformation of
	 * this IterativeDataStream will be the iteration head. The data stream
	 * given to the {@link IterativeDataStream#closeWith(DataStream)} method is
	 * the data stream that will be fed back and used as the input for the
	 * iteration head. A common usage pattern for streaming iterations is to use
	 * output splitting to send a part of the closing data stream to the head.
	 * Refer to {@link SingleOutputStreamOperator#split(OutputSelector)} for
	 * more information.
	 * <p>
	 * The iteration edge will be partitioned the same way as the first input of
	 * the iteration head.
	 * <p>
	 * By default a DataStream with iteration will never terminate, but the user
	 * can use the {@link IterativeDataStream#setMaxWaitTime} call to set a max
	 * waiting time for the iteration head. If no data received in the set time,
	 * the stream terminates.
	 * 
	 * @return The iterative data stream created.
	 */
	public IterativeDataStream<OUT> iterate() {
		return new IterativeDataStream<OUT>(this);
	}

	/**
	 * Applies a Map transformation on a {@link DataStream}. The transformation
	 * calls a {@link MapFunction} for each element of the DataStream. Each
	 * MapFunction call returns exactly one element. The user can also extend
	 * {@link RichMapFunction} to gain access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param mapper
	 *            The MapFunction that is called for each element of the
	 *            DataStream.
	 * @param <R>
	 *            output type
	 * @return The transformed {@link DataStream}.
	 */
	public <R> SingleOutputStreamOperator<R, ?> map(MapFunction<OUT, R> mapper) {

		TypeInformation<R> outType = TypeExtractor.getMapReturnTypes(clean(mapper), getType());

		return addFunction("map", clean(mapper), getType(), outType, new MapInvokable<OUT, R>(
				clean(mapper)));
	}

	/**
	 * Applies a FlatMap transformation on a {@link DataStream}. The
	 * transformation calls a {@link FlatMapFunction} for each element of the
	 * DataStream. Each FlatMapFunction call can return any number of elements
	 * including none. The user can also extend {@link RichFlatMapFunction} to
	 * gain access to other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param flatMapper
	 *            The FlatMapFunction that is called for each element of the
	 *            DataStream
	 * 
	 * @param <R>
	 *            output type
	 * @return The transformed {@link DataStream}.
	 */
	public <R> SingleOutputStreamOperator<R, ?> flatMap(FlatMapFunction<OUT, R> flatMapper) {

		TypeInformation<R> outType = TypeExtractor.getFlatMapReturnTypes(clean(flatMapper), getType());

		return addFunction("flatMap", clean(flatMapper), getType(), outType,
				new FlatMapInvokable<OUT, R>(clean(flatMapper)));
	}

	/**
	 * Applies a reduce transformation on the data stream. The user can also
	 * extend the {@link RichReduceFunction} to gain access to other features
	 * provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reducer) {

		return addFunction("reduce", clean(reducer), getType(), getType(),
				new StreamReduceInvokable<OUT>(clean(reducer)));
	}

	/**
	 * Applies a Filter transformation on a {@link DataStream}. The
	 * transformation calls a {@link FilterFunction} for each element of the
	 * DataStream and retains only those element for which the function returns
	 * true. Elements for which the function returns false are filtered. The
	 * user can also extend {@link RichFilterFunction} to gain access to other
	 * features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param filter
	 *            The FilterFunction that is called for each element of the
	 *            DataSet.
	 * @return The filtered DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> filter(FilterFunction<OUT> filter) {
		return addFunction("filter", clean(filter), getType(), getType(), new FilterInvokable<OUT>(clean(
				filter)));
	}

	/**
	 * Initiates a Project transformation on a {@link Tuple} {@link DataStream}.<br/>
	 * <b>Note: Only Tuple DataStreams can be projected.</b></br> The
	 * transformation projects each Tuple of the DataSet onto a (sub)set of
	 * fields.</br> This method returns a {@link StreamProjection} on which
	 * {@link StreamProjection#types(Class)} needs to be called to completed the
	 * transformation.
	 * 
	 * @param fieldIndexes
	 *            The field indexes of the input tuples that are retained. The
	 *            order of fields in the output tuple corresponds to the order
	 *            of field indexes.
	 * @return A StreamProjection that needs to be converted into a DataStream
	 *         to complete the project transformation by calling
	 *         {@link StreamProjection#types(Class)}.
	 * 
	 * @see Tuple
	 * @see DataStream
	 */
	public StreamProjection<OUT> project(int... fieldIndexes) {
		return new StreamProjection<OUT>(this.copy(), fieldIndexes);
	}

	/**
	 * Initiates a temporal Cross transformation.<br/>
	 * A Cross transformation combines the elements of two {@link DataStream}s
	 * into one DataStream over a specified time window. It builds all pair
	 * combinations of elements of both DataStreams, i.e., it builds a Cartesian
	 * product.
	 * 
	 * <p>
	 * This method returns a {@link StreamCrossOperator} on which the
	 * {@link StreamCrossOperator#onWindow} should be called to define the
	 * window, and then call
	 * {@link StreamCrossOperator.CrossWindow#with(org.apache.flink.api.common.functions.CrossFunction)}
	 * to define a {@link org.apache.flink.api.common.functions.CrossFunction}
	 * which is called for each pair of crossed elements. The CrossFunction
	 * returns a exactly one element for each pair of input elements.
	 * 
	 * @param dataStreamToCross
	 *            The other DataStream with which this DataStream is crossed.
	 * @return A {@link StreamCrossOperator} to continue the definition of the
	 *         Join transformation.
	 * 
	 * @see org.apache.flink.api.common.functions.CrossFunction
	 * @see DataStream
	 */
	public <IN2> StreamCrossOperator<OUT, IN2> cross(DataStream<IN2> dataStreamToCross) {
		return new StreamCrossOperator<OUT, IN2>(this, dataStreamToCross);
	}

	/**
	 * Initiates a temporal Join transformation. <br/>
	 * A temporal Join transformation joins the elements of two
	 * {@link DataStream}s on key equality over a specified time window.</br>
	 * 
	 * This method returns a {@link StreamJoinOperator} on which the
	 * {@link StreamJoinOperator#onWindow} should be called to define the
	 * window, and then the {@link StreamJoinOperator.JoinWindow#where} and
	 * {@link StreamJoinOperator.JoinPredicate#equalTo} can be used to define
	 * the join keys.
	 * 
	 * @param other
	 *            The other DataStream with which this DataStream is joined.
	 * @return A {@link StreamJoinOperator} to continue the definition of the
	 *         Join transformation.
	 * 
	 */
	public <IN2> StreamJoinOperator<OUT, IN2> join(DataStream<IN2> dataStreamToJoin) {
		return new StreamJoinOperator<OUT, IN2>(this, dataStreamToJoin);
	}

	/**
	 * Applies an aggregation that sums the data stream at the given position.
	 * 
	 * @param positionToSum
	 *            The position in the data point to sum
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> sum(int positionToSum) {
		checkFieldRange(positionToSum);
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(positionToSum,
				getClassAtPos(positionToSum), getType()));
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
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(field, getType()));
	}

	/**
	 * Applies an aggregation that that gives the minimum of the data stream at
	 * the given position.
	 * 
	 * @param positionToMin
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> min(int positionToMin) {
		checkFieldRange(positionToMin);
		return aggregate(ComparableAggregator.getAggregator(positionToMin, getType(),
				AggregationType.MIN));
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
		return aggregate(ComparableAggregator.getAggregator(field, getType(), AggregationType.MIN,
				false));
	}

	/**
	 * Applies an aggregation that gives the maximum of the data stream at the
	 * given position.
	 * 
	 * @param positionToMax
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> max(int positionToMax) {
		checkFieldRange(positionToMax);
		return aggregate(ComparableAggregator.getAggregator(positionToMax, getType(),
				AggregationType.MAX));
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
		return aggregate(ComparableAggregator.getAggregator(field, getType(), AggregationType.MAX,
				false));
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
		return aggregate(ComparableAggregator.getAggregator(field, getType(),
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
		return aggregate(ComparableAggregator.getAggregator(field, getType(),
				AggregationType.MAXBY, first));
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position, if more elements have the minimum
	 * value at the given position, the operator returns the first one by
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
	 * Applies an aggregation that that gives the current element with the
	 * minimum value at the given position, if more elements have the minimum
	 * value at the given position, the operator returns either the first or
	 * last one, depending on the parameter set.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            minimal value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> minBy(int positionToMinBy, boolean first) {
		checkFieldRange(positionToMinBy);
		return aggregate(ComparableAggregator.getAggregator(positionToMinBy, getType(),
				AggregationType.MINBY, first));
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position, if more elements have the maximum
	 * value at the given position, the operator returns the first one by
	 * default.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> maxBy(int positionToMaxBy) {
		return this.maxBy(positionToMaxBy, true);
	}

	/**
	 * Applies an aggregation that that gives the current element with the
	 * maximum value at the given position, if more elements have the maximum
	 * value at the given position, the operator returns either the first or
	 * last one, depending on the parameter set.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize.
	 * @param first
	 *            If true, then the operator return the first element with the
	 *            maximum value, otherwise returns the last
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> maxBy(int positionToMaxBy, boolean first) {
		checkFieldRange(positionToMaxBy);
		return aggregate(ComparableAggregator.getAggregator(positionToMaxBy, getType(),
				AggregationType.MAXBY, first));
	}

	/**
	 * Applies an aggregation that gives the count of the values.
	 * 
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<Long, ?> count() {
		TypeInformation<Long> outTypeInfo = TypeExtractor.getForObject(Long.valueOf(0));

		return addFunction("counter", null, getType(), outTypeInfo, new CounterInvokable<OUT>());
	}

	/**
	 * Create a {@link WindowedDataStream} that can be used to apply
	 * transformation like {@link WindowedDataStream#reduce} or aggregations on
	 * preset chunks(windows) of the data stream. To define the windows one or
	 * more {@link WindowingHelper} such as {@link Time}, {@link Count} and
	 * {@link Delta} can be used.</br></br> When applied to a grouped data
	 * stream, the windows (evictions) and slide sizes (triggers) will be
	 * computed on a per group basis. </br></br> For more advanced control over
	 * the trigger and eviction policies please refer to
	 * {@link #window(triggers, evicters)} </br> </br> For example to create a
	 * sum every 5 seconds in a tumbling fashion:</br>
	 * {@code ds.window(Time.of(5, TimeUnit.SECONDS)).sum(field)} </br></br> To
	 * create sliding windows use the
	 * {@link WindowedDataStream#every(WindowingHelper...)} </br></br> The same
	 * example with 3 second slides:</br>
	 * 
	 * {@code ds.window(Time.of(5, TimeUnit.SECONDS)).every(Time.of(3,
	 *       TimeUnit.SECONDS)).sum(field)}
	 * 
	 * @param policyHelpers
	 *            Any {@link WindowingHelper} such as {@link Time},
	 *            {@link Count} and {@link Delta} to define the window.
	 * @return A {@link WindowedDataStream} providing further operations.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public WindowedDataStream<OUT> window(WindowingHelper... policyHelpers) {
		return new WindowedDataStream<OUT>(this, policyHelpers);
	}

	/**
	 * Create a {@link WindowedDataStream} using the given {@link TriggerPolicy}
	 * s and {@link EvictionPolicy}s. Windowing can be used to apply
	 * transformation like {@link WindowedDataStream#reduce} or aggregations on
	 * preset chunks(windows) of the data stream.</br></br>For most common
	 * use-cases please refer to {@link #window(WindowingHelper...)}
	 * 
	 * @param triggers
	 *            The list of {@link TriggerPolicy}s that will determine how
	 *            often the user function is called on the window.
	 * @param evicters
	 *            The list of {@link EvictionPolicy}s that will determine the
	 *            number of elements in each time window.
	 * @return A {@link WindowedDataStream} providing further operations.
	 */
	public WindowedDataStream<OUT> window(List<TriggerPolicy<OUT>> triggers,
			List<EvictionPolicy<OUT>> evicters) {
		return new WindowedDataStream<OUT>(this, triggers, evicters);
	}

	/**
	 * Writes a DataStream to the standard output stream (stdout).<br>
	 * For each element of the DataStream the result of
	 * {@link Object#toString()} is written.
	 * 
	 * @return The closed DataStream.
	 */
	public DataStreamSink<OUT> print() {
		DataStream<OUT> inputStream = this.copy();
		PrintSinkFunction<OUT> printFunction = new PrintSinkFunction<OUT>();
		DataStreamSink<OUT> returnStream = addSink(inputStream, printFunction, getType());

		return returnStream;
	}

	/**
	 * Writes a DataStream to the standard output stream (stderr).<br>
	 * For each element of the DataStream the result of
	 * {@link Object#toString()} is written.
	 * 
	 * @return The closed DataStream.
	 */
	public DataStreamSink<OUT> printToErr() {
		DataStream<OUT> inputStream = this.copy();
		PrintSinkFunction<OUT> printFunction = new PrintSinkFunction<OUT>(true);
		DataStreamSink<OUT> returnStream = addSink(inputStream, printFunction, getType());

		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), 1, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, long millis) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), millis, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, int batchSize) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), batchSize, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            system time.
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, long millis, OUT endTuple) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), millis, endTuple);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            batchSize.
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, int batchSize, OUT endTuple) {
		return writeAsText(this, path, new WriteFormatAsText<OUT>(), batchSize, endTuple);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            system time.
	 * 
	 * @return the data stream constructed
	 */
	private DataStreamSink<OUT> writeAsText(DataStream<OUT> inputStream, String path,
			WriteFormatAsText<OUT> format, long millis, OUT endTuple) {
		DataStreamSink<OUT> returnStream = addSink(inputStream, new WriteSinkFunctionByMillis<OUT>(
				path, format, millis, endTuple), inputStream.typeInfo);
		jobGraphBuilder.setMutability(returnStream.getId(), false);
		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            batchSize.
	 * 
	 * @return the data stream constructed
	 */
	private DataStreamSink<OUT> writeAsText(DataStream<OUT> inputStream, String path,
			WriteFormatAsText<OUT> format, int batchSize, OUT endTuple) {
		DataStreamSink<OUT> returnStream = addSink(inputStream,
				new WriteSinkFunctionByBatches<OUT>(path, format, batchSize, endTuple),
				inputStream.typeInfo);
		jobGraphBuilder.setMutability(returnStream.getId(), false);
		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path) {
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), 1, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path, long millis) {
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), millis, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path, int batchSize) {
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), batchSize, null);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            system time.
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path, long millis, OUT endTuple) {
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), millis, endTuple);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            batchSize.
	 * 
	 * @return The closed DataStream
	 */
	public DataStreamSink<OUT> writeAsCsv(String path, int batchSize, OUT endTuple) {
		if (this instanceof SingleOutputStreamOperator) {
			((SingleOutputStreamOperator<?, ?>) this).setMutability(false);
		}
		return writeAsCsv(this, path, new WriteFormatAsCsv<OUT>(), batchSize, endTuple);
	}

	/**
	 * Writes a DataStream to the file specified by path in csv format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param millis
	 *            is the file update frequency
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            system time.
	 * 
	 * @return the data stream constructed
	 */
	private DataStreamSink<OUT> writeAsCsv(DataStream<OUT> inputStream, String path,
			WriteFormatAsCsv<OUT> format, long millis, OUT endTuple) {
		DataStreamSink<OUT> returnStream = addSink(inputStream, new WriteSinkFunctionByMillis<OUT>(
				path, format, millis, endTuple), inputStream.typeInfo);
		jobGraphBuilder.setMutability(returnStream.getId(), false);
		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in csv format. The
	 * writing is performed periodically in equally sized batches. For every
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @param path
	 *            is the path to the location where the tuples are written
	 * @param batchSize
	 *            is the size of the batches, i.e. the number of tuples written
	 *            to the file at a time
	 * @param endTuple
	 *            is a special tuple indicating the end of the stream. If an
	 *            endTuple is caught, the last pending batch of tuples will be
	 *            immediately appended to the target file regardless of the
	 *            batchSize.
	 * 
	 * @return the data stream constructed
	 */
	private DataStreamSink<OUT> writeAsCsv(DataStream<OUT> inputStream, String path,
			WriteFormatAsCsv<OUT> format, int batchSize, OUT endTuple) {
		DataStreamSink<OUT> returnStream = addSink(inputStream,
				new WriteSinkFunctionByBatches<OUT>(path, format, batchSize, endTuple),
				inputStream.typeInfo);
		jobGraphBuilder.setMutability(returnStream.getId(), false);
		return returnStream;
	}

	protected SingleOutputStreamOperator<OUT, ?> aggregate(AggregationFunction<OUT> aggregate) {

		StreamReduceInvokable<OUT> invokable = new StreamReduceInvokable<OUT>(aggregate);

		SingleOutputStreamOperator<OUT, ?> returnStream = addFunction("reduce", clean(aggregate),
				typeInfo, typeInfo, invokable);

		return returnStream;
	}

	protected <R> DataStream<OUT> addIterationSource(Integer iterationID, long waitTime) {

		DataStream<R> returnStream = new DataStreamSource<R>(environment, "iterationSource", null);

		jobGraphBuilder.addIterationHead(returnStream.getId(), this.getId(), iterationID,
				degreeOfParallelism, waitTime);

		return this.copy();
	}

	/**
	 * Internal function for passing the user defined functions to the JobGraph
	 * of the job.
	 * 
	 * @param functionName
	 *            name of the function
	 * @param function
	 *            the user defined function
	 * @param functionInvokable
	 *            the wrapping JobVertex instance
	 * @param <R>
	 *            type of the return stream
	 * @return the data stream constructed
	 */
	protected <R> SingleOutputStreamOperator<R, ?> addFunction(String functionName,
			final Function function, TypeInformation<OUT> inTypeInfo,
			TypeInformation<R> outTypeInfo, StreamInvokable<OUT, R> functionInvokable) {
		DataStream<OUT> inputStream = this.copy();
		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<R, ?> returnStream = new SingleOutputStreamOperator(environment,
				functionName, outTypeInfo);

		try {
			jobGraphBuilder.addStreamVertex(returnStream.getId(), functionInvokable, inTypeInfo,
					outTypeInfo, functionName,
					SerializationUtils.serialize((Serializable) function), degreeOfParallelism);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize user defined function");
		}

		connectGraph(inputStream, returnStream.getId(), 0);

		if (inputStream instanceof IterativeDataStream) {
			IterativeDataStream<OUT> iterativeStream = (IterativeDataStream<OUT>) inputStream;
			returnStream.addIterationSource(iterativeStream.iterationID, iterativeStream.waitTime);
		}

		return returnStream;
	}

	/**
	 * Internal function for setting the partitioner for the DataStream
	 * 
	 * @param partitioner
	 *            Partitioner to set.
	 * @return The modified DataStream.
	 */
	protected DataStream<OUT> setConnectionType(StreamPartitioner<OUT> partitioner) {
		DataStream<OUT> returnStream = this.copy();

		for (DataStream<OUT> stream : returnStream.mergedStreams) {
			stream.partitioner = partitioner;
		}

		return returnStream;
	}

	/**
	 * Internal function for assembling the underlying
	 * {@link org.apache.flink.runtime.jobgraph.JobGraph} of the job. Connects
	 * the outputs of the given input stream to the specified output stream
	 * given by the outputID.
	 * 
	 * @param inputStream
	 *            input data stream
	 * @param outputID
	 *            ID of the output
	 * @param typeNumber
	 *            Number of the type (used at co-functions)
	 */
	protected <X> void connectGraph(DataStream<X> inputStream, String outputID, int typeNumber) {
		for (DataStream<X> stream : inputStream.mergedStreams) {
			jobGraphBuilder.setEdge(stream.getId(), outputID, stream.partitioner, typeNumber,
					inputStream.userDefinedNames, inputStream.selectAll);
		}

	}

	/**
	 * Adds the given sink to this DataStream. Only streams with sinks added
	 * will be executed once the {@link StreamExecutionEnvironment#execute()}
	 * method is called.
	 * 
	 * @param sinkFunction
	 *            The object containing the sink's invoke function.
	 * @return The closed DataStream.
	 */
	public DataStreamSink<OUT> addSink(SinkFunction<OUT> sinkFunction) {
		return addSink(this.copy(), sinkFunction);
	}

	private DataStreamSink<OUT> addSink(DataStream<OUT> inputStream, SinkFunction<OUT> sinkFunction) {
		return addSink(inputStream, sinkFunction, getType());
	}

	private DataStreamSink<OUT> addSink(DataStream<OUT> inputStream,
			SinkFunction<OUT> sinkFunction, TypeInformation<OUT> inTypeInfo) {
		DataStreamSink<OUT> returnStream = new DataStreamSink<OUT>(environment, "sink", typeInfo);

		try {
			jobGraphBuilder.addStreamVertex(returnStream.getId(), new SinkInvokable<OUT>(
					clean(sinkFunction)), inTypeInfo, null, "sink", SerializationUtils
					.serialize(clean(sinkFunction)), degreeOfParallelism);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize SinkFunction");
		}

		inputStream.connectGraph(inputStream.copy(), returnStream.getId(), 0);

		return returnStream;
	}

	/**
	 * Gets the class of the field at the given position
	 * 
	 * @param pos
	 *            Position of the field
	 * @return The class of the field
	 */
	@SuppressWarnings("rawtypes")
	protected Class<?> getClassAtPos(int pos) {
		Class<?> type;
		TypeInformation<OUT> outTypeInfo = getType();
		if (outTypeInfo.isTupleType()) {
			type = ((TupleTypeInfo) outTypeInfo).getTypeAt(pos).getTypeClass();

		} else if (outTypeInfo instanceof BasicArrayTypeInfo) {

			type = ((BasicArrayTypeInfo) outTypeInfo).getComponentTypeClass();

		} else if (outTypeInfo instanceof PrimitiveArrayTypeInfo) {
			Class<?> clazz = outTypeInfo.getTypeClass();
			if (clazz == boolean[].class) {
				type = Boolean.class;
			} else if (clazz == short[].class) {
				type = Short.class;
			} else if (clazz == int[].class) {
				type = Integer.class;
			} else if (clazz == long[].class) {
				type = Long.class;
			} else if (clazz == float[].class) {
				type = Float.class;
			} else if (clazz == double[].class) {
				type = Double.class;
			} else if (clazz == char[].class) {
				type = Character.class;
			} else {
				throw new IndexOutOfBoundsException("Type could not be determined for array");
			}

		} else if (pos == 0) {
			type = outTypeInfo.getTypeClass();
		} else {
			throw new IndexOutOfBoundsException("Position is out of range");
		}
		return type;
	}

	/**
	 * Checks if the given field position is allowed for the output type
	 * 
	 * @param pos
	 *            Position to check
	 */
	protected void checkFieldRange(int pos) {
		try {
			getClassAtPos(pos);
		} catch (IndexOutOfBoundsException e) {
			throw new RuntimeException("Selected field is out of range");

		}
	}

	private void validateMerge(String id) {
		for (DataStream<OUT> ds : this.mergedStreams) {
			if (ds.getId().equals(id)) {
				throw new RuntimeException("A DataStream cannot be merged with itself");
			}
		}
	}

	/**
	 * Creates a copy of the {@link DataStream}
	 * 
	 * @return The copy
	 */
	protected DataStream<OUT> copy() {
		return new DataStream<OUT>(this);
	}

}
