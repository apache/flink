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
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFoldFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.temporal.StreamCrossOperator;
import org.apache.flink.streaming.api.datastream.temporal.StreamJoinOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.functions.sink.FileSinkFunctionByMillis;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamCounter;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.StreamFold;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamReduce;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Delta;
import org.apache.flink.streaming.api.windowing.helper.FullStream;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.DistributePartitioner;
import org.apache.flink.streaming.runtime.partitioner.FieldsPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.google.common.base.Preconditions;

/**
 * A DataStream represents a stream of elements of the same type. A DataStream
 * can be transformed into another DataStream by applying a transformation as
 * for example
 * <ul>
 * <li>{@link DataStream#map},</li>
 * <li>{@link DataStream#filter}, or</li>
 * <li>{@link DataStream#sum}.</li>
 * </ul>
 * 
 * @param <OUT>
 *            The type of the DataStream, i.e., the type of the elements of the
 *            DataStream.
 */
public class DataStream<OUT> {

	protected static Integer counter = 0;
	protected final StreamExecutionEnvironment environment;
	protected final Integer id;
	protected int parallelism;
	protected List<String> userDefinedNames;
	protected StreamPartitioner<OUT> partitioner;
	@SuppressWarnings("rawtypes")
	protected TypeInformation typeInfo;
	protected List<DataStream<OUT>> mergedStreams;
	
	protected Integer iterationID = null;
	protected Long iterationWaitTime = null;

	protected final StreamGraph streamGraph;
	private boolean typeUsed;

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
		this.id = counter;
		this.environment = environment;
		this.parallelism = environment.getParallelism();
		this.streamGraph = environment.getStreamGraph();
		this.userDefinedNames = new ArrayList<String>();
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
		this.parallelism = dataStream.parallelism;
		this.userDefinedNames = new ArrayList<String>(dataStream.userDefinedNames);
		this.partitioner = dataStream.partitioner.copy();
		this.streamGraph = dataStream.streamGraph;
		this.typeInfo = dataStream.typeInfo;
		this.iterationID = dataStream.iterationID;
		this.iterationWaitTime = dataStream.iterationWaitTime;
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
	public Integer getId() {
		return id;
	}

	/**
	 * Gets the parallelism for this operator.
	 * 
	 * @return The parallelism set for this operator.
	 */
	public int getParallelism() {
		return this.parallelism;
	}

	/**
	 * Gets the type of the stream.
	 * 
	 * @return The type of the datastream.
	 */
	@SuppressWarnings("unchecked")
	public TypeInformation<OUT> getType() {
		if (typeInfo instanceof MissingTypeInfo) {
			MissingTypeInfo typeInfo = (MissingTypeInfo) this.typeInfo;
			throw new InvalidTypesException(
					"The return type of function '"
							+ typeInfo.getFunctionName()
							+ "' could not be determined automatically, due to type erasure. "
							+ "You can give type information hints by using the returns(...) method on the result of "
							+ "the transformation call, or by letting your function implement the 'ResultTypeQueryable' "
							+ "interface.", typeInfo.getTypeException());
		}
		typeUsed = true;
		return this.typeInfo;
	}

	/**
	 * Tries to fill in the type information. Type information can be filled in
	 * later when the program uses a type hint. This method checks whether the
	 * type information has ever been accessed before and does not allow
	 * modifications if the type was accessed already. This ensures consistency
	 * by making sure different parts of the operation do not assume different
	 * type information.
	 * 
	 * @param typeInfo
	 *            The type information to fill in.
	 * 
	 * @throws IllegalStateException
	 *             Thrown, if the type information has been accessed before.
	 */
	protected void fillInType(TypeInformation<OUT> typeInfo) {
		if (typeUsed) {
			throw new IllegalStateException(
					"TypeInformation cannot be filled in for the type after it has been used. "
							+ "Please make sure that the type info hints are the first call after the transformation function, "
							+ "before any access to types or semantic properties, etc.");
		}
		streamGraph.setOutType(id, typeInfo);
		this.typeInfo = typeInfo;
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

	protected ExecutionConfig getExecutionConfig() {
		return environment.getConfig();
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
	 * Operator used for directing tuples to specific named outputs using an
	 * {@link org.apache.flink.streaming.api.collector.selector.OutputSelector}.
	 * Calling this method on an operator creates a new {@link SplitDataStream}.
	 * 
	 * @param outputSelector
	 *            The user defined
	 *            {@link org.apache.flink.streaming.api.collector.selector.OutputSelector}
	 *            for directing the tuples.
	 * @return The {@link SplitDataStream}
	 */
	public SplitDataStream<OUT> split(OutputSelector<OUT> outputSelector) {
		for (DataStream<OUT> ds : this.mergedStreams) {
			streamGraph.addOutputSelector(ds.getId(), clean(outputSelector));
		}

		return new SplitDataStream<OUT>(this);
	}

	/**
	 * Creates a new {@link ConnectedDataStream} by connecting
	 * {@link DataStream} outputs of (possible) different types with each other.
	 * The DataStreams connected using this operator can be used with
	 * CoFunctions to apply joint transformations.
	 * 
	 * @param dataStream
	 *            The DataStream with which this stream will be connected.
	 * @return The {@link ConnectedDataStream}.
	 */
	public <R> ConnectedDataStream<OUT, R> connect(DataStream<R> dataStream) {
		return new ConnectedDataStream<OUT, R>(this, dataStream);
	}

	/**
	 * Groups the elements of a {@link DataStream} by the given key positions to
	 * be used with grouped operators like
	 * {@link GroupedDataStream#reduce(ReduceFunction)}</p> This operator also
	 * affects the partitioning of the stream, by forcing values with the same
	 * key to go to the same processing instance.
	 * 
	 * @param fields
	 *            The position of the fields on which the {@link DataStream}
	 *            will be grouped.
	 * @return The grouped {@link DataStream}
	 */
	public GroupedDataStream<OUT> groupBy(int... fields) {
		if (getType() instanceof BasicArrayTypeInfo || getType() instanceof PrimitiveArrayTypeInfo) {
			return groupBy(new KeySelectorUtil.ArrayKeySelector<OUT>(fields));
		} else {
			return groupBy(new Keys.ExpressionKeys<OUT>(fields, getType()));
		}
	}

	/**
	 * Groups a {@link DataStream} using field expressions. A field expression
	 * is either the name of a public field or a getter method with parentheses
	 * of the {@link DataStream}S underlying type. A dot can be used to drill
	 * down into objects, as in {@code "field1.getInnerField2()" }. This method
	 * returns an {@link GroupedDataStream}.</p> This operator also affects the
	 * partitioning of the stream, by forcing values with the same key to go to
	 * the same processing instance.
	 * 
	 * @param fields
	 *            One or more field expressions on which the DataStream will be
	 *            grouped.
	 * @return The grouped {@link DataStream}
	 **/
	public GroupedDataStream<OUT> groupBy(String... fields) {

		return groupBy(new Keys.ExpressionKeys<OUT>(fields, getType()));

	}

	/**
	 * Groups the elements of a {@link DataStream} by the key extracted by the
	 * {@link KeySelector} to be used with grouped operators like
	 * {@link GroupedDataStream#reduce(ReduceFunction)}.
	 * <p/>
	 * This operator also affects the partitioning of the stream, by forcing
	 * values with the same key to go to the same processing instance.
	 * 
	 * @param keySelector
	 *            The {@link KeySelector} that will be used to extract keys for
	 *            the values
	 * @return The grouped {@link DataStream}
	 */
	public GroupedDataStream<OUT> groupBy(KeySelector<OUT, ?> keySelector) {
		return new GroupedDataStream<OUT>(this, clean(keySelector));
	}

	private GroupedDataStream<OUT> groupBy(Keys<OUT> keys) {
		return new GroupedDataStream<OUT>(this, clean(KeySelectorUtil.getSelectorForKeys(keys,
				getType(), getExecutionConfig())));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output is
	 * partitioned using the given {@link KeySelector}. This setting only
	 * effects the how the outputs will be distributed between the parallel
	 * instances of the next processing operator.
	 * 
	 * @param keySelector
	 * @return
	 */
	protected DataStream<OUT> partitionBy(KeySelector<OUT, ?> keySelector) {
		return setConnectionType(new FieldsPartitioner<OUT>(clean(keySelector)));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are broadcasted to every parallel instance of the next component. This
	 * setting only effects the how the outputs will be distributed between the
	 * parallel instances of the next processing operator.
	 * 
	 * @return The DataStream with broadcast partitioning set.
	 */
	public DataStream<OUT> broadcast() {
		return setConnectionType(new BroadcastPartitioner<OUT>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are shuffled to the next component. This setting only effects the how the
	 * outputs will be distributed between the parallel instances of the next
	 * processing operator.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> shuffle() {
		return setConnectionType(new ShufflePartitioner<OUT>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are forwarded to the local subtask of the next component (whenever
	 * possible). This is the default partitioner setting. This setting only
	 * effects the how the outputs will be distributed between the parallel
	 * instances of the next processing operator.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> forward() {
		return setConnectionType(new DistributePartitioner<OUT>(true));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are distributed evenly to the next component.This setting only effects
	 * the how the outputs will be distributed between the parallel instances of
	 * the next processing operator.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> distribute() {
		return setConnectionType(new DistributePartitioner<OUT>(false));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output values
	 * all go to the first instance of the next processing operator. Use this
	 * setting with care since it might cause a serious performance bottleneck
	 * in the application.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<OUT> global() {
		return setConnectionType(new GlobalPartitioner<OUT>());
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
	 * Refer to {@link #split(OutputSelector)} for more information.
	 * <p>
	 * The iteration edge will be partitioned the same way as the first input of
	 * the iteration head.
	 * <p>
	 * By default a DataStream with iteration will never terminate, but the user
	 * can use the maxWaitTime parameter to set a max waiting time for the
	 * iteration head. If no data received in the set time, the stream
	 * terminates.
	 * 
	 * @return The iterative data stream created.
	 */
	public IterativeDataStream<OUT> iterate() {
		return new IterativeDataStream<OUT>(this, 0);
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
	 * Refer to {@link #split(OutputSelector)} for more information.
	 * <p>
	 * The iteration edge will be partitioned the same way as the first input of
	 * the iteration head.
	 * <p>
	 * By default a DataStream with iteration will never terminate, but the user
	 * can use the maxWaitTime parameter to set a max waiting time for the
	 * iteration head. If no data received in the set time, the stream
	 * terminates.
	 * 
	 * @param maxWaitTimeMillis
	 *            Number of milliseconds to wait between inputs before shutting
	 *            down
	 * 
	 * @return The iterative data stream created.
	 */
	public IterativeDataStream<OUT> iterate(long maxWaitTimeMillis) {
		return new IterativeDataStream<OUT>(this, maxWaitTimeMillis);
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

		TypeInformation<R> outType = TypeExtractor.getMapReturnTypes(clean(mapper), getType(),
				Utils.getCallLocationName(), true);

		return transform("Map", outType, new StreamMap<OUT, R>(clean(mapper)));
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

		TypeInformation<R> outType = TypeExtractor.getFlatMapReturnTypes(clean(flatMapper),
				getType(), Utils.getCallLocationName(), true);

		return transform("Flat Map", outType, new StreamFlatMap<OUT, R>(clean(flatMapper)));

	}

	/**
	 * Applies a reduce transformation on the data stream. The returned stream
	 * contains all the intermediate values of the reduce transformation. The
	 * user can also extend the {@link RichReduceFunction} to gain access to
	 * other features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface.
	 * 
	 * @param reducer
	 *            The {@link ReduceFunction} that will be called for every
	 *            element of the input values.
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> reduce(ReduceFunction<OUT> reducer) {

		return transform("Reduce", getType(), new StreamReduce<OUT>(clean(reducer)));

	}

	/**
	 * Applies a fold transformation on the data stream. The returned stream
	 * contains all the intermediate values of the fold transformation. The user
	 * can also extend the {@link RichFoldFunction} to gain access to other
	 * features provided by the
	 * {@link org.apache.flink.api.common.functions.RichFunction} interface
	 * 
	 * @param folder
	 *            The {@link FoldFunction} that will be called for every element
	 *            of the input values.
	 * @return The transformed DataStream
	 */
	public <R> SingleOutputStreamOperator<R, ?> fold(R initialValue, FoldFunction<OUT, R> folder) {
		TypeInformation<R> outType = TypeExtractor.getFoldReturnTypes(clean(folder), getType(),
				Utils.getCallLocationName(), false);

		return transform("Fold", outType, new StreamFold<OUT, R>(clean(folder), initialValue,
				outType));
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
		return transform("Filter", getType(), new StreamFilter<OUT>(clean(filter)));

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
	public <R extends Tuple> SingleOutputStreamOperator<R, ?> project(int... fieldIndexes) {
		return new StreamProjection<OUT>(this.copy(), fieldIndexes).projectTupleX();
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
	 * window.
	 * <p>
	 * Call {@link StreamCrossOperator.CrossWindow#with(crossFunction)} to
	 * define a custom cross function.
	 * 
	 * @param dataStreamToCross
	 *            The other DataStream with which this DataStream is crossed.
	 * @return A {@link StreamCrossOperator} to continue the definition of the
	 *         cross transformation.
	 * 
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
	 * the join keys.</p> The user can also use the
	 * {@link StreamJoinOperator.JoinedStream#with(joinFunction)} to apply
	 * custom join function.
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
	 * Applies an aggregation that that gives the current sum of the pojo data
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
	public SingleOutputStreamOperator<OUT, ?> sum(String field) {
		return aggregate((AggregationFunction<OUT>) SumAggregator.getSumFunction(field, getType(),
				getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current minimum of the data
	 * stream at the given position.
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
	 * Applies an aggregation that that gives the current minimum of the pojo
	 * data stream at the given field expression. A field expression is either
	 * the name of a public field or a getter method with parentheses of the
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
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that gives the current maximum of the data stream
	 * at the given position.
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
	 * Applies an aggregation that that gives the current maximum of the pojo
	 * data stream at the given field expression. A field expression is either
	 * the name of a public field or a getter method with parentheses of the
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
				false, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current minimum element of the
	 * pojo data stream by the given field expression. A field expression is
	 * either the name of a public field or a getter method with parentheses of
	 * the {@link DataStream}S underlying type. A dot can be used to drill down
	 * into objects, as in {@code "field1.getInnerField2()" }.
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
				AggregationType.MINBY, first, getExecutionConfig()));
	}

	/**
	 * Applies an aggregation that that gives the current maximum element of the
	 * pojo data stream by the given field expression. A field expression is
	 * either the name of a public field or a getter method with parentheses of
	 * the {@link DataStream}S underlying type. A dot can be used to drill down
	 * into objects, as in {@code "field1.getInnerField2()" }.
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
				AggregationType.MAXBY, first, getExecutionConfig()));
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
	 * value at the given position, the operator returns the first one by
	 * default.
	 * 
	 * @param positionToMinBy
	 *            The position in the data point to minimize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> minBy(String positionToMinBy) {
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
	 * value at the given position, the operator returns the first one by
	 * default.
	 * 
	 * @param positionToMaxBy
	 *            The position in the data point to maximize
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<OUT, ?> maxBy(String positionToMaxBy) {
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
	 * Creates a new DataStream containing the current number (count) of
	 * received records.
	 * 
	 * @return The transformed DataStream.
	 */
	public SingleOutputStreamOperator<Long, ?> count() {
		TypeInformation<Long> outTypeInfo = BasicTypeInfo.LONG_TYPE_INFO;

		return transform("Count", outTypeInfo, new StreamCounter<OUT>());
	}

	/**
	 * Create a {@link WindowedDataStream} that can be used to apply
	 * transformation like {@link WindowedDataStream#reduceWindow},
	 * {@link WindowedDataStream#mapWindow} or aggregations on preset
	 * chunks(windows) of the data stream. To define windows a
	 * {@link WindowingHelper} such as {@link Time}, {@link Count},
	 * {@link Delta} and {@link FullStream} can be used.</br></br> When applied
	 * to a grouped data stream, the windows (evictions) and slide sizes
	 * (triggers) will be computed on a per group basis. </br></br> For more
	 * advanced control over the trigger and eviction policies please refer to
	 * {@link #window(trigger, eviction)} </br> </br> For example to create a
	 * sum every 5 seconds in a tumbling fashion:</br>
	 * {@code ds.window(Time.of(5, TimeUnit.SECONDS)).sum(field)} </br></br> To
	 * create sliding windows use the
	 * {@link WindowedDataStream#every(WindowingHelper...)} </br></br> The same
	 * example with 3 second slides:</br>
	 * 
	 * {@code ds.window(Time.of(5, TimeUnit.SECONDS)).every(Time.of(3,
	 *       TimeUnit.SECONDS)).sum(field)}
	 * 
	 * @param policyHelper
	 *            Any {@link WindowingHelper} such as {@link Time},
	 *            {@link Count}, {@link Delta} {@link FullStream} to define the
	 *            window size.
	 * @return A {@link WindowedDataStream} providing further operations.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public WindowedDataStream<OUT> window(WindowingHelper policyHelper) {
		policyHelper.setExecutionConfig(getExecutionConfig());
		return new WindowedDataStream<OUT>(this, policyHelper);
	}

	/**
	 * Create a {@link WindowedDataStream} using the given {@link TriggerPolicy}
	 * and {@link EvictionPolicy}. Windowing can be used to apply transformation
	 * like {@link WindowedDataStream#reduceWindow},
	 * {@link WindowedDataStream#mapWindow} or aggregations on preset
	 * chunks(windows) of the data stream.</br></br>For most common use-cases
	 * please refer to {@link #window(WindowingHelper)}
	 * 
	 * @param trigger
	 *            The {@link TriggerPolicy} that will determine how often the
	 *            user function is called on the window.
	 * @param eviction
	 *            The {@link EvictionPolicy} that will determine the number of
	 *            elements in each time window.
	 * @return A {@link WindowedDataStream} providing further operations.
	 */
	public WindowedDataStream<OUT> window(TriggerPolicy<OUT> trigger, EvictionPolicy<OUT> eviction) {
		return new WindowedDataStream<OUT>(this, trigger, eviction);
	}

	/**
	 * Create a {@link WindowedDataStream} on the full stream history, to
	 * produce periodic aggregates.
	 * 
	 * @return A {@link WindowedDataStream} providing further operations.
	 */
	@SuppressWarnings("rawtypes")
	public WindowedDataStream<OUT> every(WindowingHelper policyHelper) {
		policyHelper.setExecutionConfig(getExecutionConfig());
		return window(FullStream.window()).every(policyHelper);
	}

	/**
	 * Writes a DataStream to the standard output stream (stdout).<br>
	 * For each element of the DataStream the result of
	 * {@link Object#toString()} is written.
	 * 
	 * @return The closed DataStream.
	 */
	public DataStreamSink<OUT> print() {
		PrintSinkFunction<OUT> printFunction = new PrintSinkFunction<OUT>();
		DataStreamSink<OUT> returnStream = addSink(printFunction);

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
		PrintSinkFunction<OUT> printFunction = new PrintSinkFunction<OUT>(true);
		DataStreamSink<OUT> returnStream = addSink(printFunction);

		return returnStream;
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * 
	 * @return the closed DataStream.
	 */
	public DataStreamSink<OUT> writeAsText(String path) {
		return writeToFile(new TextOutputFormat<OUT>(new Path(path)), 0L);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * @param millis
	 *            the file update frequency
	 * 
	 * @return the closed DataStream
	 */
	public DataStreamSink<OUT> writeAsText(String path, long millis) {
		TextOutputFormat<OUT> tof = new TextOutputFormat<OUT>(new Path(path));
		return writeToFile(tof, millis);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * @param writeMode
	 *            Control the behavior for existing files. Options are
	 *            NO_OVERWRITE and OVERWRITE.
	 * 
	 * @return the closed DataStream.
	 */
	public DataStreamSink<OUT> writeAsText(String path, WriteMode writeMode) {
		TextOutputFormat<OUT> tof = new TextOutputFormat<OUT>(new Path(path));
		tof.setWriteMode(writeMode);
		return writeToFile(tof, 0L);
	}

	/**
	 * Writes a DataStream to the file specified by path in text format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written.
	 * 
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * @param writeMode
	 *            Controls the behavior for existing files. Options are
	 *            NO_OVERWRITE and OVERWRITE.
	 * @param millis
	 *            the file update frequency
	 * 
	 * @return the closed DataStream.
	 */
	public DataStreamSink<OUT> writeAsText(String path, WriteMode writeMode, long millis) {
		TextOutputFormat<OUT> tof = new TextOutputFormat<OUT>(new Path(path));
		tof.setWriteMode(writeMode);
		return writeToFile(tof, millis);
	}

	/**
	 * Writes a DataStream to the file specified by path in csv format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written. This method can only be used on data streams of tuples.
	 * 
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * 
	 * @return the closed DataStream
	 */
	@SuppressWarnings("unchecked")
	public <X extends Tuple> DataStreamSink<OUT> writeAsCsv(String path) {
		Preconditions.checkArgument(getType().isTupleType(),
				"The writeAsCsv() method can only be used on data sets of tuples.");
		CsvOutputFormat<X> of = new CsvOutputFormat<X>(new Path(path),
				CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
		return writeToFile((OutputFormat<OUT>) of, 0L);
	}

	/**
	 * Writes a DataStream to the file specified by path in csv format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written. This method can only be used on data streams of tuples.
	 * 
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * @param millis
	 *            the file update frequency
	 * 
	 * @return the closed DataStream
	 */
	@SuppressWarnings("unchecked")
	public <X extends Tuple> DataStreamSink<OUT> writeAsCsv(String path, long millis) {
		Preconditions.checkArgument(getType().isTupleType(),
				"The writeAsCsv() method can only be used on data sets of tuples.");
		CsvOutputFormat<X> of = new CsvOutputFormat<X>(new Path(path),
				CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
		return writeToFile((OutputFormat<OUT>) of, millis);
	}

	/**
	 * Writes a DataStream to the file specified by path in csv format. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written. This method can only be used on data streams of tuples.
	 * 
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * @param writeMode
	 *            Controls the behavior for existing files. Options are
	 *            NO_OVERWRITE and OVERWRITE.
	 * 
	 * @return the closed DataStream
	 */
	@SuppressWarnings("unchecked")
	public <X extends Tuple> DataStreamSink<OUT> writeAsCsv(String path, WriteMode writeMode) {
		Preconditions.checkArgument(getType().isTupleType(),
				"The writeAsCsv() method can only be used on data sets of tuples.");
		CsvOutputFormat<X> of = new CsvOutputFormat<X>(new Path(path),
				CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
		if (writeMode != null) {
			of.setWriteMode(writeMode);
		}
		return writeToFile((OutputFormat<OUT>) of, 0L);
	}

	/**
	 * Writes a DataStream to the file specified by path in csv format. The
	 * writing is performed periodically, in every millis milliseconds. For
	 * every element of the DataStream the result of {@link Object#toString()}
	 * is written. This method can only be used on data streams of tuples.
	 * 
	 * @param path
	 *            the path pointing to the location the text file is written to
	 * @param writeMode
	 *            Controls the behavior for existing files. Options are
	 *            NO_OVERWRITE and OVERWRITE.
	 * @param millis
	 *            the file update frequency
	 * 
	 * @return the closed DataStream
	 */
	@SuppressWarnings("unchecked")
	public <X extends Tuple> DataStreamSink<OUT> writeAsCsv(String path, WriteMode writeMode,
			long millis) {
		Preconditions.checkArgument(getType().isTupleType(),
				"The writeAsCsv() method can only be used on data sets of tuples.");
		CsvOutputFormat<X> of = new CsvOutputFormat<X>(new Path(path),
				CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
		if (writeMode != null) {
			of.setWriteMode(writeMode);
		}
		return writeToFile((OutputFormat<OUT>) of, millis);
	}

	/**
	 * Writes the DataStream to a socket as a byte array. The format of the
	 * output is specified by a {@link SerializationSchema}.
	 * 
	 * @param hostName
	 *            host of the socket
	 * @param port
	 *            port of the socket
	 * @param schema
	 *            schema for serialization
	 * @return the closed DataStream
	 */
	public DataStreamSink<OUT> writeToSocket(String hostName, int port,
			SerializationSchema<OUT, byte[]> schema) {
		DataStreamSink<OUT> returnStream = addSink(new SocketClientSink<OUT>(hostName, port, schema));
		returnStream.setParallelism(1); // It would not work if multiple instances would connect to the same port
		return returnStream;
	}

	private DataStreamSink<OUT> writeToFile(OutputFormat<OUT> format, long millis) {
		DataStreamSink<OUT> returnStream = addSink(new FileSinkFunctionByMillis<OUT>(format, millis));
		return returnStream;
	}

	protected SingleOutputStreamOperator<OUT, ?> aggregate(AggregationFunction<OUT> aggregate) {

		StreamReduce<OUT> operator = new StreamReduce<OUT>(aggregate);

		SingleOutputStreamOperator<OUT, ?> returnStream = transform("Aggregation", getType(),
				operator);

		return returnStream;
	}

	/**
	 * Method for passing user defined operators along with the type
	 * informations that will transform the DataStream.
	 * 
	 * @param operatorName
	 *            name of the operator, for logging purposes
	 * @param outTypeInfo
	 *            the output type of the operator
	 * @param operator
	 *            the object containing the transformation logic
	 * @param <R>
	 *            type of the return stream
	 * @return the data stream constructed
	 */
	public <R> SingleOutputStreamOperator<R, ?> transform(String operatorName,
			TypeInformation<R> outTypeInfo, OneInputStreamOperator<OUT, R> operator) {
		DataStream<OUT> inputStream = this.copy();
		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<R, ?> returnStream = new SingleOutputStreamOperator(environment,
				operatorName, outTypeInfo, operator);

		streamGraph.addOperator(returnStream.getId(), operator, getType(), outTypeInfo,
				operatorName);

		connectGraph(inputStream, returnStream.getId(), 0);
		
		if (iterationID != null) {
			//This data stream is an input to some iteration
			addIterationSource(returnStream);
		}

		return returnStream;
	}
	
	private <X> void addIterationSource(DataStream<X> dataStream) {
		Integer id = ++counter;
		streamGraph.addIterationHead(id, dataStream.getId(), iterationID, iterationWaitTime);
		streamGraph.setParallelism(id, dataStream.getParallelism());
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
	protected <X> void connectGraph(DataStream<X> inputStream, Integer outputID, int typeNumber) {
		for (DataStream<X> stream : inputStream.mergedStreams) {
			streamGraph.addEdge(stream.getId(), outputID, stream.partitioner, typeNumber,
					inputStream.userDefinedNames);
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

		OneInputStreamOperator<OUT, Object> sinkOperator = new StreamSink<OUT>(clean(sinkFunction));

		DataStreamSink<OUT> returnStream = new DataStreamSink<OUT>(environment, "sink", getType(),
				sinkOperator);

		streamGraph.addOperator(returnStream.getId(), sinkOperator, getType(), null, "Stream Sink");

		this.connectGraph(this.copy(), returnStream.getId(), 0);

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

	private void validateMerge(Integer id) {
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
	public DataStream<OUT> copy() {
		return new DataStream<OUT>(this);
	}
}
