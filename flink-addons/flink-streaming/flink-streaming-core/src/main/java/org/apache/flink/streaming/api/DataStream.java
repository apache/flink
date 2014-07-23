/**
 *
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
 *
 */

package org.apache.flink.streaming.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.functions.FilterFunction;
import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.sink.WriteFormatAsCsv;
import org.apache.flink.streaming.api.function.sink.WriteFormatAsText;
import org.apache.flink.streaming.api.invokable.operator.BatchReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.FilterInvokable;
import org.apache.flink.streaming.api.invokable.operator.FlatMapInvokable;
import org.apache.flink.streaming.api.invokable.operator.MapInvokable;
import org.apache.flink.streaming.api.invokable.operator.WindowReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoMapInvokable;
import org.apache.flink.streaming.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.partitioner.DistributePartitioner;
import org.apache.flink.streaming.partitioner.FieldsPartitioner;
import org.apache.flink.streaming.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.partitioner.StreamPartitioner;

/**
 * A DataStream represents a stream of elements of the same type. A DataStream
 * can be transformed into another DataStream by applying a transformation as
 * for example
 * <ul>
 * <li>{@link DataStream#map},</li>
 * <li>{@link DataStream#filter}, or</li>
 * <li>{@link DataStream#batchReduce}.</li>
 * </ul>
 * 
 * @param <T>
 *            The type of the DataStream, i.e., the type of the elements of the
 *            DataStream.
 */
public class DataStream<T extends Tuple> {

	protected static Integer counter = 0;
	protected final StreamExecutionEnvironment environment;
	protected String id;
	protected int degreeOfParallelism;
	protected String userDefinedName;
	protected OutputSelector<T> outputSelector;
	protected List<String> connectIDs;
	protected List<StreamPartitioner<T>> partitioners;
	protected boolean iterationflag;
	protected Integer iterationID;

	/**
	 * Create a new {@link DataStream} in the given execution environment with
	 * partitioning set to shuffle by default.
	 * 
	 * @param environment
	 *            StreamExecutionEnvironment
	 * @param operatorType
	 *            The type of the operator in the component
	 */
	protected DataStream(StreamExecutionEnvironment environment, String operatorType) {
		if (environment == null) {
			throw new NullPointerException("context is null");
		}

		// TODO add name based on component number an preferable sequential id
		counter++;
		this.id = operatorType + "-" + counter.toString();
		this.environment = environment;
		this.degreeOfParallelism = environment.getDegreeOfParallelism();
		initConnections();

	}

	/**
	 * Create a new DataStream by creating a copy of another DataStream
	 * 
	 * @param dataStream
	 *            The DataStream that will be copied.
	 */
	protected DataStream(DataStream<T> dataStream) {
		this.environment = dataStream.environment;
		this.id = dataStream.id;
		this.degreeOfParallelism = dataStream.degreeOfParallelism;
		this.userDefinedName = dataStream.userDefinedName;
		this.outputSelector = dataStream.outputSelector;
		this.connectIDs = new ArrayList<String>(dataStream.connectIDs);
		this.partitioners = new ArrayList<StreamPartitioner<T>>(dataStream.partitioners);
		this.iterationflag = dataStream.iterationflag;
		this.iterationID = dataStream.iterationID;
	}

	/**
	 * Initialize the connection and partitioning among the connected
	 * {@link DataStream}s.
	 */
	private void initConnections() {
		connectIDs = new ArrayList<String>();
		connectIDs.add(getId());
		partitioners = new ArrayList<StreamPartitioner<T>>();
		partitioners.add(new ShufflePartitioner<T>());
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
	 * Sets the mutability of the operator represented by the DataStream. If the
	 * operator is set to mutable, the tuples received in the user defined
	 * functions, will be reused after the function call. Setting an operator to
	 * mutable greatly reduces garbage collection overhead and thus scalability.
	 * 
	 * @param isMutable
	 *            The mutability of the operator.
	 * @return The DataStream with mutability set.
	 */
	public DataStream<T> setMutability(boolean isMutable) {
		environment.setMutability(this, isMutable);
		return this;
	}

	/**
	 * Sets the degree of parallelism for this operator. The degree must be 1 or
	 * more.
	 * 
	 * @param dop
	 *            The degree of parallelism for this operator.
	 * @return The operator with set degree of parallelism.
	 */
	public DataStream<T> setParallelism(int dop) {
		if (dop < 1) {
			throw new IllegalArgumentException("The parallelism of an operator must be at least 1.");
		}
		this.degreeOfParallelism = dop;

		environment.setOperatorParallelism(this);

		return new DataStream<T>(this);

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
	 * Gives the data transformation(vertex) a user defined name in order to use
	 * at directed outputs. The {@link OutputSelector} of the input vertex
	 * should use this name for directed emits.
	 * 
	 * @param name
	 *            The name to set
	 * @return The named DataStream.
	 */
	public DataStream<T> name(String name) {
		// copy?
		if (name == "") {
			throw new IllegalArgumentException("User defined name must not be empty string");
		}

		userDefinedName = name;
		environment.setName(this, name);
		return this;
	}

	/**
	 * Connecting {@link DataStream} outputs with each other for applying joint
	 * operators on them. The DataStreams connected using this operator will be
	 * transformed simultaneously. It creates a joint output of the connected
	 * DataStreams.
	 * 
	 * @param streams
	 *            The DataStreams to connect output with.
	 * @return The connected DataStream.
	 */
	public DataStream<T> connectWith(DataStream<T>... streams) {
		DataStream<T> returnStream = new DataStream<T>(this);

		for (DataStream<T> stream : streams) {
			addConnection(returnStream, stream);
		}
		return returnStream;
	}

	/**
	 * Connects two DataStreams
	 * 
	 * @param returnStream
	 *            The other DataStream will connected to this
	 * @param stream
	 *            This DataStream will be connected to returnStream
	 * @return Connected DataStream
	 */
	private DataStream<T> addConnection(DataStream<T> returnStream, DataStream<T> stream) {
		returnStream.connectIDs.addAll(stream.connectIDs);
		returnStream.partitioners.addAll(stream.partitioners);

		return returnStream;
	}

	/**
	 * Operator used for directing tuples to specific named outputs. Sets an
	 * {@link OutputSelector} for the vertex. The tuples emitted from this
	 * vertex will be sent to the output names selected by the OutputSelector.
	 * Unnamed outputs will not receive any tuples.
	 * 
	 * @param outputSelector
	 *            The user defined OutputSelector for directing the tuples.
	 * @return The directed DataStream.
	 */
	public DataStream<T> directTo(OutputSelector<T> outputSelector) {
		this.outputSelector = outputSelector;
		environment.addDirectedEmit(id, outputSelector);
		return this;
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are partitioned by their hashcode and are sent to only one component.
	 * 
	 * @param keyposition
	 *            The field used to compute the hashcode.
	 * @return The DataStream with field partitioning set.
	 */
	public DataStream<T> partitionBy(int keyposition) {
		if (keyposition < 0) {
			throw new IllegalArgumentException("The position of the field must be non-negative");
		}

		return setConnectionType(new FieldsPartitioner<T>(keyposition));
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are broadcasted to every parallel instance of the next component.
	 * 
	 * @return The DataStream with broadcast partitioning set.
	 */
	public DataStream<T> broadcast() {
		return setConnectionType(new BroadcastPartitioner<T>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are shuffled to the next component.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<T> shuffle() {
		return setConnectionType(new ShufflePartitioner<T>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are forwarded to the local subtask of the next component.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<T> forward() {
		return setConnectionType(new ForwardPartitioner<T>());
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are distributed evenly to the next component.
	 * 
	 * @return The DataStream with shuffle partitioning set.
	 */
	public DataStream<T> distribute() {
		return setConnectionType(new DistributePartitioner<T>());
	}

	private DataStream<T> setConnectionType(StreamPartitioner<T> partitioner) {
		DataStream<T> returnStream = new DataStream<T>(this);

		for (int i = 0; i < returnStream.partitioners.size(); i++) {
			returnStream.partitioners.set(i, partitioner);
		}
		
		return returnStream;
	}
	
	/**
	 * Applies a Map transformation on a {@link DataStream}. The transformation
	 * calls a {@link MapFunction} for each element of the DataStream. Each
	 * MapFunction call returns exactly one element.
	 * 
	 * @param mapper
	 *            The MapFunction that is called for each element of the
	 *            DataStream.
	 * @param <R>
	 *            output type
	 * @return The transformed DataStream.
	 */
	public <R extends Tuple> StreamOperator<T, R> map(MapFunction<T, R> mapper) {
		return environment.addFunction("map", new DataStream<T>(this), mapper,
				new MapInvokable<T, R>(mapper));

	}

	/**
	 * Applies a CoMap transformation on two separate {@link DataStream}s. The
	 * transformation calls a {@link CoMapFunction#map1(Tuple)} for each element
	 * of the first DataStream (on which .coMapWith was called) and
	 * {@link CoMapFunction#map2(Tuple)} for each element of the second
	 * DataStream. Each CoMapFunction call returns exactly one element.
	 * 
	 * @param coMapper
	 *            The CoMapFunction used to jointly transform the two input
	 *            DataStreams
	 * @param otherStream
	 *            The DataStream that will be transformed with
	 *            {@link CoMapFunction#map2(Tuple)}
	 * @return The transformed DataStream
	 */
	public <T2 extends Tuple, R extends Tuple> DataStream<R> coMapWith(
			CoMapFunction<T, T2, R> coMapper, DataStream<T2> otherStream) {
		return environment.addCoFunction("coMap", new DataStream<T>(this), new DataStream<T2>(
				otherStream), coMapper, new CoMapInvokable<T, T2, R>(coMapper));
	}

	/**
	 * Applies a FlatMap transformation on a {@link DataStream}. The
	 * transformation calls a FlatMapFunction for each element of the
	 * DataStream. Each FlatMapFunction call can return any number of elements
	 * including none.
	 * 
	 * @param flatMapper
	 *            The FlatMapFunction that is called for each element of the
	 *            DataStream
	 * 
	 * @param <R>
	 *            output type
	 * @return The transformed DataStream.
	 */
	public <R extends Tuple> StreamOperator<T, R> flatMap(FlatMapFunction<T, R> flatMapper) {
		return environment.addFunction("flatMap", new DataStream<T>(this), flatMapper,
				new FlatMapInvokable<T, R>(flatMapper));
	}

	/**
	 * Applies a Filter transformation on a {@link DataStream}. The
	 * transformation calls a {@link FilterFunction} for each element of the
	 * DataStream and retains only those element for which the function returns
	 * true. Elements for which the function returns false are filtered.
	 * 
	 * @param filter
	 *            The FilterFunction that is called for each element of the
	 *            DataSet.
	 * @return The filtered DataStream.
	 */
	public StreamOperator<T, T> filter(FilterFunction<T> filter) {
		return environment.addFunction("filter", new DataStream<T>(this), filter,
				new FilterInvokable<T>(filter));
	}

	/**
	 * Applies a reduce transformation on preset chunks of the DataStream. The
	 * transformation calls a {@link GroupReduceFunction} for each tuple batch
	 * of the predefined size. Each GroupReduceFunction call can return any
	 * number of elements including none.
	 * 
	 * 
	 * @param reducer
	 *            The GroupReduceFunction that is called for each tuple batch.
	 * @param batchSize
	 *            The number of tuples grouped together in the batch.
	 * @param <R>
	 *            output type
	 * @return The modified DataStream.
	 */
	public <R extends Tuple> StreamOperator<T, R> batchReduce(GroupReduceFunction<T, R> reducer,
			int batchSize) {
		return environment.addFunction("batchReduce", new DataStream<T>(this), reducer,
				new BatchReduceInvokable<T, R>(reducer, batchSize));
	}

	/**
	 * Applies a reduce transformation on preset "time" chunks of the
	 * DataStream. The transformation calls a {@link GroupReduceFunction} on
	 * records received during the predefined time window. The window shifted
	 * after each reduce call. Each GroupReduceFunction call can return any
	 * number of elements including none.
	 * 
	 * 
	 * @param reducer
	 *            The GroupReduceFunction that is called for each time window.
	 * @param windowSize
	 *            The time window to run the reducer on, in milliseconds.
	 * @param <R>
	 *            output type
	 * @return The modified DataStream.
	 */
	public <R extends Tuple> StreamOperator<T, R> windowReduce(GroupReduceFunction<T, R> reducer,
			long windowSize) {
		return environment.addFunction("batchReduce", new DataStream<T>(this), reducer,
				new WindowReduceInvokable<T, R>(reducer, windowSize));
	}

	/**
	 * Adds the given sink to this environment. Only streams with sinks added
	 * will be executed once the {@link StreamExecutionEnvironment#execute()}
	 * method is called.
	 * 
	 * @param sinkFunction
	 *            The object containing the sink's invoke function.
	 * @return The modified DataStream.
	 */
	public DataStream<T> addSink(SinkFunction<T> sinkFunction) {
		return environment.addSink(new DataStream<T>(this), sinkFunction);
	}

	/**
	 * Writes a DataStream to the standard output stream (stdout). For each
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @return The closed DataStream.
	 */
	public DataStream<T> print() {
		return environment.print(new DataStream<T>(this));
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
	public DataStream<T> writeAsText(String path) {
		environment.writeAsText(this, path, new WriteFormatAsText<T>(), 1, null);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsText(String path, long millis) {
		environment.writeAsText(this, path, new WriteFormatAsText<T>(), millis, null);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsText(String path, int batchSize) {
		environment.writeAsText(this, path, new WriteFormatAsText<T>(), batchSize, null);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsText(String path, long millis, T endTuple) {
		environment.writeAsText(this, path, new WriteFormatAsText<T>(), millis, endTuple);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsText(String path, int batchSize, T endTuple) {
		environment.writeAsText(this, path, new WriteFormatAsText<T>(), batchSize, endTuple);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsCsv(String path) {
		environment.writeAsCsv(this, path, new WriteFormatAsCsv<T>(), 1, null);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsCsv(String path, long millis) {
		environment.writeAsCsv(this, path, new WriteFormatAsCsv<T>(), millis, null);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsCsv(String path, int batchSize) {
		environment.writeAsCsv(this, path, new WriteFormatAsCsv<T>(), batchSize, null);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsCsv(String path, long millis, T endTuple) {
		environment.writeAsCsv(this, path, new WriteFormatAsCsv<T>(), millis, endTuple);
		return new DataStream<T>(this);
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
	public DataStream<T> writeAsCsv(String path, int batchSize, T endTuple) {
		setMutability(false);
		environment.writeAsCsv(this, path, new WriteFormatAsCsv<T>(), batchSize, endTuple);
		return new DataStream<T>(this);
	}

	/**
	 * Initiates an iterative part of the program that executes multiple times
	 * and feeds back data streams. The iterative part needs to be closed by
	 * calling {@link IterativeDataStream#closeWith(DataStream)}. The data
	 * stream given to the {@code closeWith(DataStream)} method is the data
	 * stream that will be fed back and used as the input for the iteration
	 * head. Unlike in batch processing by default the output of the iteration
	 * stream is directed to both to the iteration head and the next component.
	 * To direct tuples to the iteration head or the output specifically one can
	 * use the {@code directTo(OutputSelector)} while referencing the iteration
	 * head as 'iterate'.
	 * 
	 * The iteration edge will be partitioned the same way as the first input of
	 * the iteration head.
	 * 
	 * @return The iterative data stream created.
	 */
	public IterativeDataStream<T> iterate() {
		return new IterativeDataStream<T>(this);
	}

	protected DataStream<T> addIterationSource(String iterationID) {
		environment.addIterationSource(this, iterationID);
		return new DataStream<T>(this);
	}

}