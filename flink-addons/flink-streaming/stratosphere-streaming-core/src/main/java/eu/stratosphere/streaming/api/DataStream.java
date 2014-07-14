/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.streaming.api.StreamExecutionEnvironment.ConnectionType;
import eu.stratosphere.streaming.api.function.SinkFunction;
import eu.stratosphere.streaming.api.invokable.BatchReduceInvokable;
import eu.stratosphere.streaming.api.invokable.FilterInvokable;
import eu.stratosphere.streaming.api.invokable.FlatMapInvokable;
import eu.stratosphere.streaming.api.invokable.MapInvokable;
import eu.stratosphere.types.TypeInformation;

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
	protected TypeInformation<T> type;
	protected String id;
	int dop;
	List<String> connectIDs;
	List<ConnectionType> ctypes;
	List<Integer> cparams;
	List<Integer> batchSizes;

	/**
	 * Create a new {@link DataStream} in the given execution environment
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
		initConnections();

	}

	/**
	 * Create a new {@link DataStream} in the given environment with the given
	 * id
	 * 
	 * @param environment
	 *            StreamExecutionEnvironment
	 * @param id
	 *            The id of the DataStream
	 */
	protected DataStream(StreamExecutionEnvironment environment, String operatorType, String id) {
		this.environment = environment;
		this.id = id;
		initConnections();
	}

	/**
	 * Initialize the connection and partitioning among the connected
	 * {@link DataStream}s.
	 */
	private void initConnections() {
		connectIDs = new ArrayList<String>();
		connectIDs.add(getId());
		ctypes = new ArrayList<StreamExecutionEnvironment.ConnectionType>();
		ctypes.add(ConnectionType.SHUFFLE);
		cparams = new ArrayList<Integer>();
		cparams.add(0);
		batchSizes = new ArrayList<Integer>();
		batchSizes.add(1);

	}

	/**
	 * Creates an identical {@link DataStream}.
	 * 
	 * @return The DataStream copy.
	 */
	public DataStream<T> copy() {
		DataStream<T> copiedStream = new DataStream<T>(environment, "", getId());
		copiedStream.type = this.type;

		copiedStream.connectIDs = new ArrayList<String>(this.connectIDs);

		copiedStream.ctypes = new ArrayList<StreamExecutionEnvironment.ConnectionType>(this.ctypes);
		copiedStream.cparams = new ArrayList<Integer>(this.cparams);
		copiedStream.batchSizes = new ArrayList<Integer>(this.batchSizes);
		copiedStream.dop = this.dop;
		return copiedStream;
	}

	StreamExecutionEnvironment getEnvironment(){
		return environment;
	}
	
	/**
	 * Returns the ID of the {@link DataStream}.
	 * 
	 * @return ID of the datastream
	 */
	public String getId() {
		return id;
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
		this.dop = dop;

		environment.setOperatorParallelism(this);

		return this.copy();

	}

	/**
	 * Gets the degree of parallelism for this operator.
	 * 
	 * @return The parallelism set for this operator.
	 */
	public int getParallelism() {
		return this.dop;
	}

	/**
	 * Groups a number of consecutive elements from the {@link DataStream} to
	 * increase network throughput. It has no effect on the operators applied to
	 * the DataStream.
	 * 
	 * @param batchSize
	 *            The number of elements to group.
	 * @return The DataStream with batching set.
	 */
	public DataStream<T> batch(int batchSize) {
		DataStream<T> returnStream = copy();

		if (batchSize < 1) {
			throw new IllegalArgumentException("Batch size must be positive.");
		}

		for (int i = 0; i < returnStream.batchSizes.size(); i++) {
			returnStream.batchSizes.set(i, batchSize);
		}
		return returnStream;
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
		DataStream<T> returnStream = copy();

		for (DataStream<T> stream : streams) {
			addConnection(returnStream, stream);
		}
		return returnStream;
	}

	private DataStream<T> addConnection(DataStream<T> returnStream, DataStream<T> stream) {
		returnStream.connectIDs.addAll(stream.connectIDs);
		returnStream.ctypes.addAll(stream.ctypes);
		returnStream.cparams.addAll(stream.cparams);
		returnStream.batchSizes.addAll(stream.batchSizes);

		return returnStream;
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
		DataStream<T> returnStream = copy();

		for (int i = 0; i < returnStream.ctypes.size(); i++) {
			returnStream.ctypes.set(i, ConnectionType.FIELD);
			returnStream.cparams.set(i, keyposition);
		}
		return returnStream;
	}

	/**
	 * Sets the partitioning of the {@link DataStream} so that the output tuples
	 * are broadcasted to every parallel instance of the next component.
	 * 
	 * @return The DataStream with broadcast partitioning set.
	 */
	public DataStream<T> broadcast() {
		DataStream<T> returnStream = copy();

		for (int i = 0; i < returnStream.ctypes.size(); i++) {
			returnStream.ctypes.set(i, ConnectionType.BROADCAST);
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
	public <R extends Tuple> DataStream<R> map(MapFunction<T, R> mapper) {
		return environment.addFunction("map", this.copy(), mapper, new MapInvokable<T, R>(mapper));

	}

	/**
	 * Applies a FlatMap transformation on a {@link DataStream}. The
	 * transformation calls a FlatMapFunction for each element of the DataSet.
	 * Each FlatMapFunction call can return any number of elements including
	 * none.
	 * 
	 * @param flatMapper
	 *            The FlatMapFunction that is called for each element of the
	 *            DataStream
	 * 
	 * @param <R>
	 *            output type
	 * @return The transformed DataStream.
	 */
	public <R extends Tuple> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapper) {
		return environment.addFunction("flatMap", this.copy(), flatMapper,
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
	public DataStream<T> filter(FilterFunction<T> filter) {
		return environment.addFunction("filter", this.copy(), filter,
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
	public <R extends Tuple> DataStream<R> batchReduce(GroupReduceFunction<T, R> reducer,
			int batchSize) {
		return environment.addFunction("batchReduce", batch(batchSize).copy(), reducer,
				new BatchReduceInvokable<T, R>(reducer));
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
		return environment.addSink(this.copy(), sinkFunction);
	}

	/**
	 * Writes a DataStream to the standard output stream (stdout). For each
	 * element of the DataStream the result of {@link Object#toString()} is
	 * written.
	 * 
	 * @return The closed DataStream.
	 */
	public DataStream<T> print() {
		return environment.print(this.copy());
	}

	public DataStream<T> addIterationSource() {
		environment.addIterationSource(this);
		return this.copy();
	}

	public DataStream<T> addIterationSink() {
		environment.addIterationSink(this);
		return this;
	}

	public IterativeDataStream<T> iterate() {
		addIterationSource();
		return new IterativeDataStream<T>(this);
	}

	/**
	 * Set the type parameter.
	 * 
	 * @param type
	 *            The type parameter.
	 */
	protected void setType(TypeInformation<T> type) {
		this.type = type;
	}

	/**
	 * Get the type information for this DataStream.
	 * 
	 * @return The type of the generic parameter.
	 */
	public TypeInformation<T> getType() {
		return this.type;
	}
}