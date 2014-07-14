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
import eu.stratosphere.types.TypeInformation;

public class DataStream<T extends Tuple> {

	private static Integer counter = 0;
	private final StreamExecutionEnvironment environment;
	private TypeInformation<T> type;
	private String id;
	List<String> connectIDs;
	List<ConnectionType> ctypes;
	List<Integer> cparams;
	List<Integer> batchSizes;

	/**
	 * Create a new DataStream in the given environment
	 * 
	 * @param environment
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
	 * Create a new DataStream in the given environment with the given id
	 * 
	 * @param environment
	 * @param id
	 */
	private DataStream(StreamExecutionEnvironment environment, String operatorType, String id) {
		this.environment = environment;
		this.id = id;
	}

	/**
	 * Initialize the connections.
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
	 * Creates an identical DataStream.
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
		return copiedStream;
	}

	/**
	 * Returns the id of the DataStream.
	 * 
	 * @return ID
	 */
	public String getId() {
		return id;
	}

	/**
	 * Groups a number of consecutive elements from the DataStream to increase
	 * network throughput.
	 * 
	 * @param batchSize
	 *            The number of elements to group.
	 * @return The DataStream.
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
	 * Connecting DataStream outputs with each other. The streams connected
	 * using this operator will be transformed simultaneously. It creates a
	 * joint output of the connected streams.
	 * 
	 * @param stream
	 *            The DataStream to connect output with.
	 * @return The connected DataStream.
	 */
	public DataStream<T> connectWith(DataStream<T>... streams) {
		DataStream<T> returnStream = copy();

		for (DataStream<T> stream : streams) {
			addConnection(returnStream, stream);
		}
		return returnStream;
	}

	public DataStream<T> addConnection(DataStream<T> returnStream, DataStream<T> stream) {
		returnStream.connectIDs.addAll(stream.connectIDs);
		returnStream.ctypes.addAll(stream.ctypes);
		returnStream.cparams.addAll(stream.cparams);
		returnStream.batchSizes.addAll(stream.batchSizes);

		return returnStream;
	}

	/**
	 * Send the output tuples of the DataStream to the next vertices partitioned
	 * by their hashcode.
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
	 * Broadcast the output tuples to every parallel instance of the next
	 * component.
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
	 * Applies a FlatMap transformation on a DataStream. The transformation
	 * calls a FlatMapFunction for each element of the DataSet. Each
	 * FlatMapFunction call can return any number of elements including none.
	 * 
	 * @param flatMapper
	 *            The FlatMapFunction that is called for each element of the
	 *            DataStream
	 * @param parallelism
	 *            The number of threads the function runs on.
	 * @return The transformed DataStream.
	 */
	public <R extends Tuple> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapper, int parallelism) {
		return environment.addFunction("flatMap", this.copy(), flatMapper,
				new FlatMapInvokable<T, R>(flatMapper), parallelism);
	}

	/**
	 * Applies a Map transformation on a DataStream. The transformation calls a
	 * MapFunction for each element of the DataStream. Each MapFunction call
	 * returns exactly one element.
	 * 
	 * @param mapper
	 *            The MapFunction that is called for each element of the
	 *            DataStream.
	 * @param parallelism
	 *            The number of threads the function runs on.
	 * @return The transformed DataStream.
	 */
	public <R extends Tuple> DataStream<R> map(MapFunction<T, R> mapper, int parallelism) {
		return environment.addFunction("map", this.copy(), mapper, new MapInvokable<T, R>(mapper),
				parallelism);
	}

	/**
	 * Applies a reduce transformation on preset chunks of the DataStream. The
	 * transformation calls a GroupReduceFunction for each tuple batch of the
	 * predefined size. Each GroupReduceFunction call can return any number of
	 * elements including none.
	 * 
	 * 
	 * @param reducer
	 *            The GroupReduceFunction that is called for each tuple batch.
	 * @param batchSize
	 *            The number of tuples grouped together in the batch.
	 * @param parallelism
	 *            The number of threads the function runs on.
	 * @return The modified datastream.
	 */
	public <R extends Tuple> DataStream<R> batchReduce(GroupReduceFunction<T, R> reducer,
			int batchSize, int paralelism) {
		return environment.addFunction("batchReduce", batch(batchSize).copy(), reducer,
				new BatchReduceInvokable<T, R>(reducer), paralelism);
	}

	/**
	 * Applies a Filter transformation on a DataStream. The transformation calls
	 * a FilterFunction for each element of the DataStream and retains only
	 * those element for which the function returns true. Elements for which the
	 * function returns false are filtered.
	 * 
	 * @param filter
	 *            The FilterFunction that is called for each element of the
	 *            DataSet.
	 * @param paralelism
	 *            The number of threads the function runs on.
	 * @return The filtered DataStream.
	 */
	public DataStream<T> filter(FilterFunction<T> filter, int paralelism) {
		return environment.addFunction("filter", this.copy(), filter,
				new FilterInvokable<T>(filter), paralelism);
	}

	/**
	 * Sets the given sink function.
	 * 
	 * @param sinkFunction
	 *            The object containing the sink's invoke function.
	 * @param paralelism
	 *            The number of threads the function runs on.
	 * @return The modified datastream.
	 */
	public DataStream<T> addSink(SinkFunction<T> sinkFunction, int paralelism) {
		return environment.addSink(this.copy(), sinkFunction, paralelism);
	}

	/**
	 * Sets the given sink function.
	 * 
	 * @param sinkFunction
	 *            The object containing the sink's invoke function.
	 * @return
	 */
	public DataStream<T> addSink(SinkFunction<T> sinkFunction) {
		return environment.addSink(this.copy(), sinkFunction);
	}

	/**
	 * Prints the tuples from the DataStream.
	 * 
	 * @return
	 */
	public DataStream<T> print() {
		return environment.print(this.copy());
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
	 * Get the type information.
	 * 
	 * @return The type of the generic parameter.
	 */
	public TypeInformation<T> getType() {
		return this.type;
	}
}