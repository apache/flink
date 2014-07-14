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
import java.util.Random;

import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.streaming.api.StreamExecutionEnvironment.ConnectionType;
import eu.stratosphere.types.TypeInformation;

public class DataStream<T extends Tuple> {

	private final StreamExecutionEnvironment context;
	private TypeInformation<T> type;
	private final Random random = new Random();
	private String id;
	List<String> connectIDs;
	List<ConnectionType> ctypes;
	List<Integer> cparams;
	List<Integer> batchSizes;
	
	/**
	 * Constructor
	 */
	protected DataStream() {
		// TODO implement
		context = new StreamExecutionEnvironment();
		id = "source";
		initConnections();
	}

	/**
	 * Constructor
	 * @param context
	 */
	protected DataStream(StreamExecutionEnvironment context) {
		if (context == null) {
			throw new NullPointerException("context is null");
		}

		// TODO add name based on component number an preferable sequential id
		this.id = Long.toHexString(random.nextLong()) + Long.toHexString(random.nextLong());
		this.context = context;
		initConnections();

	}

	/**
	 * Constructor
	 * @param context
	 * @param id
	 */
	private DataStream(StreamExecutionEnvironment context, String id) {
		this(context);
		this.id = id;
	}
	
	//TODO: create copy method (or constructor) and copy datastream at every operator

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
	 * Creates an identical datastream.
	 * @return
	 * The identical datastream.
	 */
	public DataStream<T> copy() {
		DataStream<T> copiedStream = new DataStream<T>(context, getId());
		copiedStream.type = this.type;
		
		copiedStream.connectIDs = new ArrayList<String>(this.connectIDs);
		
		copiedStream.ctypes = new ArrayList<StreamExecutionEnvironment.ConnectionType>(this.ctypes);
		copiedStream.cparams = new ArrayList<Integer>(this.cparams);
		copiedStream.batchSizes = new ArrayList<Integer>(this.batchSizes);
		return copiedStream;
	}
	
	/**
	 * Gets the id of the datastream.
	 * @return
	 * The id of the datastream.
	 */
	public String getId() {
		return id;
	}

	/**
	 * Collects a number of consecutive elements from the datastream.
	 * @param batchSize
	 * The number of elements to collect.
	 * @return
	 * The collected elements.
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
	 * Connecting streams to each other.
	 * @param stream
	 * The stream it connects to.
	 * @return
	 * The new already connected datastream.
	 */
	public DataStream<T> connectWith(DataStream<T> stream) {
		DataStream<T> returnStream = copy();

		returnStream.connectIDs.addAll(stream.connectIDs);
		returnStream.ctypes.addAll(stream.ctypes);
		returnStream.cparams.addAll(stream.cparams);
		returnStream.batchSizes.addAll(stream.batchSizes);
		return returnStream;
	}

	/**
	 * Send the elements of the stream to the following vertices according to their hashcode.
	 * @param keyposition
	 * The field used to compute the hashcode.
	 * @return
	 * The original datastream.
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
	 * Send the elements of the stream to every following vertices of the graph.
	 * @return
	 * The datastream.
	 */
	public DataStream<T> broadcast() {
		DataStream<T> returnStream = copy();

		for (int i = 0; i < returnStream.ctypes.size(); i++) {
			returnStream.ctypes.set(i, ConnectionType.BROADCAST);
		}
		return returnStream;
	}

	/**
	 * Sets the given flatmap function.
	 * @param flatMapper
	 * The object containing the flatmap function.
	 * @param paralelism
	 * The number of threads the function runs on.
	 * @return
	 * The modified datastream.
	 */
	public <R extends Tuple> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapper, int paralelism) {
		return context.addFunction("flatMap", this.copy(), flatMapper, new FlatMapInvokable<T, R>(
				flatMapper), paralelism);
	}

	/**
	 * Sets the given map function.
	 * @param mapper
	 * The object containing the map function.
	 * @param paralelism
	 * The number of threads the function runs on.
	 * @return
	 * The modified datastream.
	 */
	public <R extends Tuple> DataStream<R> map(MapFunction<T, R> mapper, int paralelism) {
		return context.addFunction("map", this.copy(), mapper, new MapInvokable<T, R>(mapper), paralelism);
	}

	/**
	 * Sets the given batchreduce function.
	 * @param reducer
	 * The object containing the batchreduce function.
	 * @param batchSize
	 * The number of elements proceeded at the same time
	 * @param paralelism
	 * The number of threads the function runs on.
	 * @return
	 * The modified datastream.
	 */
	public <R extends Tuple> DataStream<R> batchReduce(GroupReduceFunction<T, R> reducer, int batchSize, int paralelism) {
		return context.addFunction("batchReduce", batch(batchSize).copy(), reducer, new BatchReduceInvokable<T, R>(
				reducer), paralelism);
	}

	/**
	 * Sets the given filter function.
	 * @param filter
	 * The object containing the filter function.
	 * @param paralelism
	 * The number of threads the function runs on.
	 * @return
	 * The modified datastream.
	 */
	public DataStream<T> filter(FilterFunction<T> filter, int paralelism) {
		return context.addFunction("filter", this.copy(), filter, new FilterInvokable<T>(filter), paralelism);
	}

	/**
	 * Sets the given sink function.
	 * @param sinkFunction
	 * The object containing the sink's invoke function.
	 * @param paralelism
	 * The number of threads the function runs on.
	 * @return
	 * The modified datastream.
	 */
	public DataStream<T> addSink(SinkFunction<T> sinkFunction, int paralelism) {
		return context.addSink(this.copy(), sinkFunction, paralelism);
	}
	
	/**
	 * Sets the given sink function.
	 * @param sinkFunction
	 * The object containing the sink's invoke function.
	 * @return
	 * The modified datastream.
	 */
	public DataStream<T> addSink(SinkFunction<T> sinkFunction) {
		return context.addSink(this.copy(), sinkFunction);
	}

	/**
	 * Prints the datastream.
	 * @return
	 * The original stream.
	 */
	public DataStream<T> print() {
		return context.print(this.copy());
	}

	/**
	 * Set the type parameter.
	 * @param type
	 * The type parameter.
	 */
	protected void setType(TypeInformation<T> type) {
		this.type = type;
	}

	/**
	 * Get the type information.
	 * @return
	 * The type of the generic parameter.
	 */
	public TypeInformation<T> getType() {
		return this.type;
	}
}