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
	
	protected DataStream() {
		// TODO implement
		context = new StreamExecutionEnvironment();
		id = "source";
		initConnections();
	}

	protected DataStream(StreamExecutionEnvironment context) {
		if (context == null) {
			throw new NullPointerException("context is null");
		}

		// TODO add name based on component number an preferable sequential id
		this.id = Long.toHexString(random.nextLong()) + Long.toHexString(random.nextLong());
		this.context = context;
		initConnections();

	}

	private DataStream(StreamExecutionEnvironment context, String id) {
		this(context);
		this.id = id;
	}
	
	//TODO: create copy method (or constructor) and copy datastream at every operator

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

	public DataStream<T> copy() {
		DataStream<T> copiedStream = new DataStream<T>(context, getId());
		copiedStream.type = this.type;
		
		copiedStream.connectIDs = new ArrayList<String>(this.connectIDs);
		
		copiedStream.ctypes = new ArrayList<StreamExecutionEnvironment.ConnectionType>(this.ctypes);
		copiedStream.cparams = new ArrayList<Integer>(this.cparams);
		copiedStream.batchSizes = new ArrayList<Integer>(this.batchSizes);
		return copiedStream;
	}
	
	public String getId() {
		return id;
	}

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

	public DataStream<T> connectWith(DataStream<T> stream) {
		DataStream<T> returnStream = copy();

		returnStream.connectIDs.addAll(stream.connectIDs);
		returnStream.ctypes.addAll(stream.ctypes);
		returnStream.cparams.addAll(stream.cparams);
		returnStream.batchSizes.addAll(stream.batchSizes);
		return returnStream;
	}

	public DataStream<T> partitionBy(int keyposition) {
		DataStream<T> returnStream = copy();

		for (int i = 0; i < returnStream.ctypes.size(); i++) {
			returnStream.ctypes.set(i, ConnectionType.FIELD);
			returnStream.cparams.set(i, keyposition);
		}
		return returnStream;
	}

	public DataStream<T> broadcast() {
		DataStream<T> returnStream = copy();

		for (int i = 0; i < returnStream.ctypes.size(); i++) {
			returnStream.ctypes.set(i, ConnectionType.BROADCAST);
		}
		return returnStream;
	}

	public <R extends Tuple> DataStream<R> flatMap(FlatMapFunction<T, R> flatMapper, int paralelism) {
		return context.addFunction("flatMap", this.copy(), flatMapper, new FlatMapInvokable<T, R>(
				flatMapper), paralelism);
	}

	public <R extends Tuple> DataStream<R> map(MapFunction<T, R> mapper, int paralelism) {
		return context.addFunction("map", this.copy(), mapper, new MapInvokable<T, R>(mapper), paralelism);
	}

	public <R extends Tuple> DataStream<R> batchReduce(GroupReduceFunction<T, R> reducer, int batchSize, int paralelism) {
		return context.addFunction("batchReduce", batch(batchSize).copy(), reducer, new BatchReduceInvokable<T, R>(
				reducer), paralelism);
	}

	public DataStream<T> filter(FilterFunction<T> filter, int paralelism) {
		return context.addFunction("filter", this.copy(), filter, new FilterInvokable<T>(filter), paralelism);
	}

	public DataStream<T> addSink(SinkFunction<T> sinkFunction) {
		return context.addSink(this.copy(), sinkFunction);
	}

	public DataStream<T> addDummySink() {
		return context.addDummySink(this.copy());
	}

	protected void setType(TypeInformation<T> type) {
		this.type = type;
	}

	public TypeInformation<T> getType() {
		return this.type;
	}
}