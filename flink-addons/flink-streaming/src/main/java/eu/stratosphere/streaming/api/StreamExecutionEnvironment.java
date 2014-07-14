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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;

//TODO:add link to ExecutionEnvironment
/**
 * ExecutionEnvironment for streaming jobs. An instance of it is necessary to
 * construct streaming topologies.
 * 
 */
public class StreamExecutionEnvironment {
	JobGraphBuilder jobGraphBuilder;
	
	private float clusterSize = 1;

	/**
	 * General constructor specifying the batch size in which the tuples are
	 * transmitted and their timeout boundary.
	 * 
	 * @param defaultBatchSize
	 *            number of tuples in a batch
	 * @param defaultBatchTimeoutMillis
	 *            timeout boundary in milliseconds
	 */
	public StreamExecutionEnvironment(int defaultBatchSize, long defaultBatchTimeoutMillis) {
		if (defaultBatchSize < 1) {
			throw new IllegalArgumentException("Batch size must be positive.");
		}
		if (defaultBatchTimeoutMillis < 1) {
			throw new IllegalArgumentException("Batch timeout must be positive.");
		}
		jobGraphBuilder = new JobGraphBuilder("jobGraph", FaultToleranceType.NONE,
				defaultBatchSize, defaultBatchTimeoutMillis);
	}

	/**
	 * Constructor for transmitting tuples individually with a 1 second timeout.
	 */
	public StreamExecutionEnvironment() {
		this(1, 1000);
	}

	public StreamExecutionEnvironment setClusterSize(int clusterSize) {
		this.clusterSize = clusterSize;
		return this;
	}

	/**
	 * Partitioning strategy on the stream.
	 */
	public static enum ConnectionType {
		SHUFFLE, BROADCAST, FIELD
	}

	/**
	 * Sets the batch size of the data stream in which the tuple are transmitted.
	 * 
	 * @param inputStream
	 *            input data stream
	 */
	public <T extends Tuple> void setBatchSize(DataStream<T> inputStream) {

		for (int i = 0; i < inputStream.connectIDs.size(); i++) {
			jobGraphBuilder.setBatchSize(inputStream.connectIDs.get(i),
					inputStream.batchSizes.get(i));
		}
	}

	// TODO: Link to JobGraph & JobGraphBuilder
	/**
	 * Internal function for assembling the underlying JobGraph of the job.
	 * 
	 * @param inputStream
	 *            input data stream
	 * @param outputID
	 *            ID of the output
	 */
	private <T extends Tuple> void connectGraph(DataStream<T> inputStream, String outputID) {

		for (int i = 0; i < inputStream.connectIDs.size(); i++) {
			ConnectionType type = inputStream.ctypes.get(i);
			String input = inputStream.connectIDs.get(i);
			int param = inputStream.cparams.get(i);

			switch (type) {
			case SHUFFLE:
				jobGraphBuilder.shuffleConnect(input, outputID);
				break;
			case BROADCAST:
				jobGraphBuilder.broadcastConnect(input, outputID);
				break;
			case FIELD:
				jobGraphBuilder.fieldsConnect(input, outputID, param);
				break;
			}

		}
		this.setBatchSize(inputStream);

	}

	// TODO: link to JobGraph, JobVertex
	/**
	 * Internal function for passing the user defined functions to the JobGraph
	 * of the job.
	 * 
	 * @param functionName
	 *            name of the function
	 * @param inputStream
	 *            input data stream
	 * @param function
	 *            the user defined function
	 * @param functionInvokable
	 *            the wrapping JobVertex instance
	 * @param parallelism
	 *            number of parallel instances of the function
	 * @return the data stream constructed
	 */
	<T extends Tuple, R extends Tuple> DataStream<R> addFunction(String functionName,
			DataStream<T> inputStream, final AbstractFunction function,
			UserTaskInvokable<T, R> functionInvokable, int parallelism) {
		DataStream<R> returnStream = new DataStream<R>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(function);
		} catch (IOException e) {
			e.printStackTrace();
		}

		jobGraphBuilder.setTask(returnStream.getId(), functionInvokable, functionName,
				baos.toByteArray(), parallelism,(int) Math.ceil(parallelism/clusterSize));

		connectGraph(inputStream, returnStream.getId());

		return returnStream;
	}

	/**
	 * Ads a sink to the data stream closing it.
	 * 
	 * @param inputStream
	 *            input data stream
	 * @param sinkFunction
	 *            the user defined function
	 * @param parallelism
	 *            number of parallel instances of the function
	 * @return the data stream constructed
	 */
	public <T extends Tuple> DataStream<T> addSink(DataStream<T> inputStream,
			SinkFunction<T> sinkFunction, int parallelism) {
		DataStream<T> returnStream = new DataStream<T>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(sinkFunction);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		jobGraphBuilder.setSink(returnStream.getId(), new SinkInvokable<T>(sinkFunction), "sink",
				baos.toByteArray(), parallelism, (int) Math.ceil(parallelism/clusterSize));

		connectGraph(inputStream, returnStream.getId());

		return returnStream;
	}

	/**
	 * Ads a sink to the data stream closing it. To parallelism is defaulted to
	 * 1.
	 * 
	 * @param inputStream
	 *            input data stream
	 * @param sinkFunction
	 *            the user defined function
	 * @param parallelism
	 *            number of parallel instances of the function
	 * @return the data stream constructed
	 */
	public <T extends Tuple> DataStream<T> addSink(DataStream<T> inputStream,
			SinkFunction<T> sinkFunction) {
		return addSink(inputStream, sinkFunction, 1);
	}

	// TODO: link to SinkFunction
	/**
	 * Dummy implementation of the SinkFunction writing every tuple to the
	 * standard output.
	 * 
	 * @param <IN>
	 *            Input tuple type
	 */
	private static final class DummySink<IN extends Tuple> extends SinkFunction<IN> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(IN tuple) {
			System.out.println(tuple);
		}

	}

	/**
	 * Prints the tuples of the data stream to the standard output.
	 * 
	 * @param inputStream
	 *            the input data stream
	 * @return the data stream constructed
	 */
	public <T extends Tuple> DataStream<T> print(DataStream<T> inputStream) {
		DataStream<T> returnStream = addSink(inputStream, new DummySink<T>());

		jobGraphBuilder.setBytesFrom(inputStream.getId(), returnStream.getId());

		return returnStream;
	}

	// TODO: Link to JobGraph and ClusterUtil
	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil.
	 */
	public void execute() {
		ClusterUtil.runOnMiniCluster(jobGraphBuilder.getJobGraph());
	}

	// TODO: Link to DataStream
	/**
	 * Ads a data source thus opening a data stream.
	 * 
	 * @param sourceFunction
	 *            the user defined function
	 * @param parallelism
	 *            number of parallel instances of the function
	 * @return the data stream constructed
	 */
	public <T extends Tuple> DataStream<T> addSource(SourceFunction<T> sourceFunction,
			int parallelism) {
		DataStream<T> returnStream = new DataStream<T>(this);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(sourceFunction);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		jobGraphBuilder.setSource(returnStream.getId(), sourceFunction, "source",
				baos.toByteArray(), parallelism, (int) Math.ceil(parallelism/clusterSize));

		return returnStream.copy();
	}

	//TODO: understand difference
	public DataStream<Tuple1<String>> readTextFile(String path) {
		return addSource(new FileSourceFunction(path), 1);
	}

	public DataStream<Tuple1<String>> readTextStream(String path) {
		return addSource(new FileStreamFunction(path), 1);
	}

	//TODO: Add link to JobGraphBuilder
	/**
	 * Getter of the JobGraphBuilder of the streaming job.
	 * @return
	 */
	public JobGraphBuilder jobGB() {
		return jobGraphBuilder;
	}
}
