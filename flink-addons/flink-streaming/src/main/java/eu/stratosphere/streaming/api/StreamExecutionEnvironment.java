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
import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.util.Collector;

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

	/**
	 * Set the number of machines in the executing cluster. Used for setting
	 * task parallelism.
	 * 
	 * @param clusterSize
	 * @return
	 */
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
	 * Sets the batch size of the data stream in which the tuple are
	 * transmitted.
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

		jobGraphBuilder.setTask(returnStream.getId(), functionInvokable, functionName,
				serializeToByteArray(function), parallelism,
				(int) Math.ceil(parallelism / clusterSize));

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

		jobGraphBuilder.setSink(returnStream.getId(), new SinkInvokable<T>(sinkFunction), "sink",
				serializeToByteArray(sinkFunction), parallelism,
				(int) Math.ceil(parallelism / clusterSize));

		connectGraph(inputStream, returnStream.getId());

		return returnStream;
	}

	/**
	 * Creates a new DataStream that contains a sequence of numbers.
	 * 
	 * @param from
	 *            First number in the sequence
	 * @param to
	 *            Last element in the sequence
	 * @return the data stream constructed
	 */
	public DataStream<Tuple1<Long>> generateSequence(long from, long to) {
		return addSource(new SequenceSource(from, to), 1);
	}

	/**
	 * Source Function used to generate sequence
	 * 
	 */
	private static final class SequenceSource extends SourceFunction<Tuple1<Long>> {

		private static final long serialVersionUID = 1L;

		long from;
		long to;
		Tuple1<Long> outTuple = new Tuple1<Long>();

		public SequenceSource(long from, long to) {
			this.from = from;
			this.to = to;
		}

		@Override
		public void invoke(Collector<Tuple1<Long>> collector) throws Exception {
			for (long i = from; i <= to; i++) {
				outTuple.f0 = i;
				collector.collect(outTuple);
			}
		}

	}

	/**
	 * Creates a new DataStream by iterating through the given data. The
	 * elements are inserted into a Tuple1.
	 * 
	 * @param data
	 * 
	 * @return
	 */
	public <X> DataStream<Tuple1<X>> fromElements(X... data) {
		DataStream<Tuple1<X>> returnStream = new DataStream<Tuple1<X>>(this);

		jobGraphBuilder.setSource(returnStream.getId(), new FromElementsSource<X>(data),
				"elements", serializeToByteArray(data[0]), 1, 1);

		return returnStream.copy();
	}

	/**
	 * Creates a new DataStream by iterating through the given data collection.
	 * The elements are inserted into a Tuple1.
	 * 
	 * @param data
	 * 
	 * @return
	 */
	public <X> DataStream<Tuple1<X>> fromCollection(Collection<X> data) {
		DataStream<Tuple1<X>> returnStream = new DataStream<Tuple1<X>>(this);

		jobGraphBuilder.setSource(returnStream.getId(), new FromElementsSource<X>(data),
				"elements", serializeToByteArray(data.toArray()[0]), 1, 1);

		return returnStream.copy();
	}

	/**
	 * SourceFunction created to use with fromElements and fromCollection
	 * 
	 * @param <T>
	 */
	private static class FromElementsSource<T> extends SourceFunction<Tuple1<T>> {

		private static final long serialVersionUID = 1L;

		Iterable<T> iterable;
		Tuple1<T> outTuple = new Tuple1<T>();

		public FromElementsSource(T... elements) {
			this.iterable = (Iterable<T>) Arrays.asList(elements);
		}

		public FromElementsSource(Collection<T> elements) {
			this.iterable = (Iterable<T>) elements;
		}

		@Override
		public void invoke(Collector<Tuple1<T>> collector) throws Exception {
			for (T element : iterable) {
				outTuple.f0 = element;
				collector.collect(outTuple);
			}
		}

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

		jobGraphBuilder.setSource(returnStream.getId(), sourceFunction, "source",
				serializeToByteArray(sourceFunction), parallelism,
				(int) Math.ceil(parallelism / clusterSize));

		return returnStream.copy();
	}

	/**
	 * Read a text file from the given path and emits the lines as
	 * Tuple1<Strings>-s
	 * 
	 * @param path
	 *            Input file
	 * @return the data stream constructed
	 */
	public DataStream<Tuple1<String>> readTextFile(String path) {
		return addSource(new FileSourceFunction(path), 1);
	}

	/**
	 * Streams a text file from the given path by reading through it multiple
	 * times.
	 * 
	 * @param path
	 *            Input file
	 * @return the data stream constructed
	 */
	public DataStream<Tuple1<String>> readTextStream(String path) {
		return addSource(new FileStreamFunction(path), 1);
	}

	/**
	 * Converts object to byte array using default java serialization
	 * 
	 * @param object
	 *            Object to be serialized
	 * @return Serialized object
	 */
	private byte[] serializeToByteArray(Object object) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return baos.toByteArray();
	}

	// TODO: Add link to JobGraphBuilder
	/**
	 * Getter of the JobGraphBuilder of the streaming job.
	 * 
	 * @return
	 */
	public JobGraphBuilder jobGB() {
		return jobGraphBuilder;
	}
}
