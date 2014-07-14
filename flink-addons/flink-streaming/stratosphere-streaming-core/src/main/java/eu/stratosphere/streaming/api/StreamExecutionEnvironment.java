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
import java.util.Collection;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.RemoteEnvironment;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.function.FileSourceFunction;
import eu.stratosphere.streaming.api.function.FileStreamFunction;
import eu.stratosphere.streaming.api.function.FromElementsFunction;
import eu.stratosphere.streaming.api.function.PrintSinkFunction;
import eu.stratosphere.streaming.api.function.GenSequenceFunction;
import eu.stratosphere.streaming.api.function.SinkFunction;
import eu.stratosphere.streaming.api.function.SourceFunction;
import eu.stratosphere.streaming.api.invokable.SinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;

//TODO:add link to ExecutionEnvironment
/**
 * ExecutionEnvironment for streaming jobs. An instance of it is necessary to
 * construct streaming topologies.
 * 
 */
public abstract class StreamExecutionEnvironment {
	private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();

	private int degreeOfParallelism = -1;

	protected JobGraphBuilder jobGraphBuilder;

	/**
	 * Partitioning strategy on the stream.
	 */
	public static enum ConnectionType {
		SHUFFLE, BROADCAST, FIELD
	}

	// --------------------------------------------------------------------------------------------
	// Constructor and Properties
	// --------------------------------------------------------------------------------------------

	/**
	 * Constructor for creating StreamExecutionEnvironment
	 */
	protected StreamExecutionEnvironment() {
		jobGraphBuilder = new JobGraphBuilder("jobGraph", FaultToleranceType.NONE);
	}

	public int getDegreeOfParallelism() {
		return degreeOfParallelism;
	}

	public void setDegreeOfParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1)
			throw new IllegalArgumentException("Degree of parallelism must be at least one.");

		this.degreeOfParallelism = degreeOfParallelism;
	}

	public void setDefaultBatchSize(int batchSize) {
		if (batchSize < 1) {
			throw new IllegalArgumentException("Batch size must be positive.");
		} else {
			jobGraphBuilder.setDefaultBatchSize(batchSize);
		}
	}

	public void setBatchTimeout(int timeout) {
		if (timeout < 1) {
			throw new IllegalArgumentException("Batch timeout must be positive.");
		} else {
			jobGraphBuilder.setBatchTimeout(timeout);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Data stream creations
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a DataStream that represents the Strings produced by reading the
	 * given file line wise. The file will be read with the system's default
	 * character set.
	 * 
	 * @param filePath
	 *            The path of the file, as a URI (e.g.,
	 *            "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return The DataStream representing the text file.
	 */
	public DataStream<Tuple1<String>> readTextFile(String filePath) {
		return addSource(new FileSourceFunction(filePath), 1);
	}

	public DataStream<Tuple1<String>> readTextFile(String filePath, int parallelism) {
		return addSource(new FileSourceFunction(filePath), parallelism);
	}

	/**
	 * Creates a DataStream that represents the Strings produced by reading the
	 * given file line wise multiple times(infinite). The file will be read with
	 * the system's default character set.
	 * 
	 * @param filePath
	 *            The path of the file, as a URI (e.g.,
	 *            "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return The DataStream representing the text file.
	 */
	public DataStream<Tuple1<String>> readTextStream(String filePath) {
		return addSource(new FileStreamFunction(filePath), 1);
	}

	public DataStream<Tuple1<String>> readTextStream(String filePath, int parallelism) {
		return addSource(new FileStreamFunction(filePath), parallelism);
	}

	/**
	 * Creates a new DataStream that contains the given elements. The elements
	 * must all be of the same type, for example, all of the String or Integer.
	 * The sequence of elements must not be empty. Furthermore, the elements
	 * must be serializable (as defined in java.io.Serializable), because the
	 * execution environment may ship the elements into the cluster.
	 * 
	 * @param data
	 *            The collection of elements to create the DataStream from.
	 * @param <X>
	 *            type of the returned stream
	 * @return The DataStream representing the elements.
	 */
	public <X> DataStream<Tuple1<X>> fromElements(X... data) {
		DataStream<Tuple1<X>> returnStream = new DataStream<Tuple1<X>>(this, "elements");

		jobGraphBuilder.setSource(returnStream.getId(), new FromElementsFunction<X>(data),
				"elements", serializeToByteArray(data[0]), 1);

		return returnStream.copy();
	}

	/**
	 * Creates a DataStream from the given non-empty collection. The type of the
	 * DataStream is that of the elements in the collection. The elements need
	 * to be serializable (as defined by java.io.Serializable), because the
	 * framework may move the elements into the cluster if needed.
	 * 
	 * @param data
	 *            The collection of elements to create the DataStream from.
	 * @param <X>
	 *            type of the returned stream
	 * @return The DataStream representing the elements.
	 */
	public <X> DataStream<Tuple1<X>> fromCollection(Collection<X> data) {
		DataStream<Tuple1<X>> returnStream = new DataStream<Tuple1<X>>(this, "elements");

		jobGraphBuilder.setSource(returnStream.getId(), new FromElementsFunction<X>(data),
				"elements", serializeToByteArray(data.toArray()[0]), 1);

		return returnStream.copy();
	}

	/**
	 * Creates a new DataStream that contains a sequence of numbers.
	 * 
	 * @param from
	 *            The number to start at (inclusive).
	 * @param to
	 *            The number to stop at (inclusive)
	 * @return A DataStrean, containing all number in the [from, to] interval.
	 */
	public DataStream<Tuple1<Long>> generateSequence(long from, long to) {
		return addSource(new GenSequenceFunction(from, to), 1);
	}

	// TODO: Link to DataStream
	/**
	 * Ads a data source thus opening a data stream.
	 * 
	 * @param sourceFunction
	 *            the user defined function
	 * @param parallelism
	 *            number of parallel instances of the function
	 * @param <T>
	 *            type of the returned stream
	 * @return the data stream constructed
	 */
	public <T extends Tuple> DataStream<T> addSource(SourceFunction<T> sourceFunction,
			int parallelism) {
		DataStream<T> returnStream = new DataStream<T>(this, "source");

		jobGraphBuilder.setSource(returnStream.getId(), sourceFunction, "source",
				serializeToByteArray(sourceFunction), parallelism);

		return returnStream.copy();
	}

	// --------------------------------------------------------------------------------------------
	// Data stream operators and sinks
	// --------------------------------------------------------------------------------------------

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
	 * @param <T>
	 *            type of the input stream
	 * @param <R>
	 *            type of the return stream
	 * @return the data stream constructed
	 */
	<T extends Tuple, R extends Tuple> DataStream<R> addFunction(String functionName,
			DataStream<T> inputStream, final AbstractFunction function,
			UserTaskInvokable<T, R> functionInvokable, int parallelism) {
		DataStream<R> returnStream = new DataStream<R>(this, functionName);

		jobGraphBuilder.setTask(returnStream.getId(), functionInvokable, functionName,
				serializeToByteArray(function), parallelism);

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
	 * @param <T>
	 *            type of the returned stream
	 * @return the data stream constructed
	 */
	public <T extends Tuple> DataStream<T> addSink(DataStream<T> inputStream,
			SinkFunction<T> sinkFunction, int parallelism) {
		DataStream<T> returnStream = new DataStream<T>(this, "sink");

		jobGraphBuilder.setSink(returnStream.getId(), new SinkInvokable<T>(sinkFunction), "sink",
				serializeToByteArray(sinkFunction), parallelism);

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
	 * @param <T>
	 *            type of the returned stream
	 * @return the data stream constructed
	 */
	public <T extends Tuple> DataStream<T> addSink(DataStream<T> inputStream,
			SinkFunction<T> sinkFunction) {
		return addSink(inputStream, sinkFunction, 1);
	}

	/**
	 * Prints the tuples of the data stream to the standard output.
	 * 
	 * @param inputStream
	 *            the input data stream
	 * 
	 * @param <T>
	 *            type of the returned stream
	 * @return the data stream constructed
	 */
	public <T extends Tuple> DataStream<T> print(DataStream<T> inputStream) {
		DataStream<T> returnStream = addSink(inputStream, new PrintSinkFunction<T>());
	
		jobGraphBuilder.setBytesFrom(inputStream.getId(), returnStream.getId());
	
		return returnStream;
	}

	// TODO: Link to JobGraph & JobGraphBuilder
	/**
	 * Internal function for assembling the underlying JobGraph of the job.
	 * 
	 * @param inputStream
	 *            input data stream
	 * @param outputID
	 *            ID of the output
	 * @param <T>
	 *            type of the input stream
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

	/**
	 * Sets the batch size of the data stream in which the tuple are
	 * transmitted.
	 * 
	 * @param inputStream
	 *            input data stream
	 * @param <T>
	 *            type of the input stream
	 */
	public <T extends Tuple> void setBatchSize(DataStream<T> inputStream) {

		for (int i = 0; i < inputStream.connectIDs.size(); i++) {
			jobGraphBuilder.setBatchSize(inputStream.connectIDs.get(i),
					inputStream.batchSizes.get(i));
		}
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

	

	// --------------------------------------------------------------------------------------------
	// Instantiation of Execution Contexts
	// --------------------------------------------------------------------------------------------

	public static LocalStreamEnvironment createLocalEnvironment() {
		return createLocalEnvironment(defaultLocalDop);
	}

	public static LocalStreamEnvironment createLocalEnvironment(int degreeOfParallelism) {
		LocalStreamEnvironment lee = new LocalStreamEnvironment();
		lee.setDegreeOfParallelism(degreeOfParallelism);
		return lee;
	}

	public static ExecutionEnvironment createRemoteEnvironment(String host, int port,
			String... jarFiles) {
		return new RemoteEnvironment(host, port, jarFiles);
	}

	public static StreamExecutionEnvironment createRemoteEnvironment(String host, int port,
			int degreeOfParallelism, String... jarFiles) {
		RemoteStreamEnvironment rec = new RemoteStreamEnvironment(host, port, jarFiles);
		rec.setDegreeOfParallelism(degreeOfParallelism);
		return rec;
	}

	/**
	 * Executes the JobGraph.
	 **/
	public abstract void execute();

	// TODO: Add link to JobGraphBuilder
	/**
	 * Getter of the JobGraphBuilder of the streaming job.
	 * 
	 * @return jobgraph
	 */
	public JobGraphBuilder jobGB() {
		return jobGraphBuilder;
	}
}
