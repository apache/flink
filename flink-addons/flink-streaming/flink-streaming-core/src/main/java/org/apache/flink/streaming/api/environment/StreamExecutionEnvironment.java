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

package org.apache.flink.streaming.api.environment;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.function.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.function.source.FileSourceFunction;
import org.apache.flink.streaming.api.function.source.FileStreamFunction;
import org.apache.flink.streaming.api.function.source.FromElementsFunction;
import org.apache.flink.streaming.api.function.source.GenSequenceFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;
import org.apache.flink.streaming.util.serialization.ObjectTypeWrapper;
import org.apache.flink.streaming.util.serialization.TypeWrapper;

/**
 * {@link ExecutionEnvironment} for streaming jobs. An instance of it is
 * necessary to construct streaming topologies.
 * 
 */
public abstract class StreamExecutionEnvironment {

	private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();

	private int degreeOfParallelism = 1;

	private long bufferTimeout = 100;

	protected JobGraphBuilder jobGraphBuilder;

	// --------------------------------------------------------------------------------------------
	// Constructor and Properties
	// --------------------------------------------------------------------------------------------

	/**
	 * Constructor for creating StreamExecutionEnvironment
	 */
	protected StreamExecutionEnvironment() {
		jobGraphBuilder = new JobGraphBuilder();
	}

	/**
	 * Gets the degree of parallelism with which operation are executed by
	 * default. Operations can individually override this value to use a
	 * specific degree of parallelism via {@link DataStream#setParallelism}.
	 * 
	 * @return The degree of parallelism used by operations, unless they
	 *         override that value.
	 */
	public int getDegreeOfParallelism() {
		return this.degreeOfParallelism;
	}

	/**
	 * Sets the degree of parallelism (DOP) for operations executed through this
	 * environment. Setting a DOP of x here will cause all operators (such as
	 * map, batchReduce) to run with x parallel instances. This method overrides
	 * the default parallelism for this environment. The
	 * {@link LocalStreamEnvironment} uses by default a value equal to the
	 * number of hardware contexts (CPU cores / threads). When executing the
	 * program via the command line client from a JAR file, the default degree
	 * of parallelism is the one configured for that setup.
	 * 
	 * @param degreeOfParallelism
	 *            The degree of parallelism
	 */
	public StreamExecutionEnvironment setDegreeOfParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1) {
			throw new IllegalArgumentException("Degree of parallelism must be at least one.");
		}
		this.degreeOfParallelism = degreeOfParallelism;
		return this;
	}

	/**
	 * Sets the maximum time frequency (milliseconds) for the flushing of the
	 * output buffers. By default the output buffers flush frequently to provide
	 * low latency and to aid smooth developer experience. Setting the parameter
	 * can result in three logical modes:
	 * 
	 * <ul>
	 * <li>
	 * A positive integer triggers flushing periodically by that integer</li>
	 * <li>
	 * 0 triggers flushing after every record thus minimizing latency</li>
	 * <li>
	 * -1 triggers flushing only when the output buffer is full thus maximizing
	 * throughput</li>
	 * </ul>
	 * 
	 * @param timeoutMillis
	 *            The maximum time between two output flushes.
	 */
	public StreamExecutionEnvironment setBufferTimeout(long timeoutMillis) {
		if (timeoutMillis < -1) {
			throw new IllegalArgumentException("Timeout of buffer must be non-negative or -1");
		}

		this.bufferTimeout = timeoutMillis;
		return this;
	}

	public long getBufferTimeout() {
		return this.bufferTimeout;
	}
	
	/**
	 * Sets the default parallelism that will be used for the local execution environment created by
	 * {@link #createLocalEnvironment()}.
	 * 
	 * @param degreeOfParallelism The degree of parallelism to use as the default local parallelism.
	 */
	public static void setDefaultLocalParallelism(int degreeOfParallelism) {
		defaultLocalDop = degreeOfParallelism;
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
	public DataStreamSource<String> readTextFile(String filePath) {
		checkIfFileExists(filePath);
		return addSource(new FileSourceFunction(filePath));
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
	public DataStreamSource<String> readTextStream(String filePath) {
		checkIfFileExists(filePath);
		return addSource(new FileStreamFunction(filePath));
	}

	private static void checkIfFileExists(String filePath) {
		File file = new File(filePath);
		if (!file.exists()) {
			throw new IllegalArgumentException("File not found: " + filePath);
		}

		if (!file.canRead()) {
			throw new IllegalArgumentException("Cannot read file: " + filePath);
		}

		if (file.isDirectory()) {
			throw new IllegalArgumentException("Given path is a directory: " + filePath);
		}
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
	 * @param <OUT>
	 *            type of the returned stream
	 * @return The DataStream representing the elements.
	 */
	public <OUT extends Serializable> DataStreamSource<OUT> fromElements(OUT... data) {
		if (data.length == 0) {
			throw new IllegalArgumentException(
					"fromElements needs at least one element as argument");
		}

		TypeWrapper<OUT> outTypeWrapper = new ObjectTypeWrapper<OUT>(data[0]);
		DataStreamSource<OUT> returnStream = new DataStreamSource<OUT>(this, "elements",
				outTypeWrapper);

		try {
			SourceFunction<OUT> function = new FromElementsFunction<OUT>(data);
			jobGraphBuilder.addStreamVertex(returnStream.getId(),
					new SourceInvokable<OUT>(function), null, outTypeWrapper, "source",
					SerializationUtils.serialize(function), 1);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize elements");
		}
		return returnStream;
	}

	/**
	 * Creates a DataStream from the given non-empty collection. The type of the
	 * DataStream is that of the elements in the collection. The elements need
	 * to be serializable (as defined by java.io.Serializable), because the
	 * framework may move the elements into the cluster if needed.
	 * 
	 * @param data
	 *            The collection of elements to create the DataStream from.
	 * @param <OUT>
	 *            type of the returned stream
	 * @return The DataStream representing the elements.
	 */
	public <OUT extends Serializable> DataStreamSource<OUT> fromCollection(Collection<OUT> data) {
		if (data == null) {
			throw new NullPointerException("Collection must not be null");
		}

		if (data.isEmpty()) {
			throw new IllegalArgumentException("Collection must not be empty");
		}

		TypeWrapper<OUT> outTypeWrapper = new ObjectTypeWrapper<OUT>(data.iterator().next());
		DataStreamSource<OUT> returnStream = new DataStreamSource<OUT>(this, "elements",
				outTypeWrapper);

		try {
			SourceFunction<OUT> function = new FromElementsFunction<OUT>(data);

			jobGraphBuilder.addStreamVertex(returnStream.getId(), new SourceInvokable<OUT>(
					new FromElementsFunction<OUT>(data)), null, new ObjectTypeWrapper<OUT>(data
					.iterator().next()), "source", SerializationUtils.serialize(function), 1);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize collection");
		}

		return returnStream;
	}

	/**
	 * Creates a new DataStream that contains the strings received infinitely
	 * from socket. Received strings are decoded by the system's default
	 * character set.
	 *
	 * @param hostname
	 *            The host name which a server socket bind.
	 * @param port
	 * 			  The port number which a server socket bind. A port number of
	 * 			  0 means that the port number is automatically allocated.
	 * @param delimiter
	 * 			  A character which split received strings into records.
	 * @return A DataStream, containing the strings received from socket.
	 */
	public DataStreamSource<String> socketTextStream(String hostname, int port, char delimiter) {
		return addSource(new SocketTextStreamFunction(hostname, port, delimiter));
	}
	
	
	/**
	 * Creates a new DataStream that contains the strings received infinitely
	 * from socket. Received strings are decoded by the system's default
	 * character set, uses '\n' as delimiter.
	 *
	 * @param hostname
	 *            The host name which a server socket bind.
	 * @param port
	 * 			  The port number which a server socket bind. A port number of
	 * 			  0 means that the port number is automatically allocated.
	 * @return A DataStream, containing the strings received from socket.
	 */
	public DataStreamSource<String> socketTextStream(String hostname, int port) {
		return socketTextStream(hostname, port, '\n');
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
	public DataStreamSource<Long> generateSequence(long from, long to) {
		if (from > to) {
			throw new IllegalArgumentException("Start of sequence must not be greater than the end");
		}
		return addSource(new GenSequenceFunction(from, to));
	}

	/**
	 * Ads a data source thus opening a {@link DataStream}.
	 * 
	 * @param function
	 *            the user defined function
	 * @param <OUT>
	 *            type of the returned stream
	 * @return the data stream constructed
	 */
	public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function) {
		TypeWrapper<OUT> outTypeWrapper = new FunctionTypeWrapper<OUT>(function,
				SourceFunction.class, 0);
		DataStreamSource<OUT> returnStream = new DataStreamSource<OUT>(this, "source",
				outTypeWrapper);

		try {
			jobGraphBuilder.addStreamVertex(returnStream.getId(),
					new SourceInvokable<OUT>(function), null, outTypeWrapper, "source",
					SerializationUtils.serialize(function), 1);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize SourceFunction");
		}

		return returnStream;
	}

	// --------------------------------------------------------------------------------------------
	// Instantiation of Execution Contexts
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates an execution environment that represents the context in which the
	 * program is currently executed. If the program is invoked standalone, this
	 * method returns a local execution environment, as returned by
	 * {@link #createLocalEnvironment()}.
	 * 
	 * @return The execution environment of the context in which the program is
	 *         executed.
	 */
	public static StreamExecutionEnvironment getExecutionEnvironment() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		if (env instanceof ContextEnvironment) {
			ContextEnvironment ctx = (ContextEnvironment) env;
			return createContextEnvironment(ctx.getClient(), ctx.getJars(),
					ctx.getDegreeOfParallelism());
		} else {
			return createLocalEnvironment();
		}
	}

	private static StreamExecutionEnvironment createContextEnvironment(Client client,
			List<File> jars, int dop) {
		return new StreamContextEnvironment(client, jars, dop);
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. The default degree of parallelism of the
	 * local environment is the number of hardware contexts (CPU cores /
	 * threads), unless it was specified differently by
	 * {@link #setDegreeOfParallelism(int)}.
	 * 
	 * @return A local execution environment.
	 */
	public static LocalStreamEnvironment createLocalEnvironment() {
		return createLocalEnvironment(defaultLocalDop);
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. It will use the degree of parallelism
	 * specified in the parameter.
	 * 
	 * @param degreeOfParallelism
	 *            The degree of parallelism for the local environment.
	 * @return A local execution environment with the specified degree of
	 *         parallelism.
	 */
	public static LocalStreamEnvironment createLocalEnvironment(int degreeOfParallelism) {
		LocalStreamEnvironment lee = new LocalStreamEnvironment();
		lee.setDegreeOfParallelism(degreeOfParallelism);
		return lee;
	}

	// TODO:fix cluster default parallelism
	/**
	 * Creates a {@link RemoteStreamEnvironment}. The remote environment sends
	 * (parts of) the program to a cluster for execution. Note that all file
	 * paths used in the program must be accessible from the cluster. The
	 * execution will use no parallelism, unless the parallelism is set
	 * explicitly via {@link #setDegreeOfParallelism}.
	 * 
	 * @param host
	 *            The host name or address of the master (JobManager), where the
	 *            program should be executed.
	 * @param port
	 *            The port of the master (JobManager), where the program should
	 *            be executed.
	 * @param jarFiles
	 *            The JAR files with code that needs to be shipped to the
	 *            cluster. If the program uses user-defined functions,
	 *            user-defined input formats, or any libraries, those must be
	 *            provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static StreamExecutionEnvironment createRemoteEnvironment(String host, int port,
			String... jarFiles) {
		return new RemoteStreamEnvironment(host, port, jarFiles);
	}

	/**
	 * Creates a {@link RemoteStreamEnvironment}. The remote environment sends
	 * (parts of) the program to a cluster for execution. Note that all file
	 * paths used in the program must be accessible from the cluster. The
	 * execution will use the specified degree of parallelism.
	 * 
	 * @param host
	 *            The host name or address of the master (JobManager), where the
	 *            program should be executed.
	 * @param port
	 *            The port of the master (JobManager), where the program should
	 *            be executed.
	 * @param degreeOfParallelism
	 *            The degree of parallelism to use during the execution.
	 * @param jarFiles
	 *            The JAR files with code that needs to be shipped to the
	 *            cluster. If the program uses user-defined functions,
	 *            user-defined input formats, or any libraries, those must be
	 *            provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static StreamExecutionEnvironment createRemoteEnvironment(String host, int port,
			int degreeOfParallelism, String... jarFiles) {
		RemoteStreamEnvironment rec = new RemoteStreamEnvironment(host, port, jarFiles);
		rec.setDegreeOfParallelism(degreeOfParallelism);
		return rec;
	}

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 * <p>
	 * The program execution will be logged and displayed with a generated
	 * default name.
	 * 
	 * @throws Exception
	 **/
	public abstract void execute() throws Exception;

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 * <p>
	 * The program execution will be logged and displayed with the provided name
	 * 
	 * @param jobName
	 *            Desired name of the job
	 * 
	 * @throws Exception
	 **/
	public abstract void execute(String jobName) throws Exception;

	/**
	 * Getter of the {@link JobGraphBuilder} of the streaming job.
	 * 
	 * @return jobGraphBuilder
	 */
	public JobGraphBuilder getJobGraphBuilder() {
		return jobGraphBuilder;
	}

}
