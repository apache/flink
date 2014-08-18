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

package org.apache.flink.streaming.api.environment;

import java.io.Serializable;
import java.util.Collection;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.function.source.FileSourceFunction;
import org.apache.flink.streaming.api.function.source.FileStreamFunction;
import org.apache.flink.streaming.api.function.source.FromElementsFunction;
import org.apache.flink.streaming.api.function.source.GenSequenceFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;
import org.apache.flink.streaming.util.serialization.ObjectTypeWrapper;

/**
 * {@link ExecutionEnvironment} for streaming jobs. An instance of it is
 * necessary to construct streaming topologies.
 * 
 */
public abstract class StreamExecutionEnvironment {

	/**
	 * The environment of the context (local by default, cluster if invoked
	 * through command line)
	 */
	private static StreamExecutionEnvironment contextEnvironment;

	/** flag to disable local executor when using the ContextEnvironment */
	private static boolean allowLocalExecution = true;

	private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();

	private int degreeOfParallelism = 1;

	private int executionParallelism = -1;

	private long buffertimeout = 0;;

	protected JobGraphBuilder jobGraphBuilder;

	// --------------------------------------------------------------------------------------------
	// Constructor and Properties
	// --------------------------------------------------------------------------------------------

	/**
	 * Constructor for creating StreamExecutionEnvironment
	 */
	protected StreamExecutionEnvironment() {
		jobGraphBuilder = new JobGraphBuilder("jobGraph");
	}

	public int getExecutionParallelism() {
		return executionParallelism == -1 ? degreeOfParallelism : executionParallelism;
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
	protected void setDegreeOfParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1) {
			throw new IllegalArgumentException("Degree of parallelism must be at least one.");
		}
		this.degreeOfParallelism = degreeOfParallelism;
	}

	/**
	 * Sets the maximum time frequency (ms) for the flushing of the output
	 * buffers. By default the output buffers flush only when they are full.
	 * 
	 * @param timeoutMillis
	 *            The maximum time between two output flushes.
	 */
	public void setBufferTimeout(long timeoutMillis) {
		this.buffertimeout = timeoutMillis;
	}

	public long getBufferTimeout() {
		return this.buffertimeout;
	}

	/**
	 * Sets the number of hardware contexts (CPU cores / threads) used when
	 * executed in {@link LocalStreamEnvironment}.
	 * 
	 * @param degreeOfParallelism
	 *            The degree of parallelism in local environment
	 */
	public void setExecutionParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1) {
			throw new IllegalArgumentException("Degree of parallelism must be at least one.");
		}

		this.executionParallelism = degreeOfParallelism;
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
		return addSource(new FileSourceFunction(filePath), 1);
	}

	public DataStreamSource<String> readTextFile(String filePath, int parallelism) {
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
	public DataStreamSource<String> readTextStream(String filePath) {
		return addSource(new FileStreamFunction(filePath), 1);
	}

	public DataStreamSource<String> readTextStream(String filePath, int parallelism) {
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
	 * @param <OUT>
	 *            type of the returned stream
	 * @return The DataStream representing the elements.
	 */
	public <OUT extends Serializable> DataStreamSource<OUT> fromElements(OUT... data) {
		DataStreamSource<OUT> returnStream = new DataStreamSource<OUT>(this, "elements");

		try {
			SourceFunction<OUT> function = new FromElementsFunction<OUT>(data);
			jobGraphBuilder.addSource(returnStream.getId(), new SourceInvokable<OUT>(function),
					new ObjectTypeWrapper<OUT, Tuple, OUT>(data[0], null, data[0]), "source",
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
		DataStreamSource<OUT> returnStream = new DataStreamSource<OUT>(this, "elements");

		if (data.isEmpty()) {
			throw new RuntimeException("Collection must not be empty");
		}

		try {
			SourceFunction<OUT> function = new FromElementsFunction<OUT>(data);

			jobGraphBuilder.addSource(returnStream.getId(), new SourceInvokable<OUT>(
					new FromElementsFunction<OUT>(data)), new ObjectTypeWrapper<OUT, Tuple, OUT>(
					data.iterator().next(), null, data.iterator().next()), "source",
					SerializationUtils.serialize(function), 1);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize collection");
		}

		return returnStream;
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
		return addSource(new GenSequenceFunction(from, to), 1);
	}

	/**
	 * Ads a data source thus opening a {@link DataStream}.
	 * 
	 * @param function
	 *            the user defined function
	 * @param parallelism
	 *            number of parallel instances of the function
	 * @param <OUT>
	 *            type of the returned stream
	 * @return the data stream constructed
	 */
	public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, int parallelism) {
		DataStreamSource<OUT> returnStream = new DataStreamSource<OUT>(this, "source");

		try {
			jobGraphBuilder.addSource(returnStream.getId(), new SourceInvokable<OUT>(function),
					new FunctionTypeWrapper<OUT, Tuple, OUT>(function, SourceFunction.class, 0, -1,
							0), "source", SerializationUtils.serialize(function), parallelism);
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize SourceFunction");
		}

		return returnStream;
	}

	public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> sourceFunction) {
		return addSource(sourceFunction, 1);
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
		return contextEnvironment == null ? createLocalEnvironment() : contextEnvironment;
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

	// --------------------------------------------------------------------------------------------
	// Methods to control the context and local environments for execution from
	// packaged programs
	// --------------------------------------------------------------------------------------------

	protected static void initializeContextEnvironment(StreamExecutionEnvironment ctx) {
		contextEnvironment = ctx;
	}

	protected static boolean isContextEnvironmentSet() {
		return contextEnvironment != null;
	}

	protected static void disableLocalExecution() {
		allowLocalExecution = false;
	}

	public static boolean localExecutionIsAllowed() {
		return allowLocalExecution;
	}

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 * <p>
	 * The program execution will be logged and displayed with a generated
	 * default name.
	 **/
	public abstract void execute();

	/**
	 * Getter of the {@link JobGraphBuilder} of the streaming job.
	 * 
	 * @return jobgraph
	 */
	public JobGraphBuilder getJobGraphBuilder() {
		return jobGraphBuilder;
	}

}
