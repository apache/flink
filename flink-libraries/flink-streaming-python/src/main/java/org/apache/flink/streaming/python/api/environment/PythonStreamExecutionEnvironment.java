/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.python.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.python.api.datastream.PythonDataStream;
import org.apache.flink.streaming.python.api.functions.PythonGeneratorFunction;
import org.apache.flink.streaming.python.api.functions.PythonIteratorFunction;
import org.apache.flink.streaming.python.api.functions.UtilityFunctions;
import org.apache.flink.streaming.python.util.serialization.PyObjectSerializer;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PyUnicode;
import org.python.core.PyTuple;
import org.python.core.PyObjectDerived;
import org.python.core.PyInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;


/**
 * A thin wrapper layer over {@link StreamExecutionEnvironment}.
 *
 * <p>The PythonStreamExecutionEnvironment is the context in which a streaming program is executed.
 * </p>
 *
 * <p>The environment provides methods to control the job execution (such as setting the parallelism
 * or the fault tolerance/checkpointing parameters) and to interact with the outside world
 * (data access).</p>
 */
@Public
public class PythonStreamExecutionEnvironment {
	private final StreamExecutionEnvironment env;
	private static final Logger LOG = LoggerFactory.getLogger(PythonStreamExecutionEnvironment.class);

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#getExecutionEnvironment()}. In addition it takes
	 * care for required Jython serializers registration.
	 *
	 * @return The python execution environment of the context in which the program is
	 * executed.
	 */
	public static PythonStreamExecutionEnvironment get_execution_environment() {
		return new PythonStreamExecutionEnvironment();
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. The default parallelism of the local
	 * environment is the number of hardware contexts (CPU cores / threads),
	 * unless it was specified differently by {@link #setParallelism(int)}.
	 *
	 * @param configuration
	 * 		Pass a custom configuration into the cluster
	 * @return A local execution environment with the specified parallelism.
	 */
	public static PythonStreamExecutionEnvironment create_local_execution_environment(Configuration config) {
		return new PythonStreamExecutionEnvironment(config);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#createLocalEnvironment(int, Configuration)}
	 *
	 * @param parallelism
	 * 		The parallelism for the local environment.
	 * @param config
	 * 		Pass a custom configuration into the cluster
	 * @return A local python execution environment with the specified parallelism.
	 */
	public static PythonStreamExecutionEnvironment create_local_execution_environment(int parallelism, Configuration config) {
		return new PythonStreamExecutionEnvironment(parallelism, config);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#createRemoteEnvironment(java.lang.String, int, java.lang.String...)}
	 *
	 * @param host
	 * 		The host name or address of the master (JobManager), where the
	 * 		program should be executed.
	 * @param port
	 * 		The port of the master (JobManager), where the program should
	 * 		be executed.
	 * @param jar_files
	 * 		The JAR files with code that needs to be shipped to the
	 * 		cluster. If the program uses user-defined functions,
	 * 		user-defined input formats, or any libraries, those must be
	 * 		provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, String... jar_files) {
		return new PythonStreamExecutionEnvironment(host, port, jar_files);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#createRemoteEnvironment(
	 * java.lang.String, int, Configuration, java.lang.String...)}
	 *
	 * @param host
	 * 		The host name or address of the master (JobManager), where the
	 * 		program should be executed.
	 * @param port
	 * 		The port of the master (JobManager), where the program should
	 * 		be executed.
	 * @param config
	 * 		The configuration used by the client that connects to the remote cluster.
	 * @param jar_files
	 * 		The JAR files with code that needs to be shipped to the
	 * 		cluster. If the program uses user-defined functions,
	 * 		user-defined input formats, or any libraries, those must be
	 * 		provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 *
	 */
	public static PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, Configuration config, String... jar_files) {
		return new PythonStreamExecutionEnvironment(host, port, config, jar_files);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#createRemoteEnvironment(
	 * java.lang.String, int, int, java.lang.String...)}
	 *
	 * @param host
	 * 		The host name or address of the master (JobManager), where the
	 * 		program should be executed.
	 * @param port
	 * 		The port of the master (JobManager), where the program should
	 * 		be executed.
	 * @param parallelism
	 * 		The parallelism to use during the execution.
	 * @param jar_files
	 * 		The JAR files with code that needs to be shipped to the
	 * 		cluster. If the program uses user-defined functions,
	 * 		user-defined input formats, or any libraries, those must be
	 * 		provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, int parallelism, String... jar_files) {
		return new PythonStreamExecutionEnvironment(host, port, parallelism, jar_files);
	}

	private PythonStreamExecutionEnvironment() {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(Configuration config) {
		this.env = new LocalStreamEnvironment(config);
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(int parallelism, Configuration config) {
		this.env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, config);
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(String host, int port, String... jar_files) {
		this.env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, jar_files);
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(String host, int port, Configuration config, String... jar_files) {
		this.env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, config, jar_files);
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(String host, int port, int parallelism, String... jar_files) {
		this.env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, parallelism, jar_files);
		this.registerJythonSerializers();
	}

	private void registerJythonSerializers() {
		this.env.registerTypeWithKryoSerializer(PyString.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyInteger.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyLong.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyUnicode.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyTuple.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyObjectDerived.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyInstance.class, PyObjectSerializer.class);
	}

	public PythonDataStream create_python_source(SourceFunction<Object> src) throws Exception {
		return new PythonDataStream<>(env.addSource(new PythonGeneratorFunction(src)).map(new UtilityFunctions.SerializerMap<>()));
	}

	/**
	 * Add a java source to the streaming topology. The source expected to be an java based
	 * implementation (.e.g. Kafka connector).
	 *
	 * @param src  A native java source (e.g. PythonFlinkKafkaConsumer09)
	 * @return Python data stream
	 */
	public PythonDataStream add_java_source(SourceFunction<Object> src) {
		return new PythonDataStream<>(env.addSource(src).map(new UtilityFunctions.SerializerMap<>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#fromElements(java.lang.Object[])}
	 *
	 * @param elements
	 * 		The array of PyObject elements to create the data stream from.
	 * @return The data stream representing the given array of elements
	 */
	public PythonDataStream from_elements(PyObject... elements) {
		return new PythonDataStream<>(env.fromElements(elements));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#fromCollection(java.util.Collection)}
	 *
	 * <p>The input {@code Collection} is of type {@code Object}, because it is a collection
	 * of Python elements. * There type is determined in runtime, by the Jython framework.</p>
	 *
	 * @param collection
	 * 		The collection of python elements to create the data stream from.
	 * @return
	 *     The data stream representing the given collection
	 */
	public PythonDataStream from_collection(Collection<Object> collection) {
		return new PythonDataStream<>(env.fromCollection(collection).map(new UtilityFunctions.SerializerMap<>()));
	}

	/**
	 * Creates a python data stream from the given iterator.
	 *
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e.,
	 * a data stream source with a parallelism of one.</p>
	 *
	 * @param iter
	 * 		The iterator of elements to create the data stream from
	 * @return The data stream representing the elements in the iterator
	 * @see StreamExecutionEnvironment#fromCollection(java.util.Iterator, org.apache.flink.api.common.typeinfo.TypeInformation)
	 */
	public PythonDataStream from_collection(Iterator<Object> iter) throws Exception  {
		return new PythonDataStream<>(env.addSource(new PythonIteratorFunction(iter), TypeExtractor.getForClass(Object.class))
			.map(new UtilityFunctions.SerializerMap<>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#generateSequence(long, long)}.
	 *
	 * @param from
	 * 		The number to start at (inclusive)
	 * @param to
	 * 		The number to stop at (inclusive)
	 * @return A python data stream, containing all number in the [from, to] interval
	 */
	public PythonDataStream generate_sequence(long from, long to) {
		return new PythonDataStream<>(env.generateSequence(from, to).map(new UtilityFunctions.SerializerMap<Long>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#readTextFile(java.lang.String)}.
	 *
	 * @param path
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return The data stream that represents the data read from the given file as text lines
	 * @throws IOException
	 */

	public PythonDataStream read_text_file(String path) throws IOException {
		return new PythonDataStream<>(env.readTextFile(path).map(new UtilityFunctions.SerializerMap<String>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#socketTextStream(java.lang.String, int)}.
	 *
	 * @param host
	 * 		The host name which a server socket binds
	 * @param port
	 * 		The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * 		allocated.
	 * @return A python data stream containing the strings received from the socket
	 */
	public PythonDataStream socket_text_stream(String host, int port) {
		return new PythonDataStream<>(env.socketTextStream(host, port).map(new UtilityFunctions.SerializerMap<String>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#enableCheckpointing(long)}.
	 *
	 * @param interval Time interval between state checkpoints in milliseconds.
	 * @return The same {@code PythonStreamExecutionEnvironment} instance of the caller
	 */
	public PythonStreamExecutionEnvironment enable_checkpointing(long interval) {
		this.env.enableCheckpointing(interval);
		return this;
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#enableCheckpointing(long, CheckpointingMode)}.
	 *
	 * @param interval Time interval between state checkpoints in milliseconds.
	 * @param mode
	 *             The checkpointing mode, selecting between "exactly once" and "at least once" guaranteed.
	 * @return The same {@code PythonStreamExecutionEnvironment} instance of the caller
	 */
	public PythonStreamExecutionEnvironment enable_checkpointing(long interval, CheckpointingMode mode) {
		this.env.enableCheckpointing(interval, mode);
		return this;
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#setParallelism(int)}.
	 *
	 * @param parallelism The parallelism
	 * @return The same {@code PythonStreamExecutionEnvironment} instance of the caller
	 */
	public PythonStreamExecutionEnvironment set_parallelism(int parallelism) {
		this.env.setParallelism(parallelism);
		return this;
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#execute()}.
	 *
	 * @return The result of the job execution
	 */
	public JobExecutionResult execute() throws Exception {
		return execute(false);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#execute()}.
	 *
	 * <p>In addition, it enables the caller to provide a hint about the execution mode - whether it is local
	 * or remote. In the case of local execution, the relevant cached files are distributed using the
	 * local machine temporary folder, otherwise a shared storage medium is used for this purpose.</p>
	 *
	 * @return The result of the job execution
	 */
	public JobExecutionResult execute(Boolean local) throws Exception {
		if (PythonEnvironmentConfig.pythonTmpCachePath == null) {
			// Nothing to be done! Is is executed on the task manager.
			return new JobExecutionResult(null, 0, null);
		}
		distributeFiles(local);
		JobExecutionResult result = this.env.execute();
		cleanupDistributedFiles();
		return result;
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#execute(java.lang.String)}.
	 *
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception which occurs during job execution.
	 */
	public JobExecutionResult execute(String job_name) throws Exception {
		return execute(job_name, false);
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#execute(java.lang.String).
	 *
	 * <p>In addition, it enables the caller to provide a hint about the execution mode - whether it is local
	 * or remote. In the case of local execution, the relevant cached files are distributed using the
	 * local machine temporary folder, otherwise a shared storage medium is used for this purpose.</p>
	 *
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception which occurs during job execution.
	 */
	public JobExecutionResult execute(String job_name, Boolean local) throws Exception {
		if (PythonEnvironmentConfig.pythonTmpCachePath == null) {
			// Nothing to be done! Is is executed on the task manager.
			return new JobExecutionResult(null, 0, null);
		}
		distributeFiles(local);
		JobExecutionResult result = this.env.execute(job_name);
		cleanupDistributedFiles();
		return result;
	}

	private void distributeFiles(boolean local) throws IOException, URISyntaxException
	{
		String rootDir;
		if (local || this.env instanceof LocalStreamEnvironment) {
			rootDir = System.getProperty("java.io.tmpdir");
		} else {
			rootDir = "hdfs:///tmp";
		}
		PythonEnvironmentConfig.FLINK_HDFS_PATH = Paths.get(rootDir, "flink_cache_" +
			(new Random(System.currentTimeMillis())).nextLong()).toString();

		FileCache.copy(new Path(PythonEnvironmentConfig.pythonTmpCachePath),
			new Path(PythonEnvironmentConfig.FLINK_HDFS_PATH), true);

		this.env.registerCachedFile(PythonEnvironmentConfig.FLINK_HDFS_PATH, PythonEnvironmentConfig.FLINK_PYTHON_DC_ID);
	}

	private void cleanupDistributedFiles() throws IOException, URISyntaxException {
		for (Tuple2<String, DistributedCache.DistributedCacheEntry> e : this.env.getCachedFiles()) {
			URI fileUri = new URI(e.f1.filePath);
			FileSystem fs = FileSystem.get(fileUri);
			LOG.debug(String.format("Cleaning up cached path: %s, uriPath: %s, fileSystem: %s",
				e.f1.filePath,
				fileUri.getPath(),
				fs.getClass().getName()));
			fs.delete(new Path(fileUri.getPath()), true);
		}
	}
}
