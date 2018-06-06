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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.python.api.datastream.PythonDataStream;
import org.apache.flink.streaming.python.api.functions.PythonGeneratorFunction;
import org.apache.flink.streaming.python.api.functions.PythonIteratorFunction;
import org.apache.flink.streaming.python.util.AdapterMap;
import org.apache.flink.streaming.python.util.serialization.PyBooleanSerializer;
import org.apache.flink.streaming.python.util.serialization.PyFloatSerializer;
import org.apache.flink.streaming.python.util.serialization.PyIntegerSerializer;
import org.apache.flink.streaming.python.util.serialization.PyLongSerializer;
import org.apache.flink.streaming.python.util.serialization.PyObjectSerializer;
import org.apache.flink.streaming.python.util.serialization.PyStringSerializer;

import org.python.core.PyBoolean;
import org.python.core.PyFloat;
import org.python.core.PyInstance;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PyObject;
import org.python.core.PyObjectDerived;
import org.python.core.PyString;
import org.python.core.PyTuple;
import org.python.core.PyUnicode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
@PublicEvolving
public class PythonStreamExecutionEnvironment {
	private static final Logger LOG = LoggerFactory.getLogger(PythonStreamExecutionEnvironment.class);
	private final StreamExecutionEnvironment env;
	private final Path pythonTmpCachePath;

	PythonStreamExecutionEnvironment(StreamExecutionEnvironment env, Path tmpLocalDir, String scriptName) {
		this.env = env;
		this.pythonTmpCachePath = tmpLocalDir;
		env.getConfig().setGlobalJobParameters(new PythonJobParameters(scriptName));
		registerJythonSerializers(this.env);
	}

	/**
	 * Utility class for storing/retrieving parameters in/from a {@link ExecutionConfig.GlobalJobParameters}.
	 */
	public static class PythonJobParameters extends ExecutionConfig.GlobalJobParameters {
		private static final String KEY_SCRIPT_NAME = "scriptName";
		private Map<String, String> parameters = new HashMap<>();

		PythonJobParameters(String scriptName) {
			parameters.put(KEY_SCRIPT_NAME, scriptName);
		}

		public Map<String, String> toMap() {
			return parameters;
		}

		public static String getScriptName(ExecutionConfig.GlobalJobParameters parameters) {
			return parameters.toMap().get(KEY_SCRIPT_NAME);
		}
	}

	private static void registerJythonSerializers(StreamExecutionEnvironment env) {
		env.registerTypeWithKryoSerializer(PyBoolean.class, PyBooleanSerializer.class);
		env.registerTypeWithKryoSerializer(PyFloat.class, PyFloatSerializer.class);
		env.registerTypeWithKryoSerializer(PyInteger.class, PyIntegerSerializer.class);
		env.registerTypeWithKryoSerializer(PyLong.class, PyLongSerializer.class);

		env.registerTypeWithKryoSerializer(PyString.class, PyStringSerializer.class);
		env.registerTypeWithKryoSerializer(PyUnicode.class, PyObjectSerializer.class);

		env.registerTypeWithKryoSerializer(PyTuple.class, PyObjectSerializer.class);
		env.registerTypeWithKryoSerializer(PyObjectDerived.class, PyObjectSerializer.class);
		env.registerTypeWithKryoSerializer(PyInstance.class, PyObjectSerializer.class);
	}

	public PythonDataStream create_python_source(SourceFunction<Object> src) throws Exception {
		return new PythonDataStream<>(env.addSource(new PythonGeneratorFunction(src)).map(new AdapterMap<>()));
	}

	/**
	 * Add a java source to the streaming topology. The source expected to be an java based
	 * implementation (.e.g. Kafka connector).
	 *
	 * @param src A native java source (e.g. PythonFlinkKafkaConsumer09)
	 * @return Python data stream
	 */
	public PythonDataStream add_java_source(SourceFunction<Object> src) {
		return new PythonDataStream<>(env.addSource(src).map(new AdapterMap<>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#fromElements(java.lang.Object[])}.
	 *
	 * @param elements The array of PyObject elements to create the data stream from.
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
	 * @param collection The collection of python elements to create the data stream from.
	 * @return The data stream representing the given collection
	 */
	public PythonDataStream from_collection(Collection<Object> collection) {
		return new PythonDataStream<>(env.fromCollection(collection).map(new AdapterMap<>()));
	}

	/**
	 * Creates a python data stream from the given iterator.
	 *
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e.,
	 * a data stream source with a parallelism of one.</p>
	 *
	 * @param iter The iterator of elements to create the data stream from
	 * @return The data stream representing the elements in the iterator
	 * @see StreamExecutionEnvironment#fromCollection(java.util.Iterator, org.apache.flink.api.common.typeinfo.TypeInformation)
	 */
	public PythonDataStream from_collection(Iterator<Object> iter) throws Exception {
		return new PythonDataStream<>(env.addSource(new PythonIteratorFunction(iter), TypeExtractor.getForClass(Object.class))
			.map(new AdapterMap<>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#generateSequence(long, long)}.
	 *
	 * @param from The number to start at (inclusive)
	 * @param to The number to stop at (inclusive)
	 * @return A python data stream, containing all number in the [from, to] interval
	 */
	public PythonDataStream generate_sequence(long from, long to) {
		return new PythonDataStream<>(env.generateSequence(from, to).map(new AdapterMap<>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#readTextFile(java.lang.String)}.
	 *
	 * @param path The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return The data stream that represents the data read from the given file as text lines
	 * @throws IOException
	 */

	public PythonDataStream read_text_file(String path) throws IOException {
		return new PythonDataStream<>(env.readTextFile(path).map(new AdapterMap<String>()));
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#socketTextStream(java.lang.String, int)}.
	 *
	 * @param host The host name which a server socket binds
	 * @param port The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * allocated.
	 * @return A python data stream containing the strings received from the socket
	 */
	public PythonDataStream socket_text_stream(String host, int port) {
		return new PythonDataStream<>(env.socketTextStream(host, port).map(new AdapterMap<String>()));
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
	 * @param mode The checkpointing mode, selecting between "exactly once" and "at least once" guaranteed.
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
		distributeFiles();
		JobExecutionResult result = this.env.execute();
		return result;
	}

	/**
	 * A thin wrapper layer over {@link StreamExecutionEnvironment#execute(java.lang.String)}.
	 *
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception which occurs during job execution.
	 */
	public JobExecutionResult execute(String job_name) throws Exception {
		distributeFiles();
		JobExecutionResult result = this.env.execute(job_name);
		return result;
	}

	private void distributeFiles() throws IOException {
		this.env.registerCachedFile(pythonTmpCachePath.getPath(), PythonConstants.FLINK_PYTHON_DC_ID);
	}

}
