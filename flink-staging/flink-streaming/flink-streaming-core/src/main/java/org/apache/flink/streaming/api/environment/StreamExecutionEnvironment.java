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

import com.esotericsoftware.kryo.Serializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrimitiveInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextValueInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PreviewPlanEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FileStateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType;
import org.apache.flink.streaming.api.functions.source.FileReadFunction;
import org.apache.flink.streaming.api.functions.source.FileSourceFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.streaming.api.functions.source.FromSplittableIteratorFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.SplittableIterator;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * {@link org.apache.flink.api.java.ExecutionEnvironment} for streaming jobs. An instance of it is
 * necessary to construct streaming topologies.
 */
public abstract class StreamExecutionEnvironment {

	public final static String DEFAULT_JOB_NAME = "Flink Streaming Job";

	private static int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();

	private long bufferTimeout = 100;

	private ExecutionConfig config = new ExecutionConfig();

	protected List<StreamTransformation<?>> transformations = Lists.newArrayList();

	protected boolean isChainingEnabled = true;

	protected long checkpointInterval = -1; // disabled

	protected CheckpointingMode checkpointingMode = null;

	protected boolean forceCheckpointing = false;

	protected StateHandleProvider<?> stateHandleProvider;

	/** The environment of the context (local by default, cluster if invoked through command line) */
	private static StreamExecutionEnvironmentFactory contextEnvironmentFactory;

	// --------------------------------------------------------------------------------------------
	// Constructor and Properties
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the config object.
	 */
	public ExecutionConfig getConfig() {
		return config;
	}

	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as map,
	 * batchReduce) to run with x parallel instances. This method overrides the
	 * default parallelism for this environment. The
	 * {@link LocalStreamEnvironment} uses by default a value equal to the
	 * number of hardware contexts (CPU cores / threads). When executing the
	 * program via the command line client from a JAR file, the default degree
	 * of parallelism is the one configured for that setup.
	 *
	 * @param parallelism
	 * 		The parallelism
	 * @deprecated Please use {@link #setParallelism}
	 */
	@Deprecated
	public StreamExecutionEnvironment setDegreeOfParallelism(int parallelism) {
		return setParallelism(parallelism);
	}

	/**
	 * Gets the parallelism with which operation are executed by default.
	 * Operations can individually override this value to use a specific
	 * parallelism.
	 *
	 * @return The parallelism used by operations, unless they override that
	 * value.
	 * @deprecated Please use {@link #getParallelism}
	 */
	@Deprecated
	public int getDegreeOfParallelism() {
		return getParallelism();
	}

	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as map,
	 * batchReduce) to run with x parallel instances. This method overrides the
	 * default parallelism for this environment. The
	 * {@link LocalStreamEnvironment} uses by default a value equal to the
	 * number of hardware contexts (CPU cores / threads). When executing the
	 * program via the command line client from a JAR file, the default degree
	 * of parallelism is the one configured for that setup.
	 *
	 * @param parallelism
	 * 		The parallelism
	 */
	public StreamExecutionEnvironment setParallelism(int parallelism) {
		if (parallelism < 1) {
			throw new IllegalArgumentException("parallelism must be at least one.");
		}
		config.setParallelism(parallelism);
		return this;
	}

	/**
	 * Gets the parallelism with which operation are executed by default.
	 * Operations can individually override this value to use a specific
	 * parallelism.
	 *
	 * @return The parallelism used by operations, unless they override that
	 * value.
	 */
	public int getParallelism() {
		return config.getParallelism();
	}

	/**
	 * Sets the maximum time frequency (milliseconds) for the flushing of the
	 * output buffers. By default the output buffers flush frequently to provide
	 * low latency and to aid smooth developer experience. Setting the parameter
	 * can result in three logical modes:
	 * <p/>
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
	 * 		The maximum time between two output flushes.
	 */
	public StreamExecutionEnvironment setBufferTimeout(long timeoutMillis) {
		if (timeoutMillis < -1) {
			throw new IllegalArgumentException("Timeout of buffer must be non-negative or -1");
		}

		this.bufferTimeout = timeoutMillis;
		return this;
	}

	/**
	 * Sets the maximum time frequency (milliseconds) for the flushing of the
	 * output buffers. For clarification on the extremal values see
	 * {@link #setBufferTimeout(long)}.
	 *
	 * @return The timeout of the buffer.
	 */
	public long getBufferTimeout() {
		return this.bufferTimeout;
	}

	/**
	 * Disables operator chaining for streaming operators. Operator chaining
	 * allows non-shuffle operations to be co-located in the same thread fully
	 * avoiding serialization and de-serialization.
	 *
	 * @return StreamExecutionEnvironment with chaining disabled.
	 */
	public StreamExecutionEnvironment disableOperatorChaining() {
		this.isChainingEnabled = false;
		return this;
	}

	/**
	 * Returns whether operator chaining is enabled.
	 *
	 * @return {@code true} if chaining is enabled, false otherwise.
	 */
	public boolean isChainingEnabled() {
		return isChainingEnabled;
	}

	// ------------------------------------------------------------------------
	//  Checkpointing Settings
	// ------------------------------------------------------------------------
	
	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint. This method selects
	 * {@link CheckpointingMode#EXACTLY_ONCE} guarantees.
	 * 
	 * <p>The job draws checkpoints periodically, in the given interval. The state will be
	 * stored in the configured state backend.</p>
	 * 
	 * <p>NOTE: Checkpointing iterative streaming dataflows in not properly supported at
	 * the moment. For that reason, iterative jobs will not be started if used
	 * with enabled checkpointing. To override this mechanism, use the 
	 * {@link #enableCheckpointing(long, CheckpointingMode, boolean)} method.</p>
	 *
	 * @param interval Time interval between state checkpoints in milliseconds.
	 */
	public StreamExecutionEnvironment enableCheckpointing(long interval) {
		return enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE);
	}

	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint.
	 *
	 * <p>The job draws checkpoints periodically, in the given interval. The system uses the
	 * given {@link CheckpointingMode} for the checkpointing ("exactly once" vs "at least once").
	 * The state will be stored in the configured state backend.</p>
	 *
	 * <p>NOTE: Checkpointing iterative streaming dataflows in not properly supported at
	 * the moment. For that reason, iterative jobs will not be started if used
	 * with enabled checkpointing. To override this mechanism, use the 
	 * {@link #enableCheckpointing(long, CheckpointingMode, boolean)} method.</p>
	 *
	 * @param interval 
	 *             Time interval between state checkpoints in milliseconds.
	 * @param mode 
	 *             The checkpointing mode, selecting between "exactly once" and "at least once" guaranteed.
	 */
	public StreamExecutionEnvironment enableCheckpointing(long interval, CheckpointingMode mode) {
		if (mode == null) {
			throw new NullPointerException("checkpoint mode must not be null");
		}
		if (interval <= 0) {
			throw new IllegalArgumentException("the checkpoint interval must be positive");
		}

		this.checkpointInterval = interval;
		this.checkpointingMode = mode;
		return this;
	}
	
	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint.
	 *
	 * <p>The job draws checkpoints periodically, in the given interval. The state will be
	 * stored in the configured state backend.</p>
	 *
	 * <p>NOTE: Checkpointing iterative streaming dataflows in not properly supported at
	 * the moment. If the "force" parameter is set to true, the system will execute the
	 * job nonetheless.</p>
	 * 
	 * @param interval
	 *            Time interval between state checkpoints in millis.
	 * @param mode
	 *            The checkpointing mode, selecting between "exactly once" and "at least once" guaranteed.
	 * @param force
	 *            If true checkpointing will be enabled for iterative jobs as well.
	 */
	@Deprecated
	public StreamExecutionEnvironment enableCheckpointing(long interval, CheckpointingMode mode, boolean force) {
		this.enableCheckpointing(interval, mode);

		this.forceCheckpointing = force;
		return this;
	}

	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint. This method selects
	 * {@link CheckpointingMode#EXACTLY_ONCE} guarantees.
	 *
	 * <p>The job draws checkpoints periodically, in the default interval. The state will be
	 * stored in the configured state backend.</p>
	 *
	 * <p>NOTE: Checkpointing iterative streaming dataflows in not properly supported at
	 * the moment. For that reason, iterative jobs will not be started if used
	 * with enabled checkpointing. To override this mechanism, use the 
	 * {@link #enableCheckpointing(long, CheckpointingMode, boolean)} method.</p>
	 */
	public StreamExecutionEnvironment enableCheckpointing() {
		enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		return this;
	}

	/**
	 * Returns the checkpointing interval or -1 if checkpointing is disabled.
	 *
	 * @return The checkpointing interval or -1
	 */
	public long getCheckpointInterval() {
		return checkpointInterval;
	}


	/**
	 * Returns whether checkpointing is force-enabled.
	 */
	public boolean isForceCheckpointing() {
		return forceCheckpointing;
	}

	/**
	 * Returns the {@link CheckpointingMode}.
	 */
	public CheckpointingMode getCheckpointingMode() {
		return checkpointingMode;
	}

	/**
	 * Sets the {@link StateHandleProvider} used for storing operator state
	 * checkpoints when checkpointing is enabled.
	 * <p>
	 * An example would be using a {@link FileStateHandle#createProvider(String)}
	 * to use any Flink supported file system as a state backend
	 * 
	 */
	public StreamExecutionEnvironment setStateHandleProvider(StateHandleProvider<?> provider) {
		this.stateHandleProvider = provider;
		return this;
	}

	/**
	 * Returns the {@link org.apache.flink.runtime.state.StateHandle}
	 *
	 * @see #setStateHandleProvider(org.apache.flink.runtime.state.StateHandleProvider)
	 *
	 * @return The StateHandleProvider
	 */
	public StateHandleProvider<?> getStateHandleProvider() {
		return stateHandleProvider;
	}

	/**
	 * Sets the number of times that failed tasks are re-executed. A value of
	 * zero effectively disables fault tolerance. A value of {@code -1}
	 * indicates that the system default value (as defined in the configuration)
	 * should be used.
	 *
	 * @param numberOfExecutionRetries
	 * 		The number of times the system will try to re-execute failed
	 * 		tasks.
	 */
	public void setNumberOfExecutionRetries(int numberOfExecutionRetries) {
		config.setNumberOfExecutionRetries(numberOfExecutionRetries);
	}

	/**
	 * Gets the number of times the system will try to re-execute failed tasks.
	 * A value of {@code -1} indicates that the system default value (as defined
	 * in the configuration) should be used.
	 *
	 * @return The number of times the system will try to re-execute failed tasks.
	 */
	public int getNumberOfExecutionRetries() {
		return config.getNumberOfExecutionRetries();
	}

	/**
	 * Sets the default parallelism that will be used for the local execution
	 * environment created by {@link #createLocalEnvironment()}.
	 *
	 * @param parallelism
	 * 		The parallelism to use as the default local parallelism.
	 */
	public static void setDefaultLocalParallelism(int parallelism) {
		defaultLocalParallelism = parallelism;
	}

	// --------------------------------------------------------------------------------------------
	// Registry for types and serializers
	// --------------------------------------------------------------------------------------------

	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 * <p/>
	 * Note that the serializer instance must be serializable (as defined by
	 * java.io.Serializable), because it may be distributed to the worker nodes
	 * by java serialization.
	 *
	 * @param type
	 * 		The class of the types serialized with the given serializer.
	 * @param serializer
	 * 		The serializer to use.
	 */
	public <T extends Serializer<?> & Serializable>void addDefaultKryoSerializer(Class<?> type, T serializer) {
		config.addDefaultKryoSerializer(type, serializer);
	}

	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * @param type
	 * 		The class of the types serialized with the given serializer.
	 * @param serializerClass
	 * 		The class of the serializer to use.
	 */
	public void addDefaultKryoSerializer(Class<?> type,
			Class<? extends Serializer<?>> serializerClass) {
		config.addDefaultKryoSerializer(type, serializerClass);
	}

	/**
	 * Registers the given type with a Kryo Serializer.
	 * <p/>
	 * Note that the serializer instance must be serializable (as defined by
	 * java.io.Serializable), because it may be distributed to the worker nodes
	 * by java serialization.
	 *
	 * @param type
	 * 		The class of the types serialized with the given serializer.
	 * @param serializer
	 * 		The serializer to use.
	 */
	public <T extends Serializer<?> & Serializable>void registerTypeWithKryoSerializer(Class<?> type, T serializer) {
		config.registerTypeWithKryoSerializer(type, serializer);
	}

	/**
	 * Registers the given Serializer via its class as a serializer for the
	 * given type at the KryoSerializer
	 *
	 * @param type
	 * 		The class of the types serialized with the given serializer.
	 * @param serializerClass
	 * 		The class of the serializer to use.
	 */
	public void registerTypeWithKryoSerializer(Class<?> type,
			Class<? extends Serializer<?>> serializerClass) {
		config.registerTypeWithKryoSerializer(type, serializerClass);
	}

	/**
	 * Registers the given type with the serialization stack. If the type is
	 * eventually serialized as a POJO, then the type is registered with the
	 * POJO serializer. If the type ends up being serialized with Kryo, then it
	 * will be registered at Kryo to make sure that only tags are written.
	 *
	 * @param type
	 * 		The class of the type to register.
	 */
	public void registerType(Class<?> type) {
		if (type == null) {
			throw new NullPointerException("Cannot register null type class.");
		}

		TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(type);

		if (typeInfo instanceof PojoTypeInfo) {
			config.registerPojoType(type);
		} else {
			config.registerKryoType(type);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Data stream creations
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new data stream that contains a sequence of numbers. This is a parallel source,
	 * if you manually set the parallelism to {@code 1}
	 * (using {@link org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator#setParallelism(int)})
	 * the generated sequence of elements is in order.
	 *
	 * @param from
	 * 		The number to start at (inclusive)
	 * @param to
	 * 		The number to stop at (inclusive)
	 * @return A data stream, containing all number in the [from, to] interval
	 */
	public DataStreamSource<Long> generateSequence(long from, long to) {
		if (from > to) {
			throw new IllegalArgumentException("Start of sequence must not be greater than the end");
		}
		return addSource(new StatefulSequenceSource(from, to), "Sequence Source");
	}

	/**
	 * Creates a new data stream that contains the given elements. The elements must all be of the same type, for
	 * example, all of the {@link String} or {@link Integer}.
	 * <p>
	 * The framework will try and determine the exact type from the elements. In case of generic elements, it may be
	 * necessary to manually supply the type information via {@link #fromCollection(java.util.Collection,
	 * org.apache.flink.api.common.typeinfo.TypeInformation)}.
	 * <p>
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism one.
	 *
	 * @param data
	 * 		The array of elements to create the data stream from.
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream representing the given array of elements
	 */
	@SafeVarargs
	public final <OUT> DataStreamSource<OUT> fromElements(OUT... data) {
		if (data.length == 0) {
			throw new IllegalArgumentException("fromElements needs at least one element as argument");
		}

		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(data[0]);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + data[0].getClass().getName()
				+ "; please specify the TypeInformation manually via "
				+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}
		return fromCollection(Arrays.asList(data), typeInfo);
	}

	/**
	 * Creates a data stream from the given non-empty collection. The type of the data stream is that of the
	 * elements in the collection.
	 *
	 * <p>The framework will try and determine the exact type from the collection elements. In case of generic
	 * elements, it may be necessary to manually supply the type information via
	 * {@link #fromCollection(java.util.Collection, org.apache.flink.api.common.typeinfo.TypeInformation)}.</p>
	 *
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * parallelism one.</p>
	 *
	 * @param data
	 * 		The collection of elements to create the data stream from.
	 * @param <OUT>
	 *     The generic type of the returned data stream.
	 * @return
	 *     The data stream representing the given collection
	 */
	public <OUT> DataStreamSource<OUT> fromCollection(Collection<OUT> data) {
		Preconditions.checkNotNull(data, "Collection must not be null");
		if (data.isEmpty()) {
			throw new IllegalArgumentException("Collection must not be empty");
		}

		OUT first = data.iterator().next();
		if (first == null) {
			throw new IllegalArgumentException("Collection must not contain null elements");
		}

		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(first);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + first.getClass()
					+ "; please specify the TypeInformation manually via "
					+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}
		return fromCollection(data, typeInfo);
	}

	/**
	 * Creates a data stream from the given non-empty collection.
	 * 
	 * <p>Note that this operation will result in a non-parallel data stream source,
	 * i.e., a data stream source with a parallelism one.</p>
	 *
	 * @param data
	 * 		The collection of elements to create the data stream from
	 * @param typeInfo
	 * 		The TypeInformation for the produced data stream
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream representing the given collection
	 */
	public <OUT> DataStreamSource<OUT> fromCollection(Collection<OUT> data, TypeInformation<OUT> typeInfo) {
		Preconditions.checkNotNull(data, "Collection must not be null");
		
		// must not have null elements and mixed elements
		FromElementsFunction.checkCollection(data, typeInfo.getTypeClass());

		SourceFunction<OUT> function;
		try {
			function = new FromElementsFunction<OUT>(typeInfo.createSerializer(getConfig()), data);
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		return addSource(function, "Collection Source", typeInfo).setParallelism(1);
	}

	/**
	 * Creates a data stream from the given iterator.
	 * 
	 * <p>Because the iterator will remain unmodified until the actual execution happens,
	 * the type of data returned by the iterator must be given explicitly in the form of the type
	 * class (this is due to the fact that the Java compiler erases the generic type information).</p>
	 * 
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e.,
	 * a data stream source with a parallelism of one.</p>
	 *
	 * @param data
	 * 		The iterator of elements to create the data stream from
	 * @param type
	 * 		The class of the data produced by the iterator. Must not be a generic class.
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream representing the elements in the iterator
	 * @see #fromCollection(java.util.Iterator, org.apache.flink.api.common.typeinfo.TypeInformation)
	 */
	public <OUT> DataStreamSource<OUT> fromCollection(Iterator<OUT> data, Class<OUT> type) {
		return fromCollection(data, TypeExtractor.getForClass(type));
	}

	/**
	 * Creates a data stream from the given iterator.
	 * 
	 * <p>Because the iterator will remain unmodified until the actual execution happens,
	 * the type of data returned by the iterator must be given explicitly in the form of the type
	 * information. This method is useful for cases where the type is generic.
	 * In that case, the type class (as given in
	 * {@link #fromCollection(java.util.Iterator, Class)} does not supply all type information.</p>
	 * 
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e.,
	 * a data stream source with a parallelism one.</p>
	 *
	 * @param data
	 * 		The iterator of elements to create the data stream from
	 * @param typeInfo
	 * 		The TypeInformation for the produced data stream
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream representing the elements in the iterator
	 */
	public <OUT> DataStreamSource<OUT> fromCollection(Iterator<OUT> data, TypeInformation<OUT> typeInfo) {
		Preconditions.checkNotNull(data, "The iterator must not be null");

		SourceFunction<OUT> function = new FromIteratorFunction<OUT>(data);
		return addSource(function, "Collection Source", typeInfo);
	}

	/**
	 * Creates a new data stream that contains elements in the iterator. The iterator is splittable, allowing the
	 * framework to create a parallel data stream source that returns the elements in the iterator.
	 * 
	 * <p>Because the iterator will remain unmodified until the actual execution happens, the type of data returned by the
	 * iterator must be given explicitly in the form of the type class (this is due to the fact that the Java compiler
	 * erases the generic type information).</p>
	 *
	 * @param iterator
	 * 		The iterator that produces the elements of the data stream
	 * @param type
	 * 		The class of the data produced by the iterator. Must not be a generic class.
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return A data stream representing the elements in the iterator
	 */
	public <OUT> DataStreamSource<OUT> fromParallelCollection(SplittableIterator<OUT> iterator, Class<OUT> type) {
		return fromParallelCollection(iterator, TypeExtractor.getForClass(type));
	}

	/**
	 * Creates a new data stream that contains elements in the iterator. The iterator is splittable, allowing the
	 * framework to create a parallel data stream source that returns the elements in the iterator.
	 * <p>
	 * Because the iterator will remain unmodified until the actual execution happens, the type of data returned by the
	 * iterator must be given explicitly in the form of the type information. This method is useful for cases where the
	 * type is generic. In that case, the type class (as given in {@link #fromParallelCollection(org.apache.flink.util.SplittableIterator,
	 * Class)} does not supply all type information.
	 *
	 * @param iterator
	 * 		The iterator that produces the elements of the data stream
	 * @param typeInfo
	 * 		The TypeInformation for the produced data stream.
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return A data stream representing the elements in the iterator
	 */
	public <OUT> DataStreamSource<OUT> fromParallelCollection(SplittableIterator<OUT> iterator, TypeInformation<OUT>
			typeInfo) {
		return fromParallelCollection(iterator, typeInfo, "Parallel Collection Source");
	}

	// private helper for passing different names
	private <OUT> DataStreamSource<OUT> fromParallelCollection(SplittableIterator<OUT> iterator, TypeInformation<OUT>
			typeInfo, String operatorName) {
		return addSource(new FromSplittableIteratorFunction<OUT>(iterator), operatorName).returns(typeInfo);
	}

	/**
	 * Creates a data stream that represents the Strings produced by reading the given file line wise. The file will be
	 * read with the system's default character set.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return The data stream that represents the data read from the given file as text lines
	 */
	public DataStreamSource<String> readTextFile(String filePath) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		TextInputFormat format = new TextInputFormat(new Path(filePath));
		TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;

		return createInput(format, typeInfo, "Read Text File Source");
	}

	/**
	 * Creates a data stream that represents the Strings produced by reading the given file line wise. The {@link
	 * java.nio.charset.Charset} with the given name will be used to read the files.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param charsetName
	 * 		The name of the character set used to read the file
	 * @return The data stream that represents the data read from the given file as text lines
	 */
	public DataStreamSource<String> readTextFile(String filePath, String charsetName) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		TextInputFormat format = new TextInputFormat(new Path(filePath));
		TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
		format.setCharsetName(charsetName);

		return createInput(format, typeInfo, "Read Text File Source");
	}

	/**
	 * Creates a data stream that represents the strings produced by reading the given file line wise. This method is
	 * similar to {@link #readTextFile(String)}, but it produces a data stream with mutable {@link org.apache.flink.types.StringValue}
	 * objects,
	 * rather than Java Strings. StringValues can be used to tune implementations to be less object and garbage
	 * collection heavy.
	 * <p/>
	 * The file will be read with the system's default character set.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @return A data stream that represents the data read from the given file as text lines
	 */
	public DataStreamSource<StringValue> readTextFileWithValue(String filePath) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		TextValueInputFormat format = new TextValueInputFormat(new Path(filePath));
		TypeInformation<StringValue> typeInfo = new ValueTypeInfo<StringValue>(StringValue.class);

		return createInput(format, typeInfo, "Read Text File with Value " +
				"source");
	}

	/**
	 * Creates a data stream that represents the Strings produced by reading the given file line wise. This method is
	 * similar to {@link #readTextFile(String, String)}, but it produces a data stream with mutable {@link org.apache.flink.types.StringValue}
	 * objects, rather than Java Strings. StringValues can be used to tune implementations to be less object and
	 * garbage
	 * collection heavy.
	 * <p/>
	 * The {@link java.nio.charset.Charset} with the given name will be used to read the files.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param charsetName
	 * 		The name of the character set used to read the file
	 * @param skipInvalidLines
	 * 		A flag to indicate whether to skip lines that cannot be read with the given character set
	 * @return A data stream that represents the data read from the given file as text lines
	 */
	public DataStreamSource<StringValue> readTextFileWithValue(String filePath, String charsetName, boolean
			skipInvalidLines) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		TextValueInputFormat format = new TextValueInputFormat(new Path(filePath));
		TypeInformation<StringValue> typeInfo = new ValueTypeInfo<StringValue>(StringValue.class);
		format.setCharsetName(charsetName);
		format.setSkipInvalidLines(skipInvalidLines);
		return createInput(format, typeInfo, "Read Text File with Value " +
				"source");
	}

	/**
	 * Reads the given file with the given imput format.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data read from the given file
	 */
	public <OUT> DataStreamSource<OUT> readFile(FileInputFormat<OUT> inputFormat, String filePath) {
		Preconditions.checkNotNull(inputFormat, "InputFormat must not be null.");
		Preconditions.checkNotNull(filePath, "The file path must not be null.");

		inputFormat.setFilePath(new Path(filePath));
		try {
			return createInput(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat), "Read File source");
		} catch (Exception e) {
			throw new InvalidProgramException("The type returned by the input format could not be automatically " +
					"determined. " +
					"Please specify the TypeInformation of the produced type explicitly by using the " +
					"'createInput(InputFormat, TypeInformation)' method instead.");
		}
	}

	/**
	 * Creates a data stream that represents the primitive type produced by reading the given file line wise.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param typeClass
	 * 		The primitive type class to be read
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return A data stream that represents the data read from the given file as primitive type
	 */
	public <OUT> DataStreamSource<OUT> readFileOfPrimitives(String filePath, Class<OUT> typeClass) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		PrimitiveInputFormat<OUT> inputFormat = new PrimitiveInputFormat<OUT>(new Path(filePath), typeClass);
		TypeInformation<OUT> typeInfo = TypeExtractor.getForClass(typeClass);

		return createInput(inputFormat, typeInfo, "Read File of Primitives source");
	}

	/**
	 * Creates a data stream that represents the primitive type produced by reading the given file in delimited way.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param delimiter
	 * 		The delimiter of the given file
	 * @param typeClass
	 * 		The primitive type class to be read
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return A data stream that represents the data read from the given file as primitive type.
	 */
	public <OUT> DataStreamSource<OUT> readFileOfPrimitives(String filePath, String delimiter, Class<OUT> typeClass) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		PrimitiveInputFormat<OUT> inputFormat = new PrimitiveInputFormat<OUT>(new Path(filePath), delimiter,
				typeClass);
		TypeInformation<OUT> typeInfo = TypeExtractor.getForClass(typeClass);

		return createInput(inputFormat, typeInfo, "Read File of Primitives source");
	}

	/**
	 * Creates a data stream that contains the contents of file created while system watches the given path. The file
	 * will be read with the system's default character set.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path/")
	 * @param intervalMillis
	 * 		The interval of file watching in milliseconds
	 * @param watchType
	 * 		The watch type of file stream. When watchType is {@link org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType#ONLY_NEW_FILES}, the system processes
	 * 		only
	 * 		new files. {@link org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType#REPROCESS_WITH_APPENDED} means that the system re-processes all contents of
	 * 		appended file. {@link org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType#PROCESS_ONLY_APPENDED} means that the system processes only appended
	 * 		contents
	 * 		of files.
	 * @return The DataStream containing the given directory.
	 */
	public DataStream<String> readFileStream(String filePath, long intervalMillis,
											WatchType watchType) {
		DataStream<Tuple3<String, Long, Long>> source = addSource(new FileMonitoringFunction(
				filePath, intervalMillis, watchType), "Read File Stream source");

		return source.flatMap(new FileReadFunction());
	}

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set. On the termination of the socket server connection retries can be
	 * initiated.
	 * <p/>
	 * Let us note that the socket itself does not report on abort and as a consequence retries are only initiated when
	 * the socket was gracefully terminated.
	 *
	 * @param hostname
	 * 		The host name which a server socket binds
	 * @param port
	 * 		The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * 		allocated.
	 * @param delimiter
	 * 		A character which splits received strings into records
	 * @param maxRetry
	 * 		The maximal retry interval in seconds while the program waits for a socket that is temporarily down.
	 * 		Reconnection is initiated every second. A number of 0 means that the reader is immediately terminated,
	 * 		while
	 * 		a	negative value ensures retrying forever.
	 * @return A data stream containing the strings received from the socket
	 */
	public DataStreamSource<String> socketTextStream(String hostname, int port, char delimiter, long maxRetry) {
		return addSource(new SocketTextStreamFunction(hostname, port, delimiter, maxRetry),
				"Socket Stream");
	}

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set. The reader is terminated immediately when the socket is down.
	 *
	 * @param hostname
	 * 		The host name which a server socket binds
	 * @param port
	 * 		The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * 		allocated.
	 * @param delimiter
	 * 		A character which splits received strings into records
	 * @return A data stream containing the strings received from the socket
	 */
	public DataStreamSource<String> socketTextStream(String hostname, int port, char delimiter) {
		return socketTextStream(hostname, port, delimiter, 0);
	}

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set, using'\n' as delimiter. The reader is terminated immediately when
	 * the socket is down.
	 *
	 * @param hostname
	 * 		The host name which a server socket binds
	 * @param port
	 * 		The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * 		allocated.
	 * @return A data stream containing the strings received from the socket
	 */
	public DataStreamSource<String> socketTextStream(String hostname, int port) {
		return socketTextStream(hostname, port, '\n');
	}

	/**
	 * Generic method to create an input data stream with {@link org.apache.flink.api.common.io.InputFormat}.
	 * <p/>
	 * Since all data streams need specific information about their types, this method needs to determine the type of
	 * the data produced by the input format. It will attempt to determine the data type by reflection, unless the
	 * input
	 * format implements the {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable} interface. In the latter
	 * case, this method will invoke the {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable#getProducedType()}
	 * method to determine data type produced by the input format.
	 *
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data created by the input format
	 */
	public <OUT> DataStreamSource<OUT> createInput(InputFormat<OUT, ?> inputFormat) {
		return createInput(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat), "Custom File source");
	}

	/**
	 * Generic method to create an input data stream with {@link org.apache.flink.api.common.io.InputFormat}.
	 * <p>
	 * The data stream is typed to the given TypeInformation. This method is intended for input formats where the
	 * return
	 * type cannot be determined by reflection analysis, and that do not implement the
	 * {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable} interface.
	 *
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data created by the input format
	 */
	public <OUT> DataStreamSource<OUT> createInput(InputFormat<OUT, ?> inputFormat, TypeInformation<OUT> typeInfo) {
		return createInput(inputFormat, typeInfo, "Custom File source");
	}

	// private helper for passing different names
	private <OUT> DataStreamSource<OUT> createInput(InputFormat<OUT, ?> inputFormat,
			TypeInformation<OUT> typeInfo, String sourceName) {
		FileSourceFunction<OUT> function = new FileSourceFunction<OUT>(inputFormat, typeInfo);
		return addSource(function, sourceName).returns(typeInfo);
	}

	/**
	 * Adds a Data Source to the streaming topology.
	 *
	 * <p>
	 * By default sources have a parallelism of 1. To enable parallel execution, the user defined source should
	 * implement {@link org.apache.flink.streaming.api.functions.source.ParallelSourceFunction} or extend {@link
	 * org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction}. In these cases the resulting source
	 * will have the parallelism of the environment. To change this afterwards call {@link
	 * org.apache.flink.streaming.api.datastream.DataStreamSource#setParallelism(int)}
	 *
	 * @param function
	 * 		the user defined function
	 * @param <OUT>
	 * 		type of the returned stream
	 * @return the data stream constructed
	 */
	public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function) {
		return addSource(function, "Custom Source");
	}

	/**
	 * Ads a data source with a custom type information thus opening a
	 * {@link DataStream}. Only in very special cases does the user need to
	 * support type information. Otherwise use
	 * {@link #addSource(org.apache.flink.streaming.api.functions.source.SourceFunction)}
	 *
	 * @param function
	 * 		the user defined function
	 * @param sourceName
	 * 		Name of the data source
	 * @param <OUT>
	 * 		type of the returned stream
	 * @return the data stream constructed
	 */
	public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, String sourceName) {
		return addSource(function, sourceName, null);
	}

	/**
	 * Ads a data source with a custom type information thus opening a
	 * {@link DataStream}. Only in very special cases does the user need to
	 * support type information. Otherwise use
	 * {@link #addSource(org.apache.flink.streaming.api.functions.source.SourceFunction)}
	 *
	 * @param function
	 * 		the user defined function
	 * @param <OUT>
	 * 		type of the returned stream
	 * @param typeInfo
	 * 		the user defined type information for the stream
	 * @return the data stream constructed
	 */
	public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, TypeInformation<OUT> typeInfo) {
		return addSource(function, "Custom Source", typeInfo);
	}

	/**
	 * Ads a data source with a custom type information thus opening a
	 * {@link DataStream}. Only in very special cases does the user need to
	 * support type information. Otherwise use
	 * {@link #addSource(org.apache.flink.streaming.api.functions.source.SourceFunction)}
	 *
	 * @param function
	 * 		the user defined function
	 * @param sourceName
	 * 		Name of the data source
	 * @param <OUT>
	 * 		type of the returned stream
	 * @param typeInfo
	 * 		the user defined type information for the stream
	 * @return the data stream constructed
	 */
	@SuppressWarnings("unchecked")
	public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, String sourceName, TypeInformation<OUT> typeInfo) {

		if(typeInfo == null) {
			if (function instanceof ResultTypeQueryable) {
				typeInfo = ((ResultTypeQueryable<OUT>) function).getProducedType();
			} else {
				try {
					typeInfo = TypeExtractor.createTypeInfo(
							SourceFunction.class,
							function.getClass(), 0, null, null);
				} catch (final InvalidTypesException e) {
					typeInfo = (TypeInformation<OUT>) new MissingTypeInfo(sourceName, e);
				}
			}
		}

		boolean isParallel = function instanceof ParallelSourceFunction;

		clean(function);
		StreamSource<OUT> sourceOperator = new StreamSource<OUT>(function);

		return new DataStreamSource<OUT>(this, typeInfo, sourceOperator, isParallel, sourceName);
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
	 * executed.
	 */
	public static StreamExecutionEnvironment getExecutionEnvironment() {
		if (contextEnvironmentFactory != null) {
			return contextEnvironmentFactory.createExecutionEnvironment();
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		if (env instanceof ContextEnvironment) {
			ContextEnvironment ctx = (ContextEnvironment) env;
			return createContextEnvironment(ctx.getClient(), ctx.getJars(),
					ctx.getParallelism(), ctx.isWait());
		} else if (env instanceof OptimizerPlanEnvironment | env instanceof PreviewPlanEnvironment) {
			return new StreamPlanEnvironment(env);
		} else {
			return createLocalEnvironment();
		}
	}

	private static StreamExecutionEnvironment createContextEnvironment(Client client,
			List<File> jars, int parallelism, boolean wait) {
		return new StreamContextEnvironment(client, jars, parallelism, wait);
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. The default parallelism of the local
	 * environment is the number of hardware contexts (CPU cores / threads),
	 * unless it was specified differently by {@link #setParallelism(int)}.
	 *
	 * @return A local execution environment.
	 */
	public static LocalStreamEnvironment createLocalEnvironment() {
		return createLocalEnvironment(defaultLocalParallelism);
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. It will use the parallelism specified in the
	 * parameter.
	 *
	 * @param parallelism
	 * 		The parallelism for the local environment.
	 * @return A local execution environment with the specified parallelism.
	 */
	public static LocalStreamEnvironment createLocalEnvironment(int parallelism) {
		LocalStreamEnvironment env = new LocalStreamEnvironment();
		env.setParallelism(parallelism);
		return env;
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. It will use the parallelism specified in the
	 * parameter.
	 *
	 * @param parallelism
	 * 		The parallelism for the local environment.
	 * 	@param configuration
	 * 		Pass a custom configuration into the cluster
	 * @return A local execution environment with the specified parallelism.
	 */
	public static LocalStreamEnvironment createLocalEnvironment(int parallelism, Configuration configuration) {
		LocalStreamEnvironment currentEnvironment = new LocalStreamEnvironment(configuration);
		currentEnvironment.setParallelism(parallelism);
		return (LocalStreamEnvironment) currentEnvironment;
	}

	// TODO:fix cluster default parallelism

	/**
	 * Creates a {@link RemoteStreamEnvironment}. The remote environment sends
	 * (parts of) the program to a cluster for execution. Note that all file
	 * paths used in the program must be accessible from the cluster. The
	 * execution will use no parallelism, unless the parallelism is set
	 * explicitly via {@link #setParallelism}.
	 *
	 * @param host
	 * 		The host name or address of the master (JobManager), where the
	 * 		program should be executed.
	 * @param port
	 * 		The port of the master (JobManager), where the program should
	 * 		be executed.
	 * @param jarFiles
	 * 		The JAR files with code that needs to be shipped to the
	 * 		cluster. If the program uses user-defined functions,
	 * 		user-defined input formats, or any libraries, those must be
	 * 		provided in the JAR files.
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
	 * execution will use the specified parallelism.
	 *
	 * @param host
	 * 		The host name or address of the master (JobManager), where the
	 * 		program should be executed.
	 * @param port
	 * 		The port of the master (JobManager), where the program should
	 * 		be executed.
	 * @param parallelism
	 * 		The parallelism to use during the execution.
	 * @param jarFiles
	 * 		The JAR files with code that needs to be shipped to the
	 * 		cluster. If the program uses user-defined functions,
	 * 		user-defined input formats, or any libraries, those must be
	 * 		provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static StreamExecutionEnvironment createRemoteEnvironment(String host, int port,
			int parallelism, String... jarFiles) {
		RemoteStreamEnvironment env = new RemoteStreamEnvironment(host, port, jarFiles);
		env.setParallelism(parallelism);
		return env;
	}

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 * <p/>
	 * The program execution will be logged and displayed with a generated
	 * default name.
	 *
	 * @return The result of the job execution, containing elapsed time and
	 * accumulators.
	 * @throws Exception
	 */
	public abstract JobExecutionResult execute() throws Exception;

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 * <p/>
	 * The program execution will be logged and displayed with the provided name
	 *
	 * @param jobName
	 * 		Desired name of the job
	 * @return The result of the job execution: Either JobSubmissionResult or JobExecutionResult;
	 * The latter contains elapsed time and accumulators.
	 * @throws Exception
	 */
	public abstract JobSubmissionResult execute(String jobName) throws Exception;

	/**
	 * Getter of the {@link org.apache.flink.streaming.api.graph.StreamGraph} of the streaming job.
	 *
	 * @return The streamgraph representing the transformations
	 */
	public StreamGraph getStreamGraph() {
		if (transformations.size() <= 0) {
			throw new IllegalStateException("No operators defined in streaming topology. Cannot execute.");
		}
		return StreamGraphGenerator.generate(this, transformations);
	}

	/**
	 * Creates the plan with which the system will execute the program, and
	 * returns it as a String using a JSON representation of the execution data
	 * flow graph. Note that this needs to be called, before the plan is
	 * executed.
	 *
	 * @return The execution plan of the program, as a JSON String.
	 */
	public String getExecutionPlan() {
		return getStreamGraph().getStreamingPlanAsJSON();
	}

	/**
	 * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
	 * is not disabled in the {@link org.apache.flink.api.common.ExecutionConfig}
	 */
	public <F> F clean(F f) {
		if (getConfig().isClosureCleanerEnabled()) {
			ClosureCleaner.clean(f, true);
		}
		ClosureCleaner.ensureSerializable(f);
		return f;
	}

	/**
	 * Adds an operator to the list of operators that should be executed when calling
	 * {@link #execute}.
	 *
	 * <p>
	 * When calling {@link #execute()} only the operators that where previously added to the list
	 * are executed.
	 *
	 * <p>
	 * This is not meant to be used by users. The API methods that create operators must call
	 * this method.
	 */
	public void addOperator(StreamTransformation<?> transformation) {
		Preconditions.checkNotNull(transformation, "Sinks must not be null.");
		this.transformations.add(transformation);
	}

	// --------------------------------------------------------------------------------------------
	//  Methods to control the context and local environments for execution from packaged programs
	// --------------------------------------------------------------------------------------------

	protected static void initializeContextEnvironment(StreamExecutionEnvironmentFactory ctx) {
		contextEnvironmentFactory = ctx;
	}
}
