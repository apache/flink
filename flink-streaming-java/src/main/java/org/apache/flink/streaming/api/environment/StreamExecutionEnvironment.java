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

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.esotericsoftware.kryo.Serializer;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PreviewPlanEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.FileReadFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.streaming.api.functions.source.FromSplittableIteratorFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.StoppableStreamSource;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SplittableIterator;

/**
 * The StreamExecutionEnvironment is the context in which a streaming program is executed. A
 * {@link LocalStreamEnvironment} will cause execution in the current JVM, a
 * {@link RemoteStreamEnvironment} will cause execution on a remote setup.
 *
 * <p>The environment provides methods to control the job execution (such as setting the parallelism
 * or the fault tolerance/checkpointing parameters) and to interact with the outside world (data access).
 *
 * @see org.apache.flink.streaming.api.environment.LocalStreamEnvironment
 * @see org.apache.flink.streaming.api.environment.RemoteStreamEnvironment
 */
@Public
public abstract class StreamExecutionEnvironment {

	/** The default name to use for a streaming job if no other name has been specified. */
	public static final String DEFAULT_JOB_NAME = "Flink Streaming Job";

	/** The time characteristic that is used if none other is set. */
	private static final TimeCharacteristic DEFAULT_TIME_CHARACTERISTIC = TimeCharacteristic.ProcessingTime;

	/** The default buffer timeout (max delay of records in the network stack). */
	private static final long DEFAULT_NETWORK_BUFFER_TIMEOUT = 100L;

	/**
	 * The environment of the context (local by default, cluster if invoked through command line).
	 */
	private static StreamExecutionEnvironmentFactory contextEnvironmentFactory;

	/** The default parallelism used when creating a local environment. */
	private static int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();

	// ------------------------------------------------------------------------

	/** The execution configuration for this environment. */
	private final ExecutionConfig config = new ExecutionConfig();

	/** Settings that control the checkpointing behavior. */
	private final CheckpointConfig checkpointCfg = new CheckpointConfig();

	protected final List<StreamTransformation<?>> transformations = new ArrayList<>();

	private long bufferTimeout = DEFAULT_NETWORK_BUFFER_TIMEOUT;

	protected boolean isChainingEnabled = true;

	/** The state backend used for storing k/v state and state snapshots. */
	private AbstractStateBackend defaultStateBackend;

	/** The time characteristic used by the data streams. */
	private TimeCharacteristic timeCharacteristic = DEFAULT_TIME_CHARACTERISTIC;


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
	 * @param parallelism The parallelism
	 */
	public StreamExecutionEnvironment setParallelism(int parallelism) {
		if (parallelism < 1) {
			throw new IllegalArgumentException("parallelism must be at least one.");
		}
		config.setParallelism(parallelism);
		return this;
	}

	/**
	 * Sets the maximum degree of parallelism defined for the program. The upper limit (inclusive)
	 * is Short.MAX_VALUE.
	 *
	 * <p>The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
	 * defines the number of key groups used for partitioned state.
	 *
	 * @param maxParallelism Maximum degree of parallelism to be used for the program.,
	 *              with 0 < maxParallelism <= 2^15 - 1
	 */
	public StreamExecutionEnvironment setMaxParallelism(int maxParallelism) {
		Preconditions.checkArgument(maxParallelism > 0 &&
						maxParallelism <= KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM,
				"maxParallelism is out of bounds 0 < maxParallelism <= " +
						KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM + ". Found: " + maxParallelism);

		config.setMaxParallelism(maxParallelism);
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
	 * Gets the maximum degree of parallelism defined for the program.
	 *
	 * <p>The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
	 * defines the number of key groups used for partitioned state.
	 *
	 * @return Maximum degree of parallelism
	 */
	public int getMaxParallelism() {
		return config.getMaxParallelism();
	}

	/**
	 * Sets the maximum time frequency (milliseconds) for the flushing of the
	 * output buffers. By default the output buffers flush frequently to provide
	 * low latency and to aid smooth developer experience. Setting the parameter
	 * can result in three logical modes:
	 *
	 * <ul>
	 *   <li>A positive integer triggers flushing periodically by that integer</li>
	 *   <li>0 triggers flushing after every record thus minimizing latency</li>
	 *   <li>-1 triggers flushing only when the output buffer is full thus maximizing
	 *      throughput</li>
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
	 * Gets the maximum time frequency (milliseconds) for the flushing of the
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
	@PublicEvolving
	public StreamExecutionEnvironment disableOperatorChaining() {
		this.isChainingEnabled = false;
		return this;
	}

	/**
	 * Returns whether operator chaining is enabled.
	 *
	 * @return {@code true} if chaining is enabled, false otherwise.
	 */
	@PublicEvolving
	public boolean isChainingEnabled() {
		return isChainingEnabled;
	}

	// ------------------------------------------------------------------------
	//  Checkpointing Settings
	// ------------------------------------------------------------------------

	/**
	 * Gets the checkpoint config, which defines values like checkpoint interval, delay between
	 * checkpoints, etc.
	 *
	 * @return The checkpoint config.
	 */
	public CheckpointConfig getCheckpointConfig() {
		return checkpointCfg;
	}

	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint. This method selects
	 * {@link CheckpointingMode#EXACTLY_ONCE} guarantees.
	 *
	 * <p>The job draws checkpoints periodically, in the given interval. The state will be
	 * stored in the configured state backend.
	 *
	 * <p>NOTE: Checkpointing iterative streaming dataflows in not properly supported at
	 * the moment. For that reason, iterative jobs will not be started if used
	 * with enabled checkpointing. To override this mechanism, use the
	 * {@link #enableCheckpointing(long, CheckpointingMode, boolean)} method.
	 *
	 * @param interval Time interval between state checkpoints in milliseconds.
	 */
	public StreamExecutionEnvironment enableCheckpointing(long interval) {
		checkpointCfg.setCheckpointInterval(interval);
		return this;
	}

	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint.
	 *
	 * <p>The job draws checkpoints periodically, in the given interval. The system uses the
	 * given {@link CheckpointingMode} for the checkpointing ("exactly once" vs "at least once").
	 * The state will be stored in the configured state backend.
	 *
	 * <p>NOTE: Checkpointing iterative streaming dataflows in not properly supported at
	 * the moment. For that reason, iterative jobs will not be started if used
	 * with enabled checkpointing. To override this mechanism, use the
	 * {@link #enableCheckpointing(long, CheckpointingMode, boolean)} method.
	 *
	 * @param interval
	 *             Time interval between state checkpoints in milliseconds.
	 * @param mode
	 *             The checkpointing mode, selecting between "exactly once" and "at least once" guaranteed.
	 */
	public StreamExecutionEnvironment enableCheckpointing(long interval, CheckpointingMode mode) {
		checkpointCfg.setCheckpointingMode(mode);
		checkpointCfg.setCheckpointInterval(interval);
		return this;
	}

	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint.
	 *
	 * <p>The job draws checkpoints periodically, in the given interval. The state will be
	 * stored in the configured state backend.
	 *
	 * <p>NOTE: Checkpointing iterative streaming dataflows in not properly supported at
	 * the moment. If the "force" parameter is set to true, the system will execute the
	 * job nonetheless.
	 *
	 * @param interval
	 *            Time interval between state checkpoints in millis.
	 * @param mode
	 *            The checkpointing mode, selecting between "exactly once" and "at least once" guaranteed.
	 * @param force
	 *            If true checkpointing will be enabled for iterative jobs as well.
	 *
	 * @deprecated Use {@link #enableCheckpointing(long, CheckpointingMode)} instead.
	 * Forcing checkpoints will be removed in the future.
	 */
	@Deprecated
	@SuppressWarnings("deprecation")
	@PublicEvolving
	public StreamExecutionEnvironment enableCheckpointing(long interval, CheckpointingMode mode, boolean force) {
		checkpointCfg.setCheckpointingMode(mode);
		checkpointCfg.setCheckpointInterval(interval);
		checkpointCfg.setForceCheckpointing(force);
		return this;
	}

	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint. This method selects
	 * {@link CheckpointingMode#EXACTLY_ONCE} guarantees.
	 *
	 * <p>The job draws checkpoints periodically, in the default interval. The state will be
	 * stored in the configured state backend.
	 *
	 * <p>NOTE: Checkpointing iterative streaming dataflows in not properly supported at
	 * the moment. For that reason, iterative jobs will not be started if used
	 * with enabled checkpointing. To override this mechanism, use the
	 * {@link #enableCheckpointing(long, CheckpointingMode, boolean)} method.
	 *
	 * @deprecated Use {@link #enableCheckpointing(long)} instead.
	 */
	@Deprecated
	@PublicEvolving
	public StreamExecutionEnvironment enableCheckpointing() {
		checkpointCfg.setCheckpointInterval(500);
		return this;
	}

	/**
	 * Returns the checkpointing interval or -1 if checkpointing is disabled.
	 *
	 * <p>Shorthand for {@code getCheckpointConfig().getCheckpointInterval()}.
	 *
	 * @return The checkpointing interval or -1
	 */
	public long getCheckpointInterval() {
		return checkpointCfg.getCheckpointInterval();
	}

	/**
	 * Returns whether checkpointing is force-enabled.
	 *
	 * @deprecated Forcing checkpoints will be removed in future version.
	 */
	@Deprecated
	@SuppressWarnings("deprecation")
	@PublicEvolving
	public boolean isForceCheckpointing() {
		return checkpointCfg.isForceCheckpointing();
	}

	/**
	 * Returns the checkpointing mode (exactly-once vs. at-least-once).
	 *
	 * <p>Shorthand for {@code getCheckpointConfig().getCheckpointingMode()}.
	 *
	 * @return The checkpoin
	 */
	public CheckpointingMode getCheckpointingMode() {
		return checkpointCfg.getCheckpointingMode();
	}

	/**
	 * Sets the state backend that describes how to store and checkpoint operator state. It defines in
	 * what form the key/value state ({@link ValueState}, accessible
	 * from operations on {@link org.apache.flink.streaming.api.datastream.KeyedStream}) is maintained
	 * (heap, managed memory, externally), and where state snapshots/checkpoints are stored, both for
	 * the key/value state, and for checkpointed functions (implementing the interface
	 * {@link org.apache.flink.streaming.api.checkpoint.Checkpointed}).
	 *
	 * <p>The {@link org.apache.flink.runtime.state.memory.MemoryStateBackend} for example
	 * maintains the state in heap memory, as objects. It is lightweight without extra dependencies,
	 * but can checkpoint only small states (some counters).
	 *
	 * <p>In contrast, the {@link org.apache.flink.runtime.state.filesystem.FsStateBackend}
	 * stores checkpoints of the state (also maintained as heap objects) in files. When using a replicated
	 * file system (like HDFS, S3, MapR FS, Tachyon, etc) this will guarantee that state is not lost upon
	 * failures of individual nodes and that streaming program can be executed highly available and strongly
	 * consistent (assuming that Flink is run in high-availability mode).
	 *
	 * @return This StreamExecutionEnvironment itself, to allow chaining of function calls.
	 *
	 * @see #getStateBackend()
	 */
	@PublicEvolving
	public StreamExecutionEnvironment setStateBackend(AbstractStateBackend backend) {
		this.defaultStateBackend = Preconditions.checkNotNull(backend);
		return this;
	}

	/**
	 * Returns the state backend that defines how to store and checkpoint state.
	 * @return The state backend that defines how to store and checkpoint state.
	 *
	 * @see #setStateBackend(AbstractStateBackend)
	 */
	@PublicEvolving
	public AbstractStateBackend getStateBackend() {
		return defaultStateBackend;
	}

	/**
	 * Sets the restart strategy configuration. The configuration specifies which restart strategy
	 * will be used for the execution graph in case of a restart.
	 *
	 * @param restartStrategyConfiguration Restart strategy configuration to be set
	 */
	@PublicEvolving
	public void setRestartStrategy(RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {
		config.setRestartStrategy(restartStrategyConfiguration);
	}

	/**
	 * Returns the specified restart strategy configuration.
	 *
	 * @return The restart strategy configuration to be used
	 */
	@PublicEvolving
	public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
		return config.getRestartStrategy();
	}

	/**
	 * Sets the number of times that failed tasks are re-executed. A value of
	 * zero effectively disables fault tolerance. A value of {@code -1}
	 * indicates that the system default value (as defined in the configuration)
	 * should be used.
	 *
	 * @param numberOfExecutionRetries
	 * 		The number of times the system will try to re-execute failed tasks.
	 *
	 * @deprecated This method will be replaced by {@link #setRestartStrategy}. The
	 * {@link RestartStrategies#fixedDelayRestart(int, Time)} contains the number of
	 * execution retries.
	 */
	@Deprecated
	@PublicEvolving
	public void setNumberOfExecutionRetries(int numberOfExecutionRetries) {
		config.setNumberOfExecutionRetries(numberOfExecutionRetries);
	}

	/**
	 * Gets the number of times the system will try to re-execute failed tasks.
	 * A value of {@code -1} indicates that the system default value (as defined
	 * in the configuration) should be used.
	 *
	 * @return The number of times the system will try to re-execute failed tasks.
	 *
	 * @deprecated This method will be replaced by {@link #getRestartStrategy}.
	 */
	@Deprecated
	@PublicEvolving
	public int getNumberOfExecutionRetries() {
		return config.getNumberOfExecutionRetries();
	}

	// --------------------------------------------------------------------------------------------
	// Registry for types and serializers
	// --------------------------------------------------------------------------------------------

	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * <p>Note that the serializer instance must be serializable (as defined by
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
	public void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		config.addDefaultKryoSerializer(type, serializerClass);
	}

	/**
	 * Registers the given type with a Kryo Serializer.
	 *
	 * <p>Note that the serializer instance must be serializable (as defined by
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
	 * given type at the KryoSerializer.
	 *
	 * @param type
	 * 		The class of the types serialized with the given serializer.
	 * @param serializerClass
	 * 		The class of the serializer to use.
	 */
	@SuppressWarnings("rawtypes")
	public void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer> serializerClass) {
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
	//  Time characteristic
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the time characteristic for all streams create from this environment, e.g., processing
	 * time, event time, or ingestion time.
	 *
	 * <p>If you set the characteristic to IngestionTime of EventTime this will set a default
	 * watermark update interval of 200 ms. If this is not applicable for your application
	 * you should change it using {@link ExecutionConfig#setAutoWatermarkInterval(long)}.
	 *
	 * @param characteristic The time characteristic.
	 */
	@PublicEvolving
	public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
		this.timeCharacteristic = Preconditions.checkNotNull(characteristic);
		if (characteristic == TimeCharacteristic.ProcessingTime) {
			getConfig().setAutoWatermarkInterval(0);
		} else {
			getConfig().setAutoWatermarkInterval(200);
		}
	}

	/**
	 * Gets the time characteristic.
	 *
	 * @see #setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)
	 *
	 * @return The time characteristic.
	 */
	@PublicEvolving
	public TimeCharacteristic getStreamTimeCharacteristic() {
		return timeCharacteristic;
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
	 * Creates a new data stream that contains the given elements. The elements must all be of the
	 * same type, for example, all of the {@link String} or {@link Integer}.
	 *
	 * <p>The framework will try and determine the exact type from the elements. In case of generic
	 * elements, it may be necessary to manually supply the type information via
	 * {@link #fromCollection(java.util.Collection, org.apache.flink.api.common.typeinfo.TypeInformation)}.
	 *
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e. a data
	 * stream source with a degree of parallelism one.
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
	 * Creates a new data set that contains the given elements. The framework will determine the type according to the
	 * based type user supplied. The elements should be the same or be the subclass to the based type.
	 * The sequence of elements must not be empty.
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism one.
	 *
	 * @param type
	 * 		The based class type in the collection.
	 * @param data
	 * 		The array of elements to create the data stream from.
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream representing the given array of elements
	 */
	@SafeVarargs
	public final <OUT> DataStreamSource<OUT> fromElements(Class<OUT> type, OUT... data) {
		if (data.length == 0) {
			throw new IllegalArgumentException("fromElements needs at least one element as argument");
		}

		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForClass(type);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + type.getName()
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
	 * {@link #fromCollection(java.util.Collection, org.apache.flink.api.common.typeinfo.TypeInformation)}.
	 *
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * parallelism one.
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
	 * i.e., a data stream source with a parallelism one.
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
			function = new FromElementsFunction<>(typeInfo.createSerializer(getConfig()), data);
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
	 * class (this is due to the fact that the Java compiler erases the generic type information).
	 *
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e.,
	 * a data stream source with a parallelism of one.
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
	 * {@link #fromCollection(java.util.Iterator, Class)} does not supply all type information.
	 *
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e.,
	 * a data stream source with a parallelism one.
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

		SourceFunction<OUT> function = new FromIteratorFunction<>(data);
		return addSource(function, "Collection Source", typeInfo);
	}

	/**
	 * Creates a new data stream that contains elements in the iterator. The iterator is splittable,
	 * allowing the framework to create a parallel data stream source that returns the elements in
	 * the iterator.
	 *
	 * <p>Because the iterator will remain unmodified until the actual execution happens, the type
	 * of data returned by the iterator must be given explicitly in the form of the type class
	 * (this is due to the fact that the Java compiler erases the generic type information).
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
	 * Creates a new data stream that contains elements in the iterator. The iterator is splittable,
	 * allowing the framework to create a parallel data stream source that returns the elements in
	 * the iterator.
	 *
	 * <p>Because the iterator will remain unmodified until the actual execution happens, the type
	 * of data returned by the iterator must be given explicitly in the form of the type
	 * information. This method is useful for cases where the type is generic. In that case, the
	 * type class (as given in
	 * {@link #fromParallelCollection(org.apache.flink.util.SplittableIterator, Class)} does not
	 * supply all type information.
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
		return addSource(new FromSplittableIteratorFunction<>(iterator), operatorName, typeInfo);
	}

	/**
	 * Reads the given file line-by-line and creates a data stream that contains a string with the
	 * contents of each such line. The file will be read with the system's default character set.
	 *
	 * <p><b>NOTES ON CHECKPOINTING: </b> The source monitors the path, creates the
	 * {@link org.apache.flink.core.fs.FileInputSplit FileInputSplits} to be processed, forwards
	 * them to the downstream {@link ContinuousFileReaderOperator readers} to read the actual data,
	 * and exits, without waiting for the readers to finish reading. This implies that no more
	 * checkpoint barriers are going to be forwarded after the source exits, thus having no
	 * checkpoints after that point.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return The data stream that represents the data read from the given file as text lines
	 */
	public DataStreamSource<String> readTextFile(String filePath) {
		return readTextFile(filePath, "UTF-8");
	}

	/**
	 * Reads the given file line-by-line and creates a data stream that contains a string with the
	 * contents of each such line. The {@link java.nio.charset.Charset} with the given name will be
	 * used to read the files.
	 *
	 * <p><b>NOTES ON CHECKPOINTING: </b> The source monitors the path, creates the
	 * {@link org.apache.flink.core.fs.FileInputSplit FileInputSplits} to be processed,
	 * forwards them to the downstream {@link ContinuousFileReaderOperator readers} to read the actual data,
	 * and exits, without waiting for the readers to finish reading. This implies that no more checkpoint
	 * barriers are going to be forwarded after the source exits, thus having no checkpoints after that point.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param charsetName
	 * 		The name of the character set used to read the file
	 * @return The data stream that represents the data read from the given file as text lines
	 */
	public DataStreamSource<String> readTextFile(String filePath, String charsetName) {
		Preconditions.checkNotNull(filePath, "The file path must not be null.");
		Preconditions.checkNotNull(filePath.isEmpty(), "The file path must not be empty.");

		TextInputFormat format = new TextInputFormat(new Path(filePath));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());
		TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
		format.setCharsetName(charsetName);

		return readFile(format, filePath, FileProcessingMode.PROCESS_ONCE, -1, typeInfo);
	}

	/**
	 * Reads the contents of the user-specified {@code filePath} based on the given {@link FileInputFormat}.
	 *
	 * <p>Since all data streams need specific information about their types, this method needs to determine the
	 * type of the data produced by the input format. It will attempt to determine the data type by reflection,
	 * unless the input format implements the {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable} interface.
	 * In the latter case, this method will invoke the
	 * {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable#getProducedType()} method to determine data
	 * type produced by the input format.
	 *
	 * <p><b>NOTES ON CHECKPOINTING: </b> The source monitors the path, creates the
	 * {@link org.apache.flink.core.fs.FileInputSplit FileInputSplits} to be processed,
	 * forwards them to the downstream {@link ContinuousFileReaderOperator readers} to read the actual data,
	 * and exits, without waiting for the readers to finish reading. This implies that no more checkpoint
	 * barriers are going to be forwarded after the source exits, thus having no checkpoints after that point.
	 *
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data read from the given file
	 */
	public <OUT> DataStreamSource<OUT> readFile(FileInputFormat<OUT> inputFormat,
												String filePath) {
		return readFile(inputFormat, filePath, FileProcessingMode.PROCESS_ONCE, -1);
	}

	/**
	 * Reads the contents of the user-specified {@code filePath} based on the given {@link FileInputFormat}. Depending
	 * on the provided {@link FileProcessingMode}.
	 *
	 * <p>See {@link #readFile(FileInputFormat, String, FileProcessingMode, long)}
	 *
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param watchType
	 * 		The mode in which the source should operate, i.e. monitor path and react to new data, or process once and exit
	 * @param interval
	 * 		In the case of periodic path monitoring, this specifies the interval (in millis) between consecutive path scans
	 * @param filter
	 * 		The files to be excluded from the processing
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data read from the given file
	 *
	 * @deprecated Use {@link FileInputFormat#setFilesFilter(FilePathFilter)} to set a filter and
	 * 		{@link StreamExecutionEnvironment#readFile(FileInputFormat, String, FileProcessingMode, long)}
	 *
	 */
	@PublicEvolving
	@Deprecated
	public <OUT> DataStreamSource<OUT> readFile(FileInputFormat<OUT> inputFormat,
												String filePath,
												FileProcessingMode watchType,
												long interval,
												FilePathFilter filter) {
		inputFormat.setFilesFilter(filter);

		TypeInformation<OUT> typeInformation;
		try {
			typeInformation = TypeExtractor.getInputFormatTypes(inputFormat);
		} catch (Exception e) {
			throw new InvalidProgramException("The type returned by the input format could not be " +
					"automatically determined. Please specify the TypeInformation of the produced type " +
					"explicitly by using the 'createInput(InputFormat, TypeInformation)' method instead.");
		}
		return readFile(inputFormat, filePath, watchType, interval, typeInformation);
	}

	/**
	 * Reads the contents of the user-specified {@code filePath} based on the given {@link FileInputFormat}. Depending
	 * on the provided {@link FileProcessingMode}, the source may periodically monitor (every {@code interval} ms) the path
	 * for new data ({@link FileProcessingMode#PROCESS_CONTINUOUSLY}), or process once the data currently in the path and
	 * exit ({@link FileProcessingMode#PROCESS_ONCE}). In addition, if the path contains files not to be processed, the user
	 * can specify a custom {@link FilePathFilter}. As a default implementation you can use
	 * {@link FilePathFilter#createDefaultFilter()}.
	 *
	 * <p>Since all data streams need specific information about their types, this method needs to determine the
	 * type of the data produced by the input format. It will attempt to determine the data type by reflection,
	 * unless the input format implements the {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable} interface.
	 * In the latter case, this method will invoke the
	 * {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable#getProducedType()} method to determine data
	 * type produced by the input format.
	 *
	 * <p><b>NOTES ON CHECKPOINTING: </b> If the {@code watchType} is set to {@link FileProcessingMode#PROCESS_ONCE},
	 * the source monitors the path <b>once</b>, creates the {@link org.apache.flink.core.fs.FileInputSplit FileInputSplits}
	 * to be processed, forwards them to the downstream {@link ContinuousFileReaderOperator readers} to read the actual data,
	 * and exits, without waiting for the readers to finish reading. This implies that no more checkpoint barriers
	 * are going to be forwarded after the source exits, thus having no checkpoints after that point.
	 *
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param watchType
	 * 		The mode in which the source should operate, i.e. monitor path and react to new data, or process once and exit
	 * @param interval
	 * 		In the case of periodic path monitoring, this specifies the interval (in millis) between consecutive path scans
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data read from the given file
	 */
	@PublicEvolving
	public <OUT> DataStreamSource<OUT> readFile(FileInputFormat<OUT> inputFormat,
												String filePath,
												FileProcessingMode watchType,
												long interval) {

		TypeInformation<OUT> typeInformation;
		try {
			typeInformation = TypeExtractor.getInputFormatTypes(inputFormat);
		} catch (Exception e) {
			throw new InvalidProgramException("The type returned by the input format could not be " +
					"automatically determined. Please specify the TypeInformation of the produced type " +
					"explicitly by using the 'createInput(InputFormat, TypeInformation)' method instead.");
		}
		return readFile(inputFormat, filePath, watchType, interval, typeInformation);
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
	 *
	 * @deprecated Use {@link #readFile(FileInputFormat, String, FileProcessingMode, long)} instead.
	 */
	@Deprecated
	@SuppressWarnings("deprecation")
	public DataStream<String> readFileStream(String filePath, long intervalMillis, FileMonitoringFunction.WatchType watchType) {
		DataStream<Tuple3<String, Long, Long>> source = addSource(new FileMonitoringFunction(
				filePath, intervalMillis, watchType), "Read File Stream source");

		return source.flatMap(new FileReadFunction());
	}

	/**
	 * Reads the contents of the user-specified {@code filePath} based on the given {@link FileInputFormat}.
	 * Depending on the provided {@link FileProcessingMode}, the source may periodically monitor (every {@code interval} ms)
	 * the path for new data ({@link FileProcessingMode#PROCESS_CONTINUOUSLY}), or process once the data currently in the
	 * path and exit ({@link FileProcessingMode#PROCESS_ONCE}). In addition, if the path contains files not to be processed,
	 * the user can specify a custom {@link FilePathFilter}. As a default implementation you can use
	 * {@link FilePathFilter#createDefaultFilter()}.
	 *
	 * <p><b>NOTES ON CHECKPOINTING: </b> If the {@code watchType} is set to {@link FileProcessingMode#PROCESS_ONCE},
	 * the source monitors the path <b>once</b>, creates the {@link org.apache.flink.core.fs.FileInputSplit FileInputSplits}
	 * to be processed, forwards them to the downstream {@link ContinuousFileReaderOperator readers} to read the actual data,
	 * and exits, without waiting for the readers to finish reading. This implies that no more checkpoint barriers
	 * are going to be forwarded after the source exits, thus having no checkpoints after that point.
	 *
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param filePath
	 * 		The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param watchType
	 * 		The mode in which the source should operate, i.e. monitor path and react to new data, or process once and exit
	 * @param typeInformation
	 * 		Information on the type of the elements in the output stream
	 * @param interval
	 * 		In the case of periodic path monitoring, this specifies the interval (in millis) between consecutive path scans
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data read from the given file
	 */
	@PublicEvolving
	public <OUT> DataStreamSource<OUT> readFile(FileInputFormat<OUT> inputFormat,
												String filePath,
												FileProcessingMode watchType,
												long interval,
												TypeInformation<OUT> typeInformation) {

		Preconditions.checkNotNull(inputFormat, "InputFormat must not be null.");
		Preconditions.checkNotNull(filePath, "The file path must not be null.");
		Preconditions.checkNotNull(filePath.isEmpty(), "The file path must not be empty.");

		inputFormat.setFilePath(filePath);
		return createFileInput(inputFormat, typeInformation, "Custom File Source", watchType, interval);
	}

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set. On the termination of the socket server connection retries can be
	 * initiated.
	 *
	 * <p>Let us note that the socket itself does not report on abort and as a consequence retries are only initiated when
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
	 *
	 * @deprecated Use {@link #socketTextStream(String, int, String, long)} instead.
	 */
	@Deprecated
	public DataStreamSource<String> socketTextStream(String hostname, int port, char delimiter, long maxRetry) {
		return socketTextStream(hostname, port, String.valueOf(delimiter), maxRetry);
	}

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set. On the termination of the socket server connection retries can be
	 * initiated.
	 *
	 * <p>Let us note that the socket itself does not report on abort and as a consequence retries are only initiated when
	 * the socket was gracefully terminated.
	 *
	 * @param hostname
	 * 		The host name which a server socket binds
	 * @param port
	 * 		The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * 		allocated.
	 * @param delimiter
	 * 		A string which splits received strings into records
	 * @param maxRetry
	 * 		The maximal retry interval in seconds while the program waits for a socket that is temporarily down.
	 * 		Reconnection is initiated every second. A number of 0 means that the reader is immediately terminated,
	 * 		while
	 * 		a	negative value ensures retrying forever.
	 * @return A data stream containing the strings received from the socket
	 */
	@PublicEvolving
	public DataStreamSource<String> socketTextStream(String hostname, int port, String delimiter, long maxRetry) {
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
	 *
	 * @deprecated Use {@link #socketTextStream(String, int, String)} instead.
	 */
	@Deprecated
	@SuppressWarnings("deprecation")
	public DataStreamSource<String> socketTextStream(String hostname, int port, char delimiter) {
		return socketTextStream(hostname, port, delimiter, 0);
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
	 * 		A string which splits received strings into records
	 * @return A data stream containing the strings received from the socket
	 */
	@PublicEvolving
	public DataStreamSource<String> socketTextStream(String hostname, int port, String delimiter) {
		return socketTextStream(hostname, port, delimiter, 0);
	}

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set, using"\n" as delimiter. The reader is terminated immediately when
	 * the socket is down.
	 *
	 * @param hostname
	 * 		The host name which a server socket binds
	 * @param port
	 * 		The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * 		allocated.
	 * @return A data stream containing the strings received from the socket
	 */
	@PublicEvolving
	public DataStreamSource<String> socketTextStream(String hostname, int port) {
		return socketTextStream(hostname, port, "\n");
	}

	/**
	 * Generic method to create an input data stream with {@link org.apache.flink.api.common.io.InputFormat}.
	 *
	 * <p>Since all data streams need specific information about their types, this method needs to determine the
	 * type of the data produced by the input format. It will attempt to determine the data type by reflection,
	 * unless the input format implements the {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable} interface.
	 * In the latter case, this method will invoke the
	 * {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable#getProducedType()} method to determine data
	 * type produced by the input format.
	 *
	 * <p><b>NOTES ON CHECKPOINTING: </b> In the case of a {@link FileInputFormat}, the source
	 * (which executes the {@link ContinuousFileMonitoringFunction}) monitors the path, creates the
	 * {@link org.apache.flink.core.fs.FileInputSplit FileInputSplits} to be processed, forwards
	 * them to the downstream {@link ContinuousFileReaderOperator} to read the actual data, and exits,
	 * without waiting for the readers to finish reading. This implies that no more checkpoint
	 * barriers are going to be forwarded after the source exits, thus having no checkpoints.
	 *
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data created by the input format
	 */
	@PublicEvolving
	public <OUT> DataStreamSource<OUT> createInput(InputFormat<OUT, ?> inputFormat) {
		return createInput(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat));
	}

	/**
	 * Generic method to create an input data stream with {@link org.apache.flink.api.common.io.InputFormat}.
	 *
	 * <p>The data stream is typed to the given TypeInformation. This method is intended for input formats
	 * where the return type cannot be determined by reflection analysis, and that do not implement the
	 * {@link org.apache.flink.api.java.typeutils.ResultTypeQueryable} interface.
	 *
	 * <p><b>NOTES ON CHECKPOINTING: </b> In the case of a {@link FileInputFormat}, the source
	 * (which executes the {@link ContinuousFileMonitoringFunction}) monitors the path, creates the
	 * {@link org.apache.flink.core.fs.FileInputSplit FileInputSplits} to be processed, forwards
	 * them to the downstream {@link ContinuousFileReaderOperator} to read the actual data, and exits,
	 * without waiting for the readers to finish reading. This implies that no more checkpoint
	 * barriers are going to be forwarded after the source exits, thus having no checkpoints.
	 *
	 * @param inputFormat
	 * 		The input format used to create the data stream
	 * @param typeInfo
	 * 		The information about the type of the output type
	 * @param <OUT>
	 * 		The type of the returned data stream
	 * @return The data stream that represents the data created by the input format
	 */
	@PublicEvolving
	public <OUT> DataStreamSource<OUT> createInput(InputFormat<OUT, ?> inputFormat, TypeInformation<OUT> typeInfo) {
		DataStreamSource<OUT> source;

		if (inputFormat instanceof FileInputFormat) {
			@SuppressWarnings("unchecked")
			FileInputFormat<OUT> format = (FileInputFormat<OUT>) inputFormat;

			source = createFileInput(format, typeInfo, "Custom File source",
					FileProcessingMode.PROCESS_ONCE, -1);
		} else {
			source = createInput(inputFormat, typeInfo, "Custom Source");
		}
		return source;
	}

	private <OUT> DataStreamSource<OUT> createInput(InputFormat<OUT, ?> inputFormat,
													TypeInformation<OUT> typeInfo,
													String sourceName) {

		InputFormatSourceFunction<OUT> function = new InputFormatSourceFunction<>(inputFormat, typeInfo);
		return addSource(function, sourceName, typeInfo);
	}

	private <OUT> DataStreamSource<OUT> createFileInput(FileInputFormat<OUT> inputFormat,
														TypeInformation<OUT> typeInfo,
														String sourceName,
														FileProcessingMode monitoringMode,
														long interval) {

		Preconditions.checkNotNull(inputFormat, "Unspecified file input format.");
		Preconditions.checkNotNull(typeInfo, "Unspecified output type information.");
		Preconditions.checkNotNull(sourceName, "Unspecified name for the source.");
		Preconditions.checkNotNull(monitoringMode, "Unspecified monitoring mode.");

		Preconditions.checkArgument(monitoringMode.equals(FileProcessingMode.PROCESS_ONCE) ||
				interval >= ContinuousFileMonitoringFunction.MIN_MONITORING_INTERVAL,
			"The path monitoring interval cannot be less than " +
					ContinuousFileMonitoringFunction.MIN_MONITORING_INTERVAL + " ms.");

		ContinuousFileMonitoringFunction<OUT> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(inputFormat, monitoringMode, getParallelism(), interval);

		ContinuousFileReaderOperator<OUT> reader =
			new ContinuousFileReaderOperator<>(inputFormat);

		SingleOutputStreamOperator<OUT> source = addSource(monitoringFunction, sourceName)
				.transform("Split Reader: " + sourceName, typeInfo, reader);

		return new DataStreamSource<>(source);
	}

	/**
	 * Adds a Data Source to the streaming topology.
	 *
	 * <p>By default sources have a parallelism of 1. To enable parallel execution, the user defined source should
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

		if (typeInfo == null) {
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
		StreamSource<OUT, ?> sourceOperator;
		if (function instanceof StoppableFunction) {
			sourceOperator = new StoppableStreamSource<>(cast2StoppableSourceFunction(function));
		} else {
			sourceOperator = new StreamSource<>(function);
		}

		return new DataStreamSource<>(this, typeInfo, sourceOperator, isParallel, sourceName);
	}

	/**
	 * Casts the source function into a SourceFunction implementing the StoppableFunction.
	 *
	 * <p>This method should only be used if the source function was checked to implement the
	 * {@link StoppableFunction} interface.
	 *
	 * @param sourceFunction Source function to cast
	 * @param <OUT> Output type of source function
	 * @param <T> Union type of SourceFunction and StoppableFunction
	 * @return The casted source function so that it's type implements the StoppableFunction
	 */
	@SuppressWarnings("unchecked")
	private <OUT, T extends SourceFunction<OUT> & StoppableFunction> T cast2StoppableSourceFunction(SourceFunction<OUT> sourceFunction) {
		return (T) sourceFunction;
	}

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 *
	 * <p>The program execution will be logged and displayed with a generated
	 * default name.
	 *
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception which occurs during job execution.
	 */
	public JobExecutionResult execute() throws Exception {
		return execute(DEFAULT_JOB_NAME);
	}

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 *
	 * <p>The program execution will be logged and displayed with the provided name
	 *
	 * @param jobName
	 * 		Desired name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception which occurs during job execution.
	 */
	public abstract JobExecutionResult execute(String jobName) throws Exception;

	/**
	 * Getter of the {@link org.apache.flink.streaming.api.graph.StreamGraph} of the streaming job.
	 *
	 * @return The streamgraph representing the transformations
	 */
	@Internal
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
	@Internal
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
	 * <p>When calling {@link #execute()} only the operators that where previously added to the list
	 * are executed.
	 *
	 * <p>This is not meant to be used by users. The API methods that create operators must call
	 * this method.
	 */
	@Internal
	public void addOperator(StreamTransformation<?> transformation) {
		Preconditions.checkNotNull(transformation, "transformation must not be null.");
		this.transformations.add(transformation);
	}

	// --------------------------------------------------------------------------------------------
	//  Factory methods for ExecutionEnvironments
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

		// because the streaming project depends on "flink-clients" (and not the other way around)
		// we currently need to intercept the data set environment and create a dependent stream env.
		// this should be fixed once we rework the project dependencies

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		if (env instanceof ContextEnvironment) {
			return new StreamContextEnvironment((ContextEnvironment) env);
		} else if (env instanceof OptimizerPlanEnvironment | env instanceof PreviewPlanEnvironment) {
			return new StreamPlanEnvironment(env);
		} else {
			return createLocalEnvironment();
		}
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
		return currentEnvironment;
	}

	/**
	 * Creates a {@link LocalStreamEnvironment} for local program execution that also starts the
	 * web monitoring UI.
	 *
	 * <p>The local execution environment will run the program in a multi-threaded fashion in
	 * the same JVM as the environment was created in. It will use the parallelism specified in the
	 * parameter.
	 *
	 * <p>If the configuration key 'jobmanager.web.port' was set in the configuration, that particular
	 * port will be used for the web UI. Otherwise, the default port (8081) will be used.
	 */
	@PublicEvolving
	public static StreamExecutionEnvironment createLocalEnvironmentWithWebUI(Configuration conf) {
		checkNotNull(conf, "conf");

		if (!conf.containsKey(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY)) {
			int port = ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT;
			conf.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, port);
		}
		conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

		LocalStreamEnvironment localEnv = new LocalStreamEnvironment(conf);
		localEnv.setParallelism(defaultLocalParallelism);

		return localEnv;
	}

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
	public static StreamExecutionEnvironment createRemoteEnvironment(
			String host, int port, String... jarFiles) {
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
	public static StreamExecutionEnvironment createRemoteEnvironment(
			String host, int port, int parallelism, String... jarFiles) {
		RemoteStreamEnvironment env = new RemoteStreamEnvironment(host, port, jarFiles);
		env.setParallelism(parallelism);
		return env;
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
	 * @param clientConfig
	 * 		The configuration used by the client that connects to the remote cluster.
	 * @param jarFiles
	 * 		The JAR files with code that needs to be shipped to the
	 * 		cluster. If the program uses user-defined functions,
	 * 		user-defined input formats, or any libraries, those must be
	 * 		provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static StreamExecutionEnvironment createRemoteEnvironment(
			String host, int port, Configuration clientConfig, String... jarFiles) {
		return new RemoteStreamEnvironment(host, port, clientConfig, jarFiles);
	}

	/**
	 * Gets the default parallelism that will be used for the local execution environment created by
	 * {@link #createLocalEnvironment()}.
	 *
	 * @return The default local parallelism
	 */
	@PublicEvolving
	public static int getDefaultLocalParallelism() {
		return defaultLocalParallelism;
	}

	/**
	 * Sets the default parallelism that will be used for the local execution
	 * environment created by {@link #createLocalEnvironment()}.
	 *
	 * @param parallelism The parallelism to use as the default local parallelism.
	 */
	@PublicEvolving
	public static void setDefaultLocalParallelism(int parallelism) {
		defaultLocalParallelism = parallelism;
	}

	// --------------------------------------------------------------------------------------------
	//  Methods to control the context and local environments for execution from packaged programs
	// --------------------------------------------------------------------------------------------

	protected static void initializeContextEnvironment(StreamExecutionEnvironmentFactory ctx) {
		contextEnvironmentFactory = ctx;
	}

	protected static void resetContextEnvironment() {
		contextEnvironmentFactory = null;
	}
}
