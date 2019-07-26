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

package org.apache.flink.api.java;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.IteratorInputFormat;
import org.apache.flink.api.java.io.ParallelIteratorInputFormat;
import org.apache.flink.api.java.io.PrimitiveInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextValueInputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.api.java.operators.OperatorTranslation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SplittableIterator;
import org.apache.flink.util.Visitor;

import com.esotericsoftware.kryo.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The ExecutionEnvironment is the context in which a program is executed. A
 * {@link LocalEnvironment} will cause execution in the current JVM, a
 * {@link RemoteEnvironment} will cause execution on a remote setup.
 *
 * <p>The environment provides methods to control the job execution (such as setting the parallelism)
 * and to interact with the outside world (data access).
 *
 * <p>Please note that the execution environment needs strong type information for the input and return types
 * of all operations that are executed. This means that the environments needs to know that the return
 * value of an operation is for example a Tuple of String and Integer.
 * Because the Java compiler throws much of the generic type information away, most methods attempt to re-
 * obtain that information using reflection. In certain cases, it may be necessary to manually supply that
 * information to some of the methods.
 *
 * @see LocalEnvironment
 * @see RemoteEnvironment
 */
@Public
public abstract class ExecutionEnvironment {

	/** The logger used by the environment and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);

	/** The environment of the context (local by default, cluster if invoked through command line). */
	private static ExecutionEnvironmentFactory contextEnvironmentFactory = null;

	/** The ThreadLocal used to store {@link ExecutionEnvironmentFactory}. */
	private static final ThreadLocal<ExecutionEnvironmentFactory> threadLocalContextEnvironmentFactory = new ThreadLocal<>();

	/** The default parallelism used by local environments. */
	private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();

	// --------------------------------------------------------------------------------------------

	private final List<DataSink<?>> sinks = new ArrayList<>();

	private final List<Tuple2<String, DistributedCacheEntry>> cacheFile = new ArrayList<>();

	private final ExecutionConfig config = new ExecutionConfig();

	/** Result from the latest execution, to make it retrievable when using eager execution methods. */
	protected JobExecutionResult lastJobExecutionResult;

	/** The ID of the session, defined by this execution environment. Sessions and Jobs are same in
	 *  Flink, as Jobs can consist of multiple parts that are attached to the growing dataflow graph. */
	protected JobID jobID;

	/** The session timeout in seconds. */
	protected long sessionTimeout;

	/** Flag to indicate whether sinks have been cleared in previous executions. */
	private boolean wasExecuted = false;

	/**
	 * Creates a new Execution Environment.
	 */
	protected ExecutionEnvironment() {
		jobID = JobID.generate();
	}

	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the config object that defines execution parameters.
	 *
	 * @return The environment's execution configuration.
	 */
	public ExecutionConfig getConfig() {
		return config;
	}

	/**
	 * Gets the parallelism with which operation are executed by default. Operations can
	 * individually override this value to use a specific parallelism via
	 * {@link Operator#setParallelism(int)}. Other operations may need to run with a different
	 * parallelism - for example calling
	 * {@link DataSet#reduce(org.apache.flink.api.common.functions.ReduceFunction)} over the entire
	 * set will insert eventually an operation that runs non-parallel (parallelism of one).
	 *
	 * @return The parallelism used by operations, unless they override that value. This method
	 *         returns {@link ExecutionConfig#PARALLELISM_DEFAULT}, if the environment's default parallelism should be used.
	 */
	public int getParallelism() {
		return config.getParallelism();
	}

	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run with
	 * x parallel instances.
	 *
	 * <p>This method overrides the default parallelism for this environment.
	 * The {@link LocalEnvironment} uses by default a value equal to the number of hardware
	 * contexts (CPU cores / threads). When executing the program via the command line client
	 * from a JAR file, the default parallelism is the one configured for that setup.
	 *
	 * @param parallelism The parallelism
	 */
	public void setParallelism(int parallelism) {
		config.setParallelism(parallelism);
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
	 * Sets the number of times that failed tasks are re-executed. A value of zero
	 * effectively disables fault tolerance. A value of {@code -1} indicates that the system
	 * default value (as defined in the configuration) should be used.
	 *
	 * @param numberOfExecutionRetries The number of times the system will try to re-execute failed tasks.
	 *
	 * @deprecated This method will be replaced by {@link #setRestartStrategy}. The
	 * {@link RestartStrategies.FixedDelayRestartStrategyConfiguration} contains the number of
	 * execution retries.
	 */
	@Deprecated
	@PublicEvolving
	public void setNumberOfExecutionRetries(int numberOfExecutionRetries) {
		config.setNumberOfExecutionRetries(numberOfExecutionRetries);
	}

	/**
	 * Gets the number of times the system will try to re-execute failed tasks. A value
	 * of {@code -1} indicates that the system default value (as defined in the configuration)
	 * should be used.
	 *
	 * @return The number of times the system will try to re-execute failed tasks.
	 *
	 * @deprecated This method will be replaced by {@link #getRestartStrategy}. The
	 * {@link RestartStrategies.FixedDelayRestartStrategyConfiguration} contains the number of
	 * execution retries.
	 */
	@Deprecated
	@PublicEvolving
	public int getNumberOfExecutionRetries() {
		return config.getNumberOfExecutionRetries();
	}

	/**
	 * Returns the {@link org.apache.flink.api.common.JobExecutionResult} of the last executed job.
	 *
	 * @return The execution result from the latest job execution.
	 */
	public JobExecutionResult getLastJobExecutionResult(){
		return this.lastJobExecutionResult;
	}

	// --------------------------------------------------------------------------------------------
	//  Session Management
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the JobID by which this environment is identified. The JobID sets the execution context
	 * in the cluster or local environment.
	 *
	 * @return The JobID of this environment.
	 * @see #getIdString()
	 */
	@PublicEvolving
	public JobID getId() {
		return this.jobID;
	}

	/**
	 * Gets the JobID by which this environment is identified, as a string.
	 *
	 * @return The JobID as a string.
	 * @see #getId()
	 */
	@PublicEvolving
	public String getIdString() {
		return this.jobID.toString();
	}

	/**
	 * Sets the session timeout to hold the intermediate results of a job. This only
	 * applies the updated timeout in future executions.
	 *
	 * @param timeout The timeout, in seconds.
	 */
	@PublicEvolving
	public void setSessionTimeout(long timeout) {
		throw new IllegalStateException("Support for sessions is currently disabled. " +
				"It will be enabled in future Flink versions.");
		// Session management is disabled, revert this commit to enable
		//if (timeout < 0) {
		//	throw new IllegalArgumentException("The session timeout must not be less than zero.");
		//}
		//this.sessionTimeout = timeout;
	}

	/**
	 * Gets the session timeout for this environment. The session timeout defines for how long
	 * after an execution, the job and its intermediate results will be kept for future
	 * interactions.
	 *
	 * @return The session timeout, in seconds.
	 */
	@PublicEvolving
	public long getSessionTimeout() {
		return sessionTimeout;
	}

	/**
	 * Starts a new session, discarding the previous data flow and all of its intermediate results.
	 */
	@PublicEvolving
	public abstract void startNewSession() throws Exception;

	// --------------------------------------------------------------------------------------------
	//  Registry for types and serializers
	// --------------------------------------------------------------------------------------------

	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * <p>Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public <T extends Serializer<?> & Serializable>void addDefaultKryoSerializer(Class<?> type, T serializer) {
		config.addDefaultKryoSerializer(type, serializer);
	}

	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializerClass The class of the serializer to use.
	 */
	public void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		config.addDefaultKryoSerializer(type, serializerClass);
	}

	/**
	 * Registers the given type with a Kryo Serializer.
	 *
	 * <p>Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public <T extends Serializer<?> & Serializable>void registerTypeWithKryoSerializer(Class<?> type, T serializer) {
		config.registerTypeWithKryoSerializer(type, serializer);
	}

	/**
	 * Registers the given Serializer via its class as a serializer for the given type at the KryoSerializer.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializerClass The class of the serializer to use.
	 */
	public void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		config.registerTypeWithKryoSerializer(type, serializerClass);
	}

	/**
	 * Registers the given type with the serialization stack. If the type is eventually
	 * serialized as a POJO, then the type is registered with the POJO serializer. If the
	 * type ends up being serialized with Kryo, then it will be registered at Kryo to make
	 * sure that only tags are written.
	 *
	 * @param type The class of the type to register.
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
	//  Data set creations
	// --------------------------------------------------------------------------------------------

	// ---------------------------------- Text Input Format ---------------------------------------

	/**
	 * Creates a {@link DataSet} that represents the Strings produced by reading the given file line wise.
	 * The file will be read with the UTF-8 character set.
	 *
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return A {@link DataSet} that represents the data read from the given file as text lines.
	 */
	public DataSource<String> readTextFile(String filePath) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		return new DataSource<>(this, new TextInputFormat(new Path(filePath)), BasicTypeInfo.STRING_TYPE_INFO, Utils.getCallLocationName());
	}

	/**
	 * Creates a {@link DataSet} that represents the Strings produced by reading the given file line wise.
	 * The {@link java.nio.charset.Charset} with the given name will be used to read the files.
	 *
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @param charsetName The name of the character set used to read the file.
	 * @return A {@link DataSet} that represents the data read from the given file as text lines.
	 */
	public DataSource<String> readTextFile(String filePath, String charsetName) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		TextInputFormat format = new TextInputFormat(new Path(filePath));
		format.setCharsetName(charsetName);
		return new DataSource<>(this, format, BasicTypeInfo.STRING_TYPE_INFO, Utils.getCallLocationName());
	}

	// -------------------------- Text Input Format With String Value------------------------------

	/**
	 * Creates a {@link DataSet} that represents the Strings produced by reading the given file line wise.
	 * This method is similar to {@link #readTextFile(String)}, but it produces a DataSet with mutable
	 * {@link StringValue} objects, rather than Java Strings. StringValues can be used to tune implementations
	 * to be less object and garbage collection heavy.
	 *
	 * <p>The file will be read with the UTF-8 character set.
	 *
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return A {@link DataSet} that represents the data read from the given file as text lines.
	 */
	public DataSource<StringValue> readTextFileWithValue(String filePath) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		return new DataSource<>(this, new TextValueInputFormat(new Path(filePath)), new ValueTypeInfo<>(StringValue.class), Utils.getCallLocationName());
	}

	/**
	 * Creates a {@link DataSet} that represents the Strings produced by reading the given file line wise.
	 * This method is similar to {@link #readTextFile(String, String)}, but it produces a DataSet with mutable
	 * {@link StringValue} objects, rather than Java Strings. StringValues can be used to tune implementations
	 * to be less object and garbage collection heavy.
	 *
	 * <p>The {@link java.nio.charset.Charset} with the given name will be used to read the files.
	 *
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @param charsetName The name of the character set used to read the file.
	 * @param skipInvalidLines A flag to indicate whether to skip lines that cannot be read with the given character set.
	 *
	 * @return A DataSet that represents the data read from the given file as text lines.
	 */
	public DataSource<StringValue> readTextFileWithValue(String filePath, String charsetName, boolean skipInvalidLines) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		TextValueInputFormat format = new TextValueInputFormat(new Path(filePath));
		format.setCharsetName(charsetName);
		format.setSkipInvalidLines(skipInvalidLines);
		return new DataSource<>(this, format, new ValueTypeInfo<>(StringValue.class), Utils.getCallLocationName());
	}

	// ----------------------------------- Primitive Input Format ---------------------------------------

	/**
	 * Creates a {@link DataSet} that represents the primitive type produced by reading the given file line wise.
	 * This method is similar to {@link #readCsvFile(String)} with single field, but it produces a DataSet not through
	 * {@link org.apache.flink.api.java.tuple.Tuple1}.
	 *
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @param typeClass The primitive type class to be read.
	 * @return A {@link DataSet} that represents the data read from the given file as primitive type.
	 */
	public <X> DataSource<X> readFileOfPrimitives(String filePath, Class<X> typeClass) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		return new DataSource<>(this, new PrimitiveInputFormat<>(new Path(filePath), typeClass), TypeExtractor.getForClass(typeClass), Utils.getCallLocationName());
	}

	/**
	 * Creates a {@link DataSet} that represents the primitive type produced by reading the given file in delimited way.
	 * This method is similar to {@link #readCsvFile(String)} with single field, but it produces a DataSet not through
	 * {@link org.apache.flink.api.java.tuple.Tuple1}.
	 *
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @param delimiter The delimiter of the given file.
	 * @param typeClass The primitive type class to be read.
	 * @return A {@link DataSet} that represents the data read from the given file as primitive type.
	 */
	public <X> DataSource<X> readFileOfPrimitives(String filePath, String delimiter, Class<X> typeClass) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		return new DataSource<>(this, new PrimitiveInputFormat<>(new Path(filePath), delimiter, typeClass), TypeExtractor.getForClass(typeClass), Utils.getCallLocationName());
	}

	// ----------------------------------- CSV Input Format ---------------------------------------

	/**
	 * Creates a CSV reader to read a comma separated value (CSV) file. The reader has options to
	 * define parameters and field types and will eventually produce the DataSet that corresponds to
	 * the read and parsed CSV input.
	 *
	 * @param filePath The path of the CSV file.
	 * @return A CsvReader that can be used to configure the CSV input.
	 */
	public CsvReader readCsvFile(String filePath) {
		return new CsvReader(filePath, this);
	}

	// ------------------------------------ File Input Format -----------------------------------------

	public <X> DataSource<X> readFile(FileInputFormat<X> inputFormat, String filePath) {
		if (inputFormat == null) {
			throw new IllegalArgumentException("InputFormat must not be null.");
		}
		if (filePath == null) {
			throw new IllegalArgumentException("The file path must not be null.");
		}

		inputFormat.setFilePath(new Path(filePath));
		try {
			return createInput(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat));
		}
		catch (Exception e) {
			throw new InvalidProgramException("The type returned by the input format could not be automatically determined. " +
					"Please specify the TypeInformation of the produced type explicitly by using the " +
					"'createInput(InputFormat, TypeInformation)' method instead.");
		}
	}

	// ----------------------------------- Generic Input Format ---------------------------------------

	/**
	 * Generic method to create an input {@link DataSet} with in {@link InputFormat}. The DataSet will not be
	 * immediately created - instead, this method returns a DataSet that will be lazily created from
	 * the input format once the program is executed.
	 *
	 * <p>Since all data sets need specific information about their types, this method needs to determine
	 * the type of the data produced by the input format. It will attempt to determine the data type
	 * by reflection, unless the input format implements the {@link ResultTypeQueryable} interface.
	 * In the latter case, this method will invoke the {@link ResultTypeQueryable#getProducedType()}
	 * method to determine data type produced by the input format.
	 *
	 * @param inputFormat The input format used to create the data set.
	 * @return A {@link DataSet} that represents the data created by the input format.
	 *
	 * @see #createInput(InputFormat, TypeInformation)
	 */
	public <X> DataSource<X> createInput(InputFormat<X, ?> inputFormat) {
		if (inputFormat == null) {
			throw new IllegalArgumentException("InputFormat must not be null.");
		}

		try {
			return createInput(inputFormat, TypeExtractor.getInputFormatTypes(inputFormat));
		}
		catch (Exception e) {
			throw new InvalidProgramException("The type returned by the input format could not be automatically determined. " +
					"Please specify the TypeInformation of the produced type explicitly by using the " +
					"'createInput(InputFormat, TypeInformation)' method instead.", e);
		}
	}

	/**
	 * Generic method to create an input DataSet with in {@link InputFormat}. The {@link DataSet} will not be
	 * immediately created - instead, this method returns a {@link DataSet} that will be lazily created from
	 * the input format once the program is executed.
	 *
	 * <p>The {@link DataSet} is typed to the given TypeInformation. This method is intended for input formats that
	 * where the return type cannot be determined by reflection analysis, and that do not implement the
	 * {@link ResultTypeQueryable} interface.
	 *
	 * @param inputFormat The input format used to create the data set.
	 * @return A {@link DataSet} that represents the data created by the input format.
	 *
	 * @see #createInput(InputFormat)
	 */
	public <X> DataSource<X> createInput(InputFormat<X, ?> inputFormat, TypeInformation<X> producedType) {
		if (inputFormat == null) {
			throw new IllegalArgumentException("InputFormat must not be null.");
		}

		if (producedType == null) {
			throw new IllegalArgumentException("Produced type information must not be null.");
		}

		return new DataSource<>(this, inputFormat, producedType, Utils.getCallLocationName());
	}

	// ----------------------------------- Collection ---------------------------------------

	/**
	 * Creates a DataSet from the given non-empty collection. The type of the data set is that
	 * of the elements in the collection.
	 *
	 * <p>The framework will try and determine the exact type from the collection elements.
	 * In case of generic elements, it may be necessary to manually supply the type information
	 * via {@link #fromCollection(Collection, TypeInformation)}.
	 *
	 * <p>Note that this operation will result in a non-parallel data source, i.e. a data source with
	 * a parallelism of one.
	 *
	 * @param data The collection of elements to create the data set from.
	 * @return A DataSet representing the given collection.
	 *
	 * @see #fromCollection(Collection, TypeInformation)
	 */
	public <X> DataSource<X> fromCollection(Collection<X> data) {
		if (data == null) {
			throw new IllegalArgumentException("The data must not be null.");
		}
		if (data.size() == 0) {
			throw new IllegalArgumentException("The size of the collection must not be empty.");
		}

		X firstValue = data.iterator().next();

		TypeInformation<X> type = TypeExtractor.getForObject(firstValue);
		CollectionInputFormat.checkCollection(data, type.getTypeClass());
		return new DataSource<>(this, new CollectionInputFormat<>(data, type.createSerializer(config)), type, Utils.getCallLocationName());
	}

	/**
	 * Creates a DataSet from the given non-empty collection. Note that this operation will result
	 * in a non-parallel data source, i.e. a data source with a parallelism of one.
	 *
	 * <p>The returned DataSet is typed to the given TypeInformation.
	 *
	 * @param data The collection of elements to create the data set from.
	 * @param type The TypeInformation for the produced data set.
	 * @return A DataSet representing the given collection.
	 *
	 * @see #fromCollection(Collection)
	 */
	public <X> DataSource<X> fromCollection(Collection<X> data, TypeInformation<X> type) {
		return fromCollection(data, type, Utils.getCallLocationName());
	}

	private <X> DataSource<X> fromCollection(Collection<X> data, TypeInformation<X> type, String callLocationName) {
		CollectionInputFormat.checkCollection(data, type.getTypeClass());
		return new DataSource<>(this, new CollectionInputFormat<>(data, type.createSerializer(config)), type, callLocationName);
	}

	/**
	 * Creates a DataSet from the given iterator. Because the iterator will remain unmodified until
	 * the actual execution happens, the type of data returned by the iterator must be given
	 * explicitly in the form of the type class (this is due to the fact that the Java compiler
	 * erases the generic type information).
	 *
	 * <p>Note that this operation will result in a non-parallel data source, i.e. a data source with
	 * a parallelism of one.
	 *
	 * @param data The collection of elements to create the data set from.
	 * @param type The class of the data produced by the iterator. Must not be a generic class.
	 * @return A DataSet representing the elements in the iterator.
	 *
	 * @see #fromCollection(Iterator, TypeInformation)
	 */
	public <X> DataSource<X> fromCollection(Iterator<X> data, Class<X> type) {
		return fromCollection(data, TypeExtractor.getForClass(type));
	}

	/**
	 * Creates a DataSet from the given iterator. Because the iterator will remain unmodified until
	 * the actual execution happens, the type of data returned by the iterator must be given
	 * explicitly in the form of the type information. This method is useful for cases where the type
	 * is generic. In that case, the type class (as given in {@link #fromCollection(Iterator, Class)}
	 * does not supply all type information.
	 *
	 * <p>Note that this operation will result in a non-parallel data source, i.e. a data source with
	 * a parallelism of one.
	 *
	 * @param data The collection of elements to create the data set from.
	 * @param type The TypeInformation for the produced data set.
	 * @return A DataSet representing the elements in the iterator.
	 *
	 * @see #fromCollection(Iterator, Class)
	 */
	public <X> DataSource<X> fromCollection(Iterator<X> data, TypeInformation<X> type) {
		return new DataSource<>(this, new IteratorInputFormat<>(data), type, Utils.getCallLocationName());
	}

	/**
	 * Creates a new data set that contains the given elements. The elements must all be of the same type,
	 * for example, all of the {@link String} or {@link Integer}. The sequence of elements must not be empty.
	 *
	 * <p>The framework will try and determine the exact type from the collection elements.
	 * In case of generic elements, it may be necessary to manually supply the type information
	 * via {@link #fromCollection(Collection, TypeInformation)}.
	 *
	 * <p>Note that this operation will result in a non-parallel data source, i.e. a data source with
	 * a parallelism of one.
	 *
	 * @param data The elements to make up the data set.
	 * @return A DataSet representing the given list of elements.
	 */
	@SafeVarargs
	public final <X> DataSource<X> fromElements(X... data) {
		if (data == null) {
			throw new IllegalArgumentException("The data must not be null.");
		}
		if (data.length == 0) {
			throw new IllegalArgumentException("The number of elements must not be zero.");
		}

		TypeInformation<X> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(data[0]);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + data[0].getClass().getName()
					+ "; please specify the TypeInformation manually via "
					+ "ExecutionEnvironment#fromElements(Collection, TypeInformation)", e);
		}

		return fromCollection(Arrays.asList(data), typeInfo, Utils.getCallLocationName());
	}

	/**
	 * Creates a new data set that contains the given elements. The framework will determine the type according to the
	 * based type user supplied. The elements should be the same or be the subclass to the based type.
	 * The sequence of elements must not be empty.
	 * Note that this operation will result in a non-parallel data source, i.e. a data source with
	 * a parallelism of one.
	 *
	 * @param type The base class type for every element in the collection.
	 * @param data The elements to make up the data set.
	 * @return A DataSet representing the given list of elements.
	 */
	@SafeVarargs
	public final <X> DataSource<X> fromElements(Class<X> type, X... data) {
		if (data == null) {
			throw new IllegalArgumentException("The data must not be null.");
		}
		if (data.length == 0) {
			throw new IllegalArgumentException("The number of elements must not be zero.");
		}

		TypeInformation<X> typeInfo;
		try {
			typeInfo = TypeExtractor.getForClass(type);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + type.getName()
					+ "; please specify the TypeInformation manually via "
					+ "ExecutionEnvironment#fromElements(Collection, TypeInformation)", e);
		}

		return fromCollection(Arrays.asList(data), typeInfo, Utils.getCallLocationName());
	}

	/**
	 * Creates a new data set that contains elements in the iterator. The iterator is splittable, allowing the
	 * framework to create a parallel data source that returns the elements in the iterator.
	 *
	 * <p>Because the iterator will remain unmodified until the actual execution happens, the type of data
	 * returned by the iterator must be given explicitly in the form of the type class (this is due to the
	 * fact that the Java compiler erases the generic type information).
	 *
	 * @param iterator The iterator that produces the elements of the data set.
	 * @param type The class of the data produced by the iterator. Must not be a generic class.
	 * @return A DataSet representing the elements in the iterator.
	 *
	 * @see #fromParallelCollection(SplittableIterator, TypeInformation)
	 */
	public <X> DataSource<X> fromParallelCollection(SplittableIterator<X> iterator, Class<X> type) {
		return fromParallelCollection(iterator, TypeExtractor.getForClass(type));
	}

	/**
	 * Creates a new data set that contains elements in the iterator. The iterator is splittable, allowing the
	 * framework to create a parallel data source that returns the elements in the iterator.
	 *
	 * <p>Because the iterator will remain unmodified until the actual execution happens, the type of data
	 * returned by the iterator must be given explicitly in the form of the type information.
	 * This method is useful for cases where the type is generic. In that case, the type class
	 * (as given in {@link #fromParallelCollection(SplittableIterator, Class)} does not supply all type information.
	 *
	 * @param iterator The iterator that produces the elements of the data set.
	 * @param type The TypeInformation for the produced data set.
	 * @return A DataSet representing the elements in the iterator.
	 *
	 * @see #fromParallelCollection(SplittableIterator, Class)
	 */
	public <X> DataSource<X> fromParallelCollection(SplittableIterator<X> iterator, TypeInformation<X> type) {
		return fromParallelCollection(iterator, type, Utils.getCallLocationName());
	}

	// private helper for passing different call location names
	private <X> DataSource<X> fromParallelCollection(SplittableIterator<X> iterator, TypeInformation<X> type, String callLocationName) {
		return new DataSource<>(this, new ParallelIteratorInputFormat<>(iterator), type, callLocationName);
	}

	/**
	 * Creates a new data set that contains a sequence of numbers. The data set will be created in parallel,
	 * so there is no guarantee about the order of the elements.
	 *
	 * @param from The number to start at (inclusive).
	 * @param to The number to stop at (inclusive).
	 * @return A DataSet, containing all number in the {@code [from, to]} interval.
	 */
	public DataSource<Long> generateSequence(long from, long to) {
		return fromParallelCollection(new NumberSequenceIterator(from, to), BasicTypeInfo.LONG_TYPE_INFO, Utils.getCallLocationName());
	}

	// --------------------------------------------------------------------------------------------
	//  Executing
	// --------------------------------------------------------------------------------------------

	/**
	 * Triggers the program execution. The environment will execute all parts of the program that have
	 * resulted in a "sink" operation. Sink operations are for example printing results ({@link DataSet#print()},
	 * writing results (e.g. {@link DataSet#writeAsText(String)},
	 * {@link DataSet#write(org.apache.flink.api.common.io.FileOutputFormat, String)}, or other generic
	 * data sinks created with {@link DataSet#output(org.apache.flink.api.common.io.OutputFormat)}.
	 *
	 * <p>The program execution will be logged and displayed with a generated default name.
	 *
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception Thrown, if the program executions fails.
	 */
	public JobExecutionResult execute() throws Exception {
		return execute(getDefaultName());
	}

	/**
	 * Triggers the program execution. The environment will execute all parts of the program that have
	 * resulted in a "sink" operation. Sink operations are for example printing results ({@link DataSet#print()},
	 * writing results (e.g. {@link DataSet#writeAsText(String)},
	 * {@link DataSet#write(org.apache.flink.api.common.io.FileOutputFormat, String)}, or other generic
	 * data sinks created with {@link DataSet#output(org.apache.flink.api.common.io.OutputFormat)}.
	 *
	 * <p>The program execution will be logged and displayed with the given job name.
	 *
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception Thrown, if the program executions fails.
	 */
	public abstract JobExecutionResult execute(String jobName) throws Exception;

	/**
	 * Creates the plan with which the system will execute the program, and returns it as
	 * a String using a JSON representation of the execution data flow graph.
	 * Note that this needs to be called, before the plan is executed.
	 *
	 * @return The execution plan of the program, as a JSON String.
	 * @throws Exception Thrown, if the compiler could not be instantiated, or the master could not
	 *                   be contacted to retrieve information relevant to the execution planning.
	 */
	public abstract String getExecutionPlan() throws Exception;

	/**
	 * Registers a file at the distributed cache under the given name. The file will be accessible
	 * from any user-defined function in the (distributed) runtime under a local path. Files
	 * may be local files (which will be distributed via BlobServer), or files in a distributed file system.
	 * The runtime will copy the files temporarily to a local cache, if needed.
	 *
	 * <p>The {@link org.apache.flink.api.common.functions.RuntimeContext} can be obtained inside UDFs via
	 * {@link org.apache.flink.api.common.functions.RichFunction#getRuntimeContext()} and provides access
	 * {@link org.apache.flink.api.common.cache.DistributedCache} via
	 * {@link org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache()}.
	 *
	 * @param filePath The path of the file, as a URI (e.g. "file:///some/path" or "hdfs://host:port/and/path")
	 * @param name The name under which the file is registered.
	 */
	public void registerCachedFile(String filePath, String name){
		registerCachedFile(filePath, name, false);
	}

	/**
	 * Registers a file at the distributed cache under the given name. The file will be accessible
	 * from any user-defined function in the (distributed) runtime under a local path. Files
	 * may be local files (which will be distributed via BlobServer), or files in a distributed file system.
	 * The runtime will copy the files temporarily to a local cache, if needed.
	 *
	 * <p>The {@link org.apache.flink.api.common.functions.RuntimeContext} can be obtained inside UDFs via
	 * {@link org.apache.flink.api.common.functions.RichFunction#getRuntimeContext()} and provides access
	 * {@link org.apache.flink.api.common.cache.DistributedCache} via
	 * {@link org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache()}.
	 *
	 * @param filePath The path of the file, as a URI (e.g. "file:///some/path" or "hdfs://host:port/and/path")
	 * @param name The name under which the file is registered.
	 * @param executable flag indicating whether the file should be executable
	 */
	public void registerCachedFile(String filePath, String name, boolean executable){
		this.cacheFile.add(new Tuple2<>(name, new DistributedCacheEntry(filePath, executable)));
	}

	/**
	 * Registers all files that were registered at this execution environment's cache registry of the
	 * given plan's cache registry.
	 *
	 * @param p The plan to register files at.
	 * @throws IOException Thrown if checks for existence and sanity fail.
	 */
	protected void registerCachedFilesWithPlan(Plan p) throws IOException {
		for (Tuple2<String, DistributedCacheEntry> entry : cacheFile) {
			p.registerCachedFile(entry.f0, entry.f1);
		}
	}

	/**
	 * Creates the program's {@link Plan}. The plan is a description of all data sources, data sinks,
	 * and operations and how they interact, as an isolated unit that can be executed with a
	 * {@link org.apache.flink.api.common.PlanExecutor}. Obtaining a plan and starting it with an
	 * executor is an alternative way to run a program and is only possible if the program consists
	 * only of distributed operations.
	 * This automatically starts a new stage of execution.
	 *
	 * @return The program's plan.
	 */
	@Internal
	public Plan createProgramPlan() {
		return createProgramPlan(null);
	}

	/**
	 * Creates the program's {@link Plan}. The plan is a description of all data sources, data sinks,
	 * and operations and how they interact, as an isolated unit that can be executed with a
	 * {@link org.apache.flink.api.common.PlanExecutor}. Obtaining a plan and starting it with an
	 * executor is an alternative way to run a program and is only possible if the program consists
	 * only of distributed operations.
	 * This automatically starts a new stage of execution.
	 *
	 * @param jobName The name attached to the plan (displayed in logs and monitoring).
	 * @return The program's plan.
	 */
	@Internal
	public Plan createProgramPlan(String jobName) {
		return createProgramPlan(jobName, true);
	}

	/**
	 * Creates the program's {@link Plan}. The plan is a description of all data sources, data sinks,
	 * and operations and how they interact, as an isolated unit that can be executed with a
	 * {@link org.apache.flink.api.common.PlanExecutor}. Obtaining a plan and starting it with an
	 * executor is an alternative way to run a program and is only possible if the program consists
	 * only of distributed operations.
	 *
	 * @param jobName The name attached to the plan (displayed in logs and monitoring).
	 * @param clearSinks Whether or not to start a new stage of execution.
	 * @return The program's plan.
	 */
	@Internal
	public Plan createProgramPlan(String jobName, boolean clearSinks) {
		if (this.sinks.isEmpty()) {
			if (wasExecuted) {
				throw new RuntimeException("No new data sinks have been defined since the " +
						"last execution. The last execution refers to the latest call to " +
						"'execute()', 'count()', 'collect()', or 'print()'.");
			} else {
				throw new RuntimeException("No data sinks have been created yet. " +
						"A program needs at least one sink that consumes data. " +
						"Examples are writing the data set or printing it.");
			}
		}

		if (jobName == null) {
			jobName = getDefaultName();
		}

		OperatorTranslation translator = new OperatorTranslation();
		Plan plan = translator.translateToPlan(this.sinks, jobName);

		if (getParallelism() > 0) {
			plan.setDefaultParallelism(getParallelism());
		}
		plan.setExecutionConfig(getConfig());

		// Check plan for GenericTypeInfo's and register the types at the serializers.
		if (!config.isAutoTypeRegistrationDisabled()) {
			plan.accept(new Visitor<org.apache.flink.api.common.operators.Operator<?>>() {

				private final Set<Class<?>> registeredTypes = new HashSet<>();
				private final Set<org.apache.flink.api.common.operators.Operator<?>> visitedOperators = new HashSet<>();

				@Override
				public boolean preVisit(org.apache.flink.api.common.operators.Operator<?> visitable) {
					if (!visitedOperators.add(visitable)) {
						return false;
					}
					OperatorInformation<?> opInfo = visitable.getOperatorInfo();
					Serializers.recursivelyRegisterType(opInfo.getOutputType(), config, registeredTypes);
					return true;
				}

				@Override
				public void postVisit(org.apache.flink.api.common.operators.Operator<?> visitable) {}
			});
		}

		try {
			registerCachedFilesWithPlan(plan);
		} catch (Exception e) {
			throw new RuntimeException("Error while registering cached files: " + e.getMessage(), e);
		}

		// clear all the sinks such that the next execution does not redo everything
		if (clearSinks) {
			this.sinks.clear();
			wasExecuted = true;
		}

		// All types are registered now. Print information.
		int registeredTypes = config.getRegisteredKryoTypes().size() +
				config.getRegisteredPojoTypes().size() +
				config.getRegisteredTypesWithKryoSerializerClasses().size() +
				config.getRegisteredTypesWithKryoSerializers().size();
		int defaultKryoSerializers = config.getDefaultKryoSerializers().size() +
				config.getDefaultKryoSerializerClasses().size();
		LOG.info("The job has {} registered types and {} default Kryo serializers", registeredTypes, defaultKryoSerializers);

		if (config.isForceKryoEnabled() && config.isForceAvroEnabled()) {
			LOG.warn("In the ExecutionConfig, both Avro and Kryo are enforced. Using Kryo serializer");
		}
		if (config.isForceKryoEnabled()) {
			LOG.info("Using KryoSerializer for serializing POJOs");
		}
		if (config.isForceAvroEnabled()) {
			LOG.info("Using AvroSerializer for serializing POJOs");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Registered Kryo types: {}", config.getRegisteredKryoTypes().toString());
			LOG.debug("Registered Kryo with Serializers types: {}", config.getRegisteredTypesWithKryoSerializers().entrySet().toString());
			LOG.debug("Registered Kryo with Serializer Classes types: {}", config.getRegisteredTypesWithKryoSerializerClasses().entrySet().toString());
			LOG.debug("Registered Kryo default Serializers: {}", config.getDefaultKryoSerializers().entrySet().toString());
			LOG.debug("Registered Kryo default Serializers Classes {}", config.getDefaultKryoSerializerClasses().entrySet().toString());
			LOG.debug("Registered POJO types: {}", config.getRegisteredPojoTypes().toString());

			// print information about static code analysis
			LOG.debug("Static code analysis mode: {}", config.getCodeAnalysisMode());
		}

		return plan;
	}

	/**
	 * Adds the given sink to this environment. Only sinks that have been added will be executed once
	 * the {@link #execute()} or {@link #execute(String)} method is called.
	 *
	 * @param sink The sink to add for execution.
	 */
	@Internal
	void registerDataSink(DataSink<?> sink) {
		this.sinks.add(sink);
	}

	/**
	 * Gets a default job name, based on the timestamp when this method is invoked.
	 *
	 * @return A default job name.
	 */
	private static String getDefaultName() {
		return "Flink Java Job at " + Calendar.getInstance().getTime();
	}

	// --------------------------------------------------------------------------------------------
	//  Instantiation of Execution Contexts
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates an execution environment that represents the context in which the program is currently executed.
	 * If the program is invoked standalone, this method returns a local execution environment, as returned by
	 * {@link #createLocalEnvironment()}. If the program is invoked from within the command line client to be
	 * submitted to a cluster, this method returns the execution environment of this cluster.
	 *
	 * @return The execution environment of the context in which the program is executed.
	 */
	public static ExecutionEnvironment getExecutionEnvironment() {
		return Utils.resolveFactory(threadLocalContextEnvironmentFactory, contextEnvironmentFactory)
			.map(ExecutionEnvironmentFactory::createExecutionEnvironment)
			.orElseGet(ExecutionEnvironment::createLocalEnvironment);
	}

	/**
	 * Creates a {@link CollectionEnvironment} that uses Java Collections underneath. This will execute in a
	 * single thread in the current JVM. It is very fast but will fail if the data does not fit into
	 * memory. parallelism will always be 1. This is useful during implementation and for debugging.
	 * @return A Collection Environment
	 */
	@PublicEvolving
	public static CollectionEnvironment createCollectionsEnvironment(){
		CollectionEnvironment ce = new CollectionEnvironment();
		ce.setParallelism(1);
		return ce;
	}

	/**
	 * Creates a {@link LocalEnvironment}. The local execution environment will run the program in a
	 * multi-threaded fashion in the same JVM as the environment was created in. The default
	 * parallelism of the local environment is the number of hardware contexts (CPU cores / threads),
	 * unless it was specified differently by {@link #setDefaultLocalParallelism(int)}.
	 *
	 * @return A local execution environment.
	 */
	public static LocalEnvironment createLocalEnvironment() {
		return createLocalEnvironment(defaultLocalDop);
	}

	/**
	 * Creates a {@link LocalEnvironment}. The local execution environment will run the program in a
	 * multi-threaded fashion in the same JVM as the environment was created in. It will use the
	 * parallelism specified in the parameter.
	 *
	 * @param parallelism The parallelism for the local environment.
	 * @return A local execution environment with the specified parallelism.
	 */
	public static LocalEnvironment createLocalEnvironment(int parallelism) {
		return createLocalEnvironment(new Configuration(), parallelism);
	}

	/**
	 * Creates a {@link LocalEnvironment}. The local execution environment will run the program in a
	 * multi-threaded fashion in the same JVM as the environment was created in. It will use the
	 * parallelism specified in the parameter.
	 *
	 * @param customConfiguration Pass a custom configuration to the LocalEnvironment.
	 * @return A local execution environment with the specified parallelism.
	 */
	public static LocalEnvironment createLocalEnvironment(Configuration customConfiguration) {
		return createLocalEnvironment(customConfiguration, -1);
	}

	/**
	 * Creates a {@link LocalEnvironment} for local program execution that also starts the
	 * web monitoring UI.
	 *
	 * <p>The local execution environment will run the program in a multi-threaded fashion in
	 * the same JVM as the environment was created in. It will use the parallelism specified in the
	 * parameter.
	 *
	 * <p>If the configuration key 'rest.port' was set in the configuration, that particular
	 * port will be used for the web UI. Otherwise, the default port (8081) will be used.
	 */
	@PublicEvolving
	public static ExecutionEnvironment createLocalEnvironmentWithWebUI(Configuration conf) {
		checkNotNull(conf, "conf");

		conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

		if (!conf.contains(RestOptions.PORT)) {
			// explicitly set this option so that it's not set to 0 later
			conf.setInteger(RestOptions.PORT, RestOptions.PORT.defaultValue());
		}

		return createLocalEnvironment(conf, -1);
	}

	/**
	 * Creates a {@link LocalEnvironment} which is used for executing Flink jobs.
	 *
	 * @param configuration to start the {@link LocalEnvironment} with
	 * @param defaultParallelism to initialize the {@link LocalEnvironment} with
	 * @return {@link LocalEnvironment}
	 */
	private static LocalEnvironment createLocalEnvironment(Configuration configuration, int defaultParallelism) {
		final LocalEnvironment localEnvironment = new LocalEnvironment(configuration);

		if (defaultParallelism > 0) {
			localEnvironment.setParallelism(defaultParallelism);
		}

		return localEnvironment;
	}

	/**
	 * Creates a {@link RemoteEnvironment}. The remote environment sends (parts of) the program
	 * to a cluster for execution. Note that all file paths used in the program must be accessible from the
	 * cluster. The execution will use the cluster's default parallelism, unless the parallelism is
	 * set explicitly via {@link ExecutionEnvironment#setParallelism(int)}.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static ExecutionEnvironment createRemoteEnvironment(String host, int port, String... jarFiles) {
		return new RemoteEnvironment(host, port, jarFiles);
	}

	/**
	 * Creates a {@link RemoteEnvironment}. The remote environment sends (parts of) the program
	 * to a cluster for execution. Note that all file paths used in the program must be accessible from the
	 * cluster. The custom configuration file is used to configure Akka specific configuration parameters
	 * for the Client only; Program parallelism can be set via {@link ExecutionEnvironment#setParallelism(int)}.
	 *
	 * <p>Cluster configuration has to be done in the remotely running Flink instance.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param clientConfiguration Configuration used by the client that connects to the cluster.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static ExecutionEnvironment createRemoteEnvironment(
			String host, int port, Configuration clientConfiguration, String... jarFiles) {
		return new RemoteEnvironment(host, port, clientConfiguration, jarFiles, null);
	}

	/**
	 * Creates a {@link RemoteEnvironment}. The remote environment sends (parts of) the program
	 * to a cluster for execution. Note that all file paths used in the program must be accessible from the
	 * cluster. The execution will use the specified parallelism.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param parallelism The parallelism to use during the execution.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static ExecutionEnvironment createRemoteEnvironment(String host, int port, int parallelism, String... jarFiles) {
		RemoteEnvironment rec = new RemoteEnvironment(host, port, jarFiles);
		rec.setParallelism(parallelism);
		return rec;
	}

	// --------------------------------------------------------------------------------------------
	//  Default parallelism for local execution
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the default parallelism that will be used for the local execution environment created by
	 * {@link #createLocalEnvironment()}.
	 *
	 * @return The default local parallelism
	 */
	public static int getDefaultLocalParallelism() {
		return defaultLocalDop;
	}

	/**
	 * Sets the default parallelism that will be used for the local execution environment created by
	 * {@link #createLocalEnvironment()}.
	 *
	 * @param parallelism The parallelism to use as the default local parallelism.
	 */
	public static void setDefaultLocalParallelism(int parallelism) {
		defaultLocalDop = parallelism;
	}

	// --------------------------------------------------------------------------------------------
	//  Methods to control the context environment and creation of explicit environments other
	//  than the context environment
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets a context environment factory, that creates the context environment for running programs
	 * with pre-configured environments. Examples are running programs from the command line, and
	 * running programs in the Scala shell.
	 *
	 * <p>When the context environment factory is set, no other environments can be explicitly used.
	 *
	 * @param ctx The context environment factory.
	 */
	protected static void initializeContextEnvironment(ExecutionEnvironmentFactory ctx) {
		contextEnvironmentFactory = Preconditions.checkNotNull(ctx);
		threadLocalContextEnvironmentFactory.set(contextEnvironmentFactory);
	}

	/**
	 * Un-sets the context environment factory. After this method is called, the call to
	 * {@link #getExecutionEnvironment()} will again return a default local execution environment, and
	 * it is possible to explicitly instantiate the LocalEnvironment and the RemoteEnvironment.
	 */
	protected static void resetContextEnvironment() {
		contextEnvironmentFactory = null;
		threadLocalContextEnvironmentFactory.remove();
	}

	/**
	 * Checks whether it is currently permitted to explicitly instantiate a LocalEnvironment
	 * or a RemoteEnvironment.
	 *
	 * @return True, if it is possible to explicitly instantiate a LocalEnvironment or a
	 *         RemoteEnvironment, false otherwise.
	 */
	@Internal
	public static boolean areExplicitEnvironmentsAllowed() {
		return contextEnvironmentFactory == null && threadLocalContextEnvironmentFactory.get() == null;
	}
}
