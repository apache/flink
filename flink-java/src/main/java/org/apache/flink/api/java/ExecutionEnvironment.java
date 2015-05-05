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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
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
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.SplittableIterator;
import org.apache.flink.util.Visitor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Serializer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * The ExecutionEnviroment is the context in which a program is executed. A
 * {@link LocalEnvironment} will cause execution in the current JVM, a
 * {@link RemoteEnvironment} will cause execution on a remote setup.
 * <p>
 * The environment provides methods to control the job execution (such as setting the parallelism)
 * and to interact with the outside world (data access).
 * <p>
 * Please note that the execution environment needs strong type information for the input and return types
 * of all operations that are executed. This means that the environments needs to know that the return 
 * value of an operation is for example a Tuple of String and Integer.
 * Because the Java compiler throws much of the generic type information away, most methods attempt to re-
 * obtain that information using reflection. In certain cases, it may be necessary to manually supply that
 * information to some of the methods.
 * 
 * @see LocalEnvironment
 * @see RemoteEnvironment
 */
public abstract class ExecutionEnvironment {


	protected JobExecutionResult lastJobExecutionResult;

	private static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);
	
	/** The environment of the context (local by default, cluster if invoked through command line) */
	private static ExecutionEnvironmentFactory contextEnvironmentFactory;
	
	/** The default parallelism used by local environments */
	private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();
	
	/** flag to disable local executor when using the ContextEnvironment */
	private static boolean allowLocalExecution = true;
	
	// --------------------------------------------------------------------------------------------
	
	private final UUID executionId;
	
	private final List<DataSink<?>> sinks = new ArrayList<DataSink<?>>();
	
	private final List<Tuple2<String, DistributedCacheEntry>> cacheFile = new ArrayList<Tuple2<String, DistributedCacheEntry>>();

	private ExecutionConfig config = new ExecutionConfig();

	// --------------------------------------------------------------------------------------------
	//  Constructor and Properties
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new Execution Environment.
	 */
	protected ExecutionEnvironment() {
		this.executionId = UUID.randomUUID();
	}

	/**
	 * Gets the config object.
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
	 *         returns {@code -1}, if the environments default parallelism should be used.
	 * @deprecated Please use {@link #getParallelism}
	 */
	@Deprecated
	public int getDegreeOfParallelism() {
		return getParallelism();
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
	 *         returns {@code -1}, if the environments default parallelism should be used.
	 */
	public int getParallelism() {
		return config.getParallelism();
	}
	
	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run with
	 * x parallel instances.
	 * <p>
	 * This method overrides the default parallelism for this environment.
	 * The {@link LocalEnvironment} uses by default a value equal to the number of hardware
	 * contexts (CPU cores / threads). When executing the program via the command line client 
	 * from a JAR file, the default parallelism is the one configured for that setup.
	 * 
	 * @param parallelism The parallelism
	 * @deprecated Please use {@link #setParallelism}
	 */
	@Deprecated
	public void setDegreeOfParallelism(int parallelism) {
		setParallelism(parallelism);
	}

	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run with
	 * x parallel instances.
	 * <p>
	 * This method overrides the default parallelism for this environment.
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
	 * Sets the number of times that failed tasks are re-executed. A value of zero
	 * effectively disables fault tolerance. A value of {@code -1} indicates that the system
	 * default value (as defined in the configuration) should be used.
	 * 
	 * @param numberOfExecutionRetries The number of times the system will try to re-execute failed tasks.
	 */
	public void setNumberOfExecutionRetries(int numberOfExecutionRetries) {
		config.setNumberOfExecutionRetries(numberOfExecutionRetries);
	}
	
	/**
	 * Gets the number of times the system will try to re-execute failed tasks. A value
	 * of {@code -1} indicates that the system default value (as defined in the configuration)
	 * should be used.
	 * 
	 * @return The number of times the system will try to re-execute failed tasks.
	 */
	public int getNumberOfExecutionRetries() {
		return config.getNumberOfExecutionRetries();
	}
	
	/**
	 * Gets the UUID by which this environment is identified. The UUID sets the execution context
	 * in the cluster or local environment.
	 *
	 * @return The UUID of this environment.
	 * @see #getIdString()
	 */
	public UUID getId() {
		return this.executionId;
	}

	/**
	 * Returns the {@link org.apache.flink.api.common.JobExecutionResult} of the last executed job.
	 */
	public JobExecutionResult getLastJobExecutionResult(){
		return this.lastJobExecutionResult;
	}


	/**
	 * Gets the UUID by which this environment is identified, as a string.
	 * 
	 * @return The UUID as a string.
	 * @see #getId()
	 */
	public String getIdString() {
		return this.executionId.toString();
	}

	// --------------------------------------------------------------------------------------------
	//  Registry for types and serializers
	// --------------------------------------------------------------------------------------------


	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public void addDefaultKryoSerializer(Class<?> type, Serializer<?> serializer) {
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
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public void registerTypeWithKryoSerializer(Class<?> type, Serializer<?> serializer) {
		config.registerTypeWithKryoSerializer(type, serializer);
	}

	/**
	 * Registers the given Serializer via its class as a serializer for the given type at the KryoSerializer
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
	 * Creates a DataSet that represents the Strings produced by reading the given file line wise.
	 * The file will be read with the system's default character set.
	 * 
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return A DataSet that represents the data read from the given file as text lines.
	 */
	public DataSource<String> readTextFile(String filePath) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		
		return new DataSource<String>(this, new TextInputFormat(new Path(filePath)), BasicTypeInfo.STRING_TYPE_INFO, Utils.getCallLocationName());
	}
	
	/**
	 * Creates a DataSet that represents the Strings produced by reading the given file line wise.
	 * The {@link java.nio.charset.Charset} with the given name will be used to read the files.
	 * 
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @param charsetName The name of the character set used to read the file.
	 * @return A DataSet that represents the data read from the given file as text lines.
	 */
	public DataSource<String> readTextFile(String filePath, String charsetName) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		TextInputFormat format = new TextInputFormat(new Path(filePath));
		format.setCharsetName(charsetName);
		return new DataSource<String>(this, format, BasicTypeInfo.STRING_TYPE_INFO, Utils.getCallLocationName());
	}
	
	// -------------------------- Text Input Format With String Value------------------------------
	
	/**
	 * Creates a DataSet that represents the Strings produced by reading the given file line wise.
	 * This method is similar to {@link #readTextFile(String)}, but it produces a DataSet with mutable
	 * {@link StringValue} objects, rather than Java Strings. StringValues can be used to tune implementations
	 * to be less object and garbage collection heavy.
	 * <p>
	 * The file will be read with the system's default character set.
	 * 
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return A DataSet that represents the data read from the given file as text lines.
	 */
	public DataSource<StringValue> readTextFileWithValue(String filePath) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");
		
		return new DataSource<StringValue>(this, new TextValueInputFormat(new Path(filePath)), new ValueTypeInfo<StringValue>(StringValue.class), Utils.getCallLocationName());
	}
	
	/**
	 * Creates a DataSet that represents the Strings produced by reading the given file line wise.
	 * This method is similar to {@link #readTextFile(String, String)}, but it produces a DataSet with mutable
	 * {@link StringValue} objects, rather than Java Strings. StringValues can be used to tune implementations
	 * to be less object and garbage collection heavy.
	 * <p>
	 * The {@link java.nio.charset.Charset} with the given name will be used to read the files.
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
		return new DataSource<StringValue>(this, format, new ValueTypeInfo<StringValue>(StringValue.class), Utils.getCallLocationName());
	}

	// ----------------------------------- Primitive Input Format ---------------------------------------

	/**
	 * Creates a DataSet that represents the primitive type produced by reading the given file line wise.
	 * This method is similar to {@link #readCsvFile(String)} with single field, but it produces a DataSet not through
	 * {@link org.apache.flink.api.java.tuple.Tuple1}.
	 *
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @param typeClass The primitive type class to be read.
	 * @return A DataSet that represents the data read from the given file as primitive type.
	 */
	public <X> DataSource<X> readFileOfPrimitives(String filePath, Class<X> typeClass) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		return new DataSource<X>(this, new PrimitiveInputFormat<X>(new Path(filePath), typeClass), TypeExtractor.getForClass(typeClass), Utils.getCallLocationName());
	}

	/**
	 * Creates a DataSet that represents the primitive type produced by reading the given file in delimited way.
	 * This method is similar to {@link #readCsvFile(String)} with single field, but it produces a DataSet not through
	 * {@link org.apache.flink.api.java.tuple.Tuple1}.
	 *
	 * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @param delimiter The delimiter of the given file.
	 * @param typeClass The primitive type class to be read.
	 * @return A DataSet that represents the data read from the given file as primitive type.
	 */
	public <X> DataSource<X> readFileOfPrimitives(String filePath, String delimiter, Class<X> typeClass) {
		Preconditions.checkNotNull(filePath, "The file path may not be null.");

		return new DataSource<X>(this, new PrimitiveInputFormat<X>(new Path(filePath), delimiter, typeClass), TypeExtractor.getForClass(typeClass), Utils.getCallLocationName());
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
	 * Generic method to create an input DataSet with in {@link InputFormat}. The DataSet will not be
	 * immediately created - instead, this method returns a DataSet that will be lazily created from
	 * the input format once the program is executed.
	 * <p>
	 * Since all data sets need specific information about their types, this method needs to determine
	 * the type of the data produced by the input format. It will attempt to determine the data type
	 * by reflection, unless the input format implements the {@link ResultTypeQueryable} interface.
	 * In the latter case, this method will invoke the {@link ResultTypeQueryable#getProducedType()}
	 * method to determine data type produced by the input format.
	 * 
	 * @param inputFormat The input format used to create the data set.
	 * @return A DataSet that represents the data created by the input format.
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
					"'createInput(InputFormat, TypeInformation)' method instead.");
		}
	}

	/**
	 * Generic method to create an input DataSet with in {@link InputFormat}. The DataSet will not be
	 * immediately created - instead, this method returns a DataSet that will be lazily created from
	 * the input format once the program is executed.
	 * <p>
	 * The data set is typed to the given TypeInformation. This method is intended for input formats that
	 * where the return type cannot be determined by reflection analysis, and that do not implement the
	 * {@link ResultTypeQueryable} interface.
	 * 
	 * @param inputFormat The input format used to create the data set.
	 * @return A DataSet that represents the data created by the input format.
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
		
		return new DataSource<X>(this, inputFormat, producedType, Utils.getCallLocationName());
	}

	// ----------------------------------- Hadoop Input Format ---------------------------------------

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapred.FileInputFormat}. The
	 * given inputName is set on the given job.
	 */
	public <K,V> DataSource<Tuple2<K, V>> readHadoopFile(org.apache.hadoop.mapred.FileInputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, String inputPath, JobConf job) {
		DataSource<Tuple2<K, V>> result = createHadoopInput(mapredInputFormat, key, value, job);

		org.apache.hadoop.mapred.FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(inputPath));

		return result;
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapred.FileInputFormat}. A
	 * {@link org.apache.hadoop.mapred.JobConf} with the given inputPath is created.
	 */
	public <K,V> DataSource<Tuple2<K, V>> readHadoopFile(org.apache.hadoop.mapred.FileInputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, String inputPath) {
		return readHadoopFile(mapredInputFormat, key, value, inputPath, new JobConf());
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapred.InputFormat}.
	 */
	public <K,V> DataSource<Tuple2<K, V>> createHadoopInput(org.apache.hadoop.mapred.InputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, JobConf job) {
		HadoopInputFormat<K, V> hadoopInputFormat = new HadoopInputFormat<K, V>(mapredInputFormat, key, value, job);

		return this.createInput(hadoopInputFormat);
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}. The
	 * given inputName is set on the given job.
	 */
	public <K,V> DataSource<Tuple2<K, V>> readHadoopFile(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, String inputPath, Job job) throws IOException {
		DataSource<Tuple2<K, V>> result = createHadoopInput(mapredInputFormat, key, value, job);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new org.apache
				.hadoop.fs.Path(inputPath));

		return result;
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}. A
	 * {@link org.apache.hadoop.mapreduce.Job} with the given inputPath is created.
	 */
	public <K,V> DataSource<Tuple2<K, V>> readHadoopFile(org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, String inputPath) throws IOException {
		return readHadoopFile(mapredInputFormat, key, value, inputPath, Job.getInstance());
	}

	/**
	 * Creates a {@link DataSet} from the given {@link org.apache.hadoop.mapreduce.InputFormat}.
	 */
	public <K,V> DataSource<Tuple2<K, V>> createHadoopInput(org.apache.hadoop.mapreduce.InputFormat<K,V> mapredInputFormat, Class<K> key, Class<V> value, Job job) {
		org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> hadoopInputFormat = new org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V>(mapredInputFormat, key, value, job);

		return this.createInput(hadoopInputFormat);
	}
	
	// ----------------------------------- Collection ---------------------------------------
	
	/**
	 * Creates a DataSet from the given non-empty collection. The type of the data set is that
	 * of the elements in the collection. The elements need to be serializable (as defined by
	 * {@link java.io.Serializable}), because the framework may move the elements into the cluster
	 * if needed.
	 * <p>
	 * The framework will try and determine the exact type from the collection elements.
	 * In case of generic elements, it may be necessary to manually supply the type information
	 * via {@link #fromCollection(Collection, TypeInformation)}.
	 * <p>
	 * Note that this operation will result in a non-parallel data source, i.e. a data source with
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
		return new DataSource<X>(this, new CollectionInputFormat<X>(data, type.createSerializer(config)), type, Utils.getCallLocationName());
	}
	
	/**
	 * Creates a DataSet from the given non-empty collection. The type of the data set is that
	 * of the elements in the collection. The elements need to be serializable (as defined by
	 * {@link java.io.Serializable}), because the framework may move the elements into the cluster
	 * if needed.
	 * <p>
	 * Note that this operation will result in a non-parallel data source, i.e. a data source with
	 * a parallelism of one.
	 * <p>
	 * The returned DataSet is typed to the given TypeInformation.
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
		return new DataSource<X>(this, new CollectionInputFormat<X>(data, type.createSerializer(config)), type, callLocationName);
	}
	
	/**
	 * Creates a DataSet from the given iterator. Because the iterator will remain unmodified until
	 * the actual execution happens, the type of data returned by the iterator must be given
	 * explicitly in the form of the type class (this is due to the fact that the Java compiler
	 * erases the generic type information).
	 * <p>
	 * The iterator must be serializable (as defined in {@link java.io.Serializable}), because the
	 * framework may move it to a remote environment, if needed.
	 * <p>
	 * Note that this operation will result in a non-parallel data source, i.e. a data source with
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
	 * <p>
	 * The iterator must be serializable (as defined in {@link java.io.Serializable}), because the
	 * framework may move it to a remote environment, if needed.
	 * <p>
	 * Note that this operation will result in a non-parallel data source, i.e. a data source with
	 * a parallelism of one.
	 * 
	 * @param data The collection of elements to create the data set from.
	 * @param type The TypeInformation for the produced data set.
	 * @return A DataSet representing the elements in the iterator.
	 * 
	 * @see #fromCollection(Iterator, Class)
	 */
	public <X> DataSource<X> fromCollection(Iterator<X> data, TypeInformation<X> type) {
		if (!(data instanceof Serializable)) {
			throw new IllegalArgumentException("The iterator must be serializable.");
		}
		
		return new DataSource<X>(this, new IteratorInputFormat<X>(data), type, Utils.getCallLocationName());
	}
	
	
	/**
	 * Creates a new data set that contains the given elements. The elements must all be of the same type,
	 * for example, all of the {@link String} or {@link Integer}. The sequence of elements must not be empty.
	 * Furthermore, the elements must be serializable (as defined in {@link java.io.Serializable}, because the
	 * execution environment may ship the elements into the cluster.
	 * <p>
	 * The framework will try and determine the exact type from the collection elements.
	 * In case of generic elements, it may be necessary to manually supply the type information
	 * via {@link #fromCollection(Collection, TypeInformation)}.
	 * <p>
	 * Note that this operation will result in a non-parallel data source, i.e. a data source with
	 * a parallelism of one.
	 * 
	 * @param data The elements to make up the data set.
	 * @return A DataSet representing the given list of elements.
	 */
	public <X> DataSource<X> fromElements(X... data) {
		if (data == null) {
			throw new IllegalArgumentException("The data must not be null.");
		}
		if (data.length == 0) {
			throw new IllegalArgumentException("The number of elements must not be zero.");
		}
		
		return fromCollection(Arrays.asList(data), TypeExtractor.getForObject(data[0]), Utils.getCallLocationName());
	}
	
	
	/**
	 * Creates a new data set that contains elements in the iterator. The iterator is splittable, allowing the
	 * framework to create a parallel data source that returns the elements in the iterator.
	 * The iterator must be serializable (as defined in {@link java.io.Serializable}, because the
	 * execution environment may ship the elements into the cluster.
	 * <p>
	 * Because the iterator will remain unmodified until the actual execution happens, the type of data
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
	 * The iterator must be serializable (as defined in {@link java.io.Serializable}, because the
	 * execution environment may ship the elements into the cluster.
	 * <p>
	 * Because the iterator will remain unmodified until the actual execution happens, the type of data
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
		return new DataSource<X>(this, new ParallelIteratorInputFormat<X>(iterator), type, callLocationName);
	}
	
	/**
	 * Creates a new data set that contains a sequence of numbers. The data set will be created in parallel,
	 * so there is no guarantee about the oder of the elements.
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
	 * <p>
	 * The program execution will be logged and displayed with a generated default name.
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
	 * <p>
	 * The program execution will be logged and displayed with the given job name.
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
	 * may be local files (as long as all relevant workers have access to it), or files in a distributed file system.
	 * The runtime will copy the files temporarily to a local cache, if needed.
	 * <p>
	 * The {@link org.apache.flink.api.common.functions.RuntimeContext} can be obtained inside UDFs via
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
	 * may be local files (as long as all relevant workers have access to it), or files in a distributed file system. 
	 * The runtime will copy the files temporarily to a local cache, if needed.
	 * <p>
	 * The {@link org.apache.flink.api.common.functions.RuntimeContext} can be obtained inside UDFs via
	 * {@link org.apache.flink.api.common.functions.RichFunction#getRuntimeContext()} and provides access
	 * {@link org.apache.flink.api.common.cache.DistributedCache} via 
	 * {@link org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache()}.
	 * 
	 * @param filePath The path of the file, as a URI (e.g. "file:///some/path" or "hdfs://host:port/and/path")
	 * @param name The name under which the file is registered.
	 * @param executable flag indicating whether the file should be executable
	 */
	public void registerCachedFile(String filePath, String name, boolean executable){
		this.cacheFile.add(new Tuple2<String, DistributedCacheEntry>(name, new DistributedCacheEntry(filePath, executable)));
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
	public JavaPlan createProgramPlan() {
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
	public JavaPlan createProgramPlan(String jobName) {
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
	public JavaPlan createProgramPlan(String jobName, boolean clearSinks) {
		if (this.sinks.isEmpty()) {
			throw new RuntimeException("No data sinks have been created yet. A program needs at least one sink that consumes data. Examples are writing the data set or printing it.");
		}
		
		if (jobName == null) {
			jobName = getDefaultName();
		}
		
		OperatorTranslation translator = new OperatorTranslation();
		JavaPlan plan = translator.translateToPlan(this.sinks, jobName);

		if (getParallelism() > 0) {
			plan.setDefaultParallelism(getParallelism());
		}
		plan.setExecutionConfig(getConfig());
		// Check plan for GenericTypeInfo's and register the types at the serializers.
		plan.accept(new Visitor<org.apache.flink.api.common.operators.Operator<?>>() {
			@Override
			public boolean preVisit(org.apache.flink.api.common.operators.Operator<?> visitable) {
				OperatorInformation<?> opInfo = visitable.getOperatorInfo();
				TypeInformation<?> typeInfo = opInfo.getOutputType();
				if(typeInfo instanceof GenericTypeInfo) {
					GenericTypeInfo<?> genericTypeInfo = (GenericTypeInfo<?>) typeInfo;
					if(!config.isAutoTypeRegistrationDisabled()) {
						Serializers.recursivelyRegisterType(genericTypeInfo.getTypeClass(), config);
					}
				}
				if(typeInfo instanceof CompositeType) {
					List<GenericTypeInfo<?>> genericTypesInComposite = new ArrayList<GenericTypeInfo<?>>();
					Utils.getContainedGenericTypes((CompositeType)typeInfo, genericTypesInComposite);
					for(GenericTypeInfo<?> gt : genericTypesInComposite) {
						Serializers.recursivelyRegisterType(gt.getTypeClass(), config);
					}
				}
				return true;
			}
			@Override
			public void postVisit(org.apache.flink.api.common.operators.Operator<?> visitable) {}
		});

		try {
			registerCachedFilesWithPlan(plan);
		} catch (Exception e) {
			throw new RuntimeException("Error while registering cached files: " + e.getMessage(), e);
		}
		
		// clear all the sinks such that the next execution does not redo everything
		if (clearSinks) {
			this.sinks.clear();
		}

		// All types are registered now. Print information.
		int registeredTypes = config.getRegisteredKryoTypes().size() +
				config.getRegisteredPojoTypes().size() +
				config.getRegisteredTypesWithKryoSerializerClasses().size() +
				config.getRegisteredTypesWithKryoSerializers().size();
		int defaultKryoSerializers = config.getDefaultKryoSerializers().size() +
				config.getDefaultKryoSerializerClasses().size();
		LOG.info("The job has {} registered types and {} default Kryo serializers", registeredTypes, defaultKryoSerializers);

		if(config.isForceKryoEnabled() && config.isForceAvroEnabled()) {
			LOG.warn("In the ExecutionConfig, both Avro and Kryo are enforced. Using Kryo serializer");
		}
		if(config.isForceKryoEnabled()) {
			LOG.info("Using KryoSerializer for serializing POJOs");
		}
		if(config.isForceAvroEnabled()) {
			LOG.info("Using AvroSerializer for serializing POJOs");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("Registered Kryo types: {}", Joiner.on(',').join(config.getRegisteredKryoTypes()));
			LOG.debug("Registered Kryo with Serializers types: {}", Joiner.on(',').join(config.getRegisteredTypesWithKryoSerializers()));
			LOG.debug("Registered Kryo with Serializer Classes types: {}", Joiner.on(',').join(config.getRegisteredTypesWithKryoSerializerClasses()));
			LOG.debug("Registered Kryo default Serializers: {}", Joiner.on(',').join(config.getDefaultKryoSerializers()));
			LOG.debug("Registered Kryo default Serializers Classes {}", Joiner.on(',').join(config.getDefaultKryoSerializerClasses()));
			LOG.debug("Registered POJO types: {}", Joiner.on(',').join(config.getRegisteredPojoTypes()));
		}

		return plan;
	}
	
	/**
	 * Adds the given sink to this environment. Only sinks that have been added will be executed once
	 * the {@link #execute()} or {@link #execute(String)} method is called.
	 * 
	 * @param sink The sink to add for execution.
	 */
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
		return contextEnvironmentFactory == null ? 
				createLocalEnvironment() : contextEnvironmentFactory.createExecutionEnvironment();
	}

	/**
	 * Creates a {@link CollectionEnvironment} that uses Java Collections underneath. This will execute in a
	 * single thread in the current JVM. It is very fast but will fail if the data does not fit into
	 * memory. parallelism will always be 1. This is useful during implementation and for debugging.
	 * @return A Collection Environment
	 */
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
		LocalEnvironment lee = new LocalEnvironment();
		lee.setParallelism(parallelism);
		return lee;
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
		LocalEnvironment lee = new LocalEnvironment();
		lee.setConfiguration(customConfiguration);
		return lee;
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
	//  Methods to control the context and local environments for execution from packaged programs
	// --------------------------------------------------------------------------------------------
	
	protected static void initializeContextEnvironment(ExecutionEnvironmentFactory ctx) {
		contextEnvironmentFactory = ctx;
	}
	
	protected static boolean isContextEnvironmentSet() {
		return contextEnvironmentFactory != null;
	}
	
	protected static void enableLocalExecution(boolean enabled) {
		allowLocalExecution = enabled;
	}
	
	public static boolean localExecutionIsAllowed() {
		return allowLocalExecution;
	}
}
