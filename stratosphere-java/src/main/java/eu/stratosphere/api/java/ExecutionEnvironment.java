/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.Validate;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.java.io.CollectionInputFormat;
import eu.stratosphere.api.java.io.CsvReader;
import eu.stratosphere.api.java.io.IteratorInputFormat;
import eu.stratosphere.api.java.io.ParallelIteratorInputFormat;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.io.TextValueInputFormat;
import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.operators.DataSource;
import eu.stratosphere.api.java.operators.Operator;
import eu.stratosphere.api.java.operators.OperatorTranslation;
import eu.stratosphere.api.java.operators.translation.JavaPlan;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.ValueTypeInfo;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.NumberSequenceIterator;
import eu.stratosphere.util.SplittableIterator;


/**
 * The ExecutionEnviroment is the context in which a program is executed. A
 * {@link LocalEnvironment} will cause execution in the current JVM, a
 * {@link RemoteEnvironment} will cause execution on a remote setup.
 * <p>
 * The environment provides methods to control the job execution (such as 
 * setting the parallelism) and to interact with the outside world (data access).
 */
public abstract class ExecutionEnvironment {
	
	/** The environment of the context (local by default, cluster if invoked through command line) */
	private static ExecutionEnvironment contextEnvironment;
	
	/** The default parallelism used by local environments */
	private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();
	
	/** flag to disable local executor when using the ContextEnvironment */
	private static boolean allowLocalExecution = true;
	
	// --------------------------------------------------------------------------------------------
	
	private final UUID executionId;
	
	private final List<DataSink<?>> sinks = new ArrayList<DataSink<?>>();
	
	private final List<Tuple2<String, String>> cacheFile = new ArrayList<Tuple2<String, String>>();

	private int degreeOfParallelism = -1;
	
	
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
	 * Gets the degree of parallelism with which operation are executed by default. Operations can
	 * individually override this value to use a specific degree of parallelism via
	 * {@link Operator#setParallelism(int)}. Other operations may need to run with a different
	 * degree of parallelism - for example calling
	 * {@link DataSet#reduce(eu.stratosphere.api.java.functions.ReduceFunction)} over the entire
	 * set will insert eventually an operation that runs non-parallel (degree of parallelism of one).
	 * 
	 * @return The degree of parallelism used by operations, unless they override that value. This method
	 *         returns {@code -1}, if the environments default parallelism should be used.
	 */
	public int getDegreeOfParallelism() {
		return degreeOfParallelism;
	}
	
	/**
	 * Sets the degree of parallelism (DOP) for operations executed through this environment.
	 * Setting a DOP of x here will cause all operators (such as join, map, reduce) to run with
	 * x parallel instances.
	 * <p>
	 * This method overrides the default parallelism for this environment.
	 * The {@link LocalEnvironment} uses by default a value equal to the number of hardware
	 * contexts (CPU cores / threads). When executing the program via the command line client 
	 * from a JAR file, the default degree of parallelism is the one configured for that setup.
	 * 
	 * @param degreeOfParallelism The degree of parallelism
	 */
	public void setDegreeOfParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1) {
			throw new IllegalArgumentException("Degree of parallelism must be at least one.");
		}
		
		this.degreeOfParallelism = degreeOfParallelism;
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
	 * Gets the UUID by which this environment is identified, as a string.
	 * 
	 * @return The UUID as a string.
	 * @see #getId()
	 */
	public String getIdString() {
		return this.executionId.toString();
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
		Validate.notNull(filePath, "The file path may not be null.");
		
		return new DataSource<String>(this, new TextInputFormat(new Path(filePath)), BasicTypeInfo.STRING_TYPE_INFO );
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
		Validate.notNull(filePath, "The file path may not be null.");

		TextInputFormat format = new TextInputFormat(new Path(filePath));
		format.setCharsetName(charsetName);
		return new DataSource<String>(this, format, BasicTypeInfo.STRING_TYPE_INFO );
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
		Validate.notNull(filePath, "The file path may not be null.");
		
		return new DataSource<StringValue>(this, new TextValueInputFormat(new Path(filePath)), new ValueTypeInfo<StringValue>(StringValue.class) );
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
		Validate.notNull(filePath, "The file path may not be null.");
		
		TextValueInputFormat format = new TextValueInputFormat(new Path(filePath));
		format.setCharsetName(charsetName);
		format.setSkipInvalidLines(skipInvalidLines);
		return new DataSource<StringValue>(this, format, new ValueTypeInfo<StringValue>(StringValue.class) );
	}
	
	// ----------------------------------- CSV Input Format ---------------------------------------
	
	public CsvReader readCsvFile(Path filePath) {
		return new CsvReader(filePath, this);
	}
	
	public CsvReader readCsvFile(String filePath) {
		return new CsvReader(filePath, this);
	}
	
	// ----------------------------------- Generic Input Format ---------------------------------------
	
	public <X> DataSource<X> createInput(InputFormat<X, ?> inputFormat) {
		if (inputFormat == null) {
			throw new IllegalArgumentException("InputFormat must not be null.");
		}
		
		try {
			@SuppressWarnings("unchecked")
			TypeInformation<X> producedType = (inputFormat instanceof ResultTypeQueryable) ?
					((ResultTypeQueryable<X>) inputFormat).getProducedType() :
					TypeExtractor.extractInputFormatTypes(inputFormat);
			
			return createInput(inputFormat, producedType);
		}
		catch (Exception e) {
			throw new InvalidProgramException("The type returned by the input format could not be automatically determined. " +
					"Please specify the TypeInformation of the produced type explicitly.");
		}
	}
	
	public <X> DataSource<X> createInput(InputFormat<X, ?> inputFormat, TypeInformation<X> producedType) {
		if (inputFormat == null) {
			throw new IllegalArgumentException("InputFormat must not be null.");
		}
		
		if (producedType == null) {
			throw new IllegalArgumentException("Produced type information must not be null.");
		}
		
		return new DataSource<X>(this, inputFormat, producedType);
	}
	
	// ----------------------------------- Collection ---------------------------------------
	
	public <X> DataSource<X> fromCollection(Collection<X> data) {
		if (data == null) {
			throw new IllegalArgumentException("The data must not be null.");
		}
		if (data.size() == 0) {
			throw new IllegalArgumentException("The size of the collection must not be empty.");
		}
		
		X firstValue = data.iterator().next();
		
		return fromCollection(data, TypeExtractor.getForObject(firstValue));
	}
	
	
	public <X> DataSource<X> fromCollection(Collection<X> data, TypeInformation<X> type) {
		CollectionInputFormat.checkCollection(data, type.getTypeClass());
		
		return new DataSource<X>(this, new CollectionInputFormat<X>(data), type);
	}
	
	public <X> DataSource<X> fromCollection(Iterator<X> data, Class<X> type) {
		return fromCollection(data, TypeExtractor.getForClass(type));
	}
	
	public <X> DataSource<X> fromCollection(Iterator<X> data, TypeInformation<X> type) {
		if (!(data instanceof Serializable)) {
			throw new IllegalArgumentException("The iterator must be serializable.");
		}
		
		return new DataSource<X>(this, new IteratorInputFormat<X>(data), type);
	}
	
	
	/**
	 * Creates a new data set that contains the given elements. The elements must all be of the same type,
	 * for example, all of the {@link String} or {@link Integer}. The sequence of elements must not be empty.
	 * Furthermore, the elements must be serializable (as defined in {@link java.io.Serializable}, because the
	 * execution environment may ship the elements into the cluster.
	 * 
	 * @param data The elements to make up the data set.
	 * @return A data set representing the given list of elements.
	 */
	public <X> DataSource<X> fromElements(X... data) {
		if (data == null) {
			throw new IllegalArgumentException("The data must not be null.");
		}
		if (data.length == 0) {
			throw new IllegalArgumentException("The number of elements must not be zero.");
		}
		
		return fromCollection(Arrays.asList(data), TypeExtractor.getForObject(data[0]));
	}
	
	
	public <X> DataSource<X> fromParallelCollection(SplittableIterator<X> iterator, Class<X> type) {
		return fromParallelCollection(iterator, TypeExtractor.getForClass(type));
	}
	
	
	public <X> DataSource<X> fromParallelCollection(SplittableIterator<X> iterator, TypeInformation<X> type) {
		return new DataSource<X>(this, new ParallelIteratorInputFormat<X>(iterator), type);
	}
	
	
	public DataSource<Long> generateSequence(long from, long to) {
		return fromParallelCollection(new NumberSequenceIterator(from, to), BasicTypeInfo.LONG_TYPE_INFO);
	}	
	
	// --------------------------------------------------------------------------------------------
	//  Executing
	// --------------------------------------------------------------------------------------------
	
	public JobExecutionResult execute() throws Exception {
		return execute(getDefaultName());
	}
	
	public abstract JobExecutionResult execute(String jobName) throws Exception;
	
	public abstract String getExecutionPlan() throws Exception;
	
	public void registerCachedFile(String filePath, String name){
		this.cacheFile.add(new Tuple2<String, String>(filePath, name));
	}
	
	protected void registerCachedFilesWithPlan(Plan p) throws IOException {
		for (Tuple2<String, String> entry : cacheFile) {
			p.registerCachedFile(entry.f0, entry.f1);
		}
	}
	
	public JavaPlan createProgramPlan() {
		return createProgramPlan(null);
	}
	
	public JavaPlan createProgramPlan(String jobName) {
		if (this.sinks.isEmpty()) {
			throw new RuntimeException("No data sinks have been created yet. A program needs at least one sink that consumes data. Examples are writing the data set or printing it.");
		}
		
		if (jobName == null) {
			jobName = getDefaultName();
		}
		
		OperatorTranslation translator = new OperatorTranslation();
		return translator.translateToPlan(this.sinks, jobName);
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
		return "Stratosphere Java Job at " + Calendar.getInstance().getTime();
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
		return contextEnvironment == null ? createLocalEnvironment() : contextEnvironment;
	}
	
	/**
	 * Creates a {@link LocalEnvironment}. The local execution environment will run the program in a
	 * multi-threaded fashion in the same JVM as the environment was created in. The default degree of
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
	 * degree of parallelism specified in the parameter.
	 * 
	 * @param degreeOfParallelism The degree of parallelism for the local environment.
	 * @return A local execution environment with the specified degree of parallelism.
	 */
	public static LocalEnvironment createLocalEnvironment(int degreeOfParallelism) {
		LocalEnvironment lee = new LocalEnvironment();
		lee.setDegreeOfParallelism(degreeOfParallelism);
		return lee;
	}
	
	/**
	 * Creates a {@link RemoteEnvironment}. The remote environment sends (parts of) the program 
	 * to a cluster for execution. Note that all file paths used in the program must be accessible from the
	 * cluster. The execution will use the cluster's default degree of parallelism, unless the parallelism is
	 * set explicitly via {@link ExecutionEnvironment#setDegreeOfParallelism(int)}.
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
	 * cluster. The execution will use the specified degree of parallelism.
	 * 
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed. 
	 * @param degreeOfParallelism The degree of parallelism to use during the execution.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 * @return A remote environment that executes the program on a cluster.
	 */
	public static ExecutionEnvironment createRemoteEnvironment(String host, int port, int degreeOfParallelism, String... jarFiles) {
		RemoteEnvironment rec = new RemoteEnvironment(host, port, jarFiles);
		rec.setDegreeOfParallelism(degreeOfParallelism);
		return rec;
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
	//  Methods to control the context and local environments for execution from packaged programs
	// --------------------------------------------------------------------------------------------
	
	protected static void initializeContextEnvironment(ExecutionEnvironment ctx) {
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
}
