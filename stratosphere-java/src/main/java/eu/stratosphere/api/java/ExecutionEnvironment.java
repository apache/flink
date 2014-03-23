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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.java.io.CollectionInputFormat;
import eu.stratosphere.api.java.io.CsvReader;
import eu.stratosphere.api.java.io.IteratorInputFormat;
import eu.stratosphere.api.java.io.ParallelIteratorInputFormat;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.io.TextValueInputFormat;
import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.operators.DataSource;
import eu.stratosphere.api.java.operators.OperatorTranslation;
import eu.stratosphere.api.java.operators.translation.JavaPlan;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.Typed;
import eu.stratosphere.api.java.typeutils.ValueTypeInfo;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.NumberSequenceIterator;
import eu.stratosphere.util.SplittableIterator;


public abstract class ExecutionEnvironment {
	
	private static ExecutionEnvironment contextEnvironment;
	
	private static int defaultLocalDop = Runtime.getRuntime().availableProcessors();
	
	// --------------------------------------------------------------------------------------------
	
	private final UUID executionId;
	
	private final List<DataSink<?>> sinks = new ArrayList<DataSink<?>>();
	
	private int degreeOfParallelism = -1;
	
	
	// --------------------------------------------------------------------------------------------
	//  Constructor and Properties
	// --------------------------------------------------------------------------------------------
	
	protected ExecutionEnvironment() {
		this.executionId = UUID.randomUUID();
	}
	
	public int getDegreeOfParallelism() {
		return degreeOfParallelism;
	}
	
	public void setDegreeOfParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1)
			throw new IllegalArgumentException("Degree of parallelism must be at least one.");
		
		this.degreeOfParallelism = degreeOfParallelism;
	}
	
	public UUID getId() {
		return this.executionId;
	}
	
	public String getIdString() {
		return this.executionId.toString();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Data set creations
	// --------------------------------------------------------------------------------------------

	// ---------------------------------- Text Input Format ---------------------------------------
	
	public DataSet<String> readTextFile(String filePath) {
		if (filePath == null)
			throw new IllegalArgumentException("The file path may not be null.");
		
		return new DataSource<String>(this, new TextInputFormat(new Path(filePath)), BasicTypeInfo.STRING_TYPE_INFO );
	}
	
	public DataSet<String> readTextFile(String filePath, String charsetName) {
		if (filePath == null)
			throw new IllegalArgumentException("The file path may not be null.");

		TextInputFormat format = new TextInputFormat(new Path(filePath));
		format.setCharsetName(charsetName);
		return new DataSource<String>(this, format, BasicTypeInfo.STRING_TYPE_INFO );
	}
	
	// -------------------------- Text Input Format With String Value------------------------------
	
	public DataSet<StringValue> readTextFileWithValue(String filePath) {
		if (filePath == null)
			throw new IllegalArgumentException("The file path may not be null.");
		
		return new DataSource<StringValue>(this, new TextValueInputFormat(new Path(filePath)), new ValueTypeInfo<StringValue>(StringValue.class) );
	}
	
	public DataSet<StringValue> readTextFileWithValue(String filePath, String charsetName, boolean skipInvalidLines) {
		if (filePath == null)
			throw new IllegalArgumentException("The file path may not be null.");
		
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
	
	public <X> DataSet<X> createInput(InputFormat<X, ?> inputFormat) {
		if (inputFormat == null) {
			throw new IllegalArgumentException("InputFormat must not be null.");
		}
		
		@SuppressWarnings("unchecked")
		TypeInformation<X> producedType = (inputFormat instanceof Typed) ?
				(TypeInformation<X>) ((Typed) inputFormat).getProducedType() :
				TypeExtractor.extractInputFormatTypes(inputFormat);
		
		return createInput(inputFormat, producedType);
	}
	
	public <X> DataSet<X> createInput(InputFormat<X, ?> inputFormat, TypeInformation<X> producedType) {
		if (inputFormat == null)
			throw new IllegalArgumentException("InputFormat must not be null.");
		
		if (producedType == null)
			throw new IllegalArgumentException("Produced type information must not be null.");
		
		return new DataSource<X>(this, inputFormat, producedType);
	}
	
	// ----------------------------------- Collection ---------------------------------------
	
	public <X> DataSet<X> fromCollection(Collection<X> data) {
		if (data == null)
			throw new IllegalArgumentException("The data must not be null.");
		
		if (data.size() == 0)
			throw new IllegalArgumentException("The size of the collection must not be empty.");
		
		X firstValue = data.iterator().next();
		
		return fromCollection(data, TypeInformation.getForObject(firstValue));
	}
	
	
	public <X> DataSet<X> fromCollection(Collection<X> data, TypeInformation<X> type) {
		CollectionInputFormat.checkCollection(data, type.getTypeClass());
		
		return new DataSource<X>(this, new CollectionInputFormat<X>(data), type);
	}
	
	public <X> DataSet<X> fromCollection(Iterator<X> data, Class<X> type) {
		return fromCollection(data, TypeInformation.getForClass(type));
	}
	
	public <X> DataSet<X> fromCollection(Iterator<X> data, TypeInformation<X> type) {
		if (!(data instanceof Serializable))
			throw new IllegalArgumentException("The iterator must be serializable.");
		
		return new DataSource<X>(this, new IteratorInputFormat<X>(data), type);
	}
	
	
	/**
	 * Creates a new data set that contains the given elements. The elements must all be of the same type,
	 * for example, all of the Strings or integers.
	 * 
	 * @param data The elements to make up the data set.
	 * @return A data set representing the given list of elements.
	 */
	public <X> DataSet<X> fromElements(X... data) {
		if (data == null) {
			throw new IllegalArgumentException("The data must not be null.");
		}
		if (data.length == 0) {
			throw new IllegalArgumentException("The number of elements must not be zero.");
		}
		
		return fromCollection(Arrays.asList(data), TypeInformation.getForObject(data[0]));
	}
	
	
	public <X> DataSet<X> fromParallelCollection(SplittableIterator<X> iterator, Class<X> type) {
		return fromParallelCollection(iterator, TypeInformation.getForClass(type));
	}
	
	
	public <X> DataSet<X> fromParallelCollection(SplittableIterator<X> iterator, TypeInformation<X> type) {
		return new DataSource<X>(this, new ParallelIteratorInputFormat<X>(iterator), type);
	}
	
	
	public DataSet<Long> generateSequence(long from, long to) {
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
	
	public JavaPlan createProgramPlan() {
		return createProgramPlan(null);
	}
	
	public JavaPlan createProgramPlan(String jobName) {
		if (this.sinks.isEmpty()) {
			throw new RuntimeException("No data sinks have been created yet.");
		}
		
		if (jobName == null) {
			jobName = getDefaultName();
		}
		
		OperatorTranslation translator = new OperatorTranslation();
		return translator.translateToPlan(this.sinks, jobName);
	}
	
	void registerDataSink(DataSink<?> sink) {
		this.sinks.add(sink);
	}
	
	private static String getDefaultName() {
		return "Stratosphere Java Job at " + Calendar.getInstance().getTime();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Instantiation of Execution Contexts
	// --------------------------------------------------------------------------------------------
	
	public static ExecutionEnvironment getExecutionEnvironment() {
		return contextEnvironment == null ? createLocalEnvironment() : contextEnvironment;
	}
	
	public static LocalEnvironment createLocalEnvironment() {
		return createLocalEnvironment(defaultLocalDop);
	}
	
	public static LocalEnvironment createLocalEnvironment(int degreeOfParallelism) {
		LocalEnvironment lee = new LocalEnvironment();
		lee.setDegreeOfParallelism(degreeOfParallelism);
		return lee;
	}
	
	public static ExecutionEnvironment createRemoteEnvironment(String host, int port, String... jarFiles) {
		return new RemoteEnvironment(host, port, jarFiles);
	}
	
	public static ExecutionEnvironment createRemoteEnvironment(String host, int port, int degreeOfParallelism, String... jarFiles) {
		RemoteEnvironment rec = new RemoteEnvironment(host, port, jarFiles);
		rec.setDegreeOfParallelism(degreeOfParallelism);
		return rec;
	}
	
	public static void setDefaultLocalParallelism(int degreeOfParallelism) {
		defaultLocalDop = degreeOfParallelism;
	}
	
	protected static void initializeContextEnvironment(ExecutionEnvironment ctx) {
		contextEnvironment = ctx;
	}
	
	protected boolean isContextEnvironmentSet() {
		return contextEnvironment != null;
	}
}
