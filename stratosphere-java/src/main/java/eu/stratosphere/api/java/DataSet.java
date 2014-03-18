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

import org.apache.commons.lang3.Validate;
import java.util.Arrays;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.io.CsvOutputFormat;
import eu.stratosphere.api.java.io.PrintingOutputFormat;
import eu.stratosphere.api.java.io.TextOutputFormat;
import eu.stratosphere.api.java.operators.AggregateOperator;
import eu.stratosphere.api.java.operators.CoGroupOperator;
import eu.stratosphere.api.java.operators.CrossOperator;
import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.operators.DistinctOperator;
import eu.stratosphere.api.java.operators.FilterOperator;
import eu.stratosphere.api.java.operators.FlatMapOperator;
import eu.stratosphere.api.java.operators.Grouping;
import eu.stratosphere.api.java.operators.JoinOperator.JoinHint;
import eu.stratosphere.api.java.operators.JoinOperator.JoinOperatorSets;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.operators.MapOperator;
import eu.stratosphere.api.java.operators.ProjectOperator.Projection;
import eu.stratosphere.api.java.operators.ReduceGroupOperator;
import eu.stratosphere.api.java.operators.ReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.InputTypeConfigurable;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.core.fs.Path;

/**
 *
 * @param <T> The data type of the data set.
 */
public abstract class DataSet<T> {
	
	private final ExecutionEnvironment context;
	
	private final TypeInformation<T> type;
	
	
	protected DataSet(ExecutionEnvironment context, TypeInformation<T> type) {
		if (context == null)
			throw new NullPointerException("context is null");

		if (type == null)
			throw new NullPointerException("type is null");
		
		this.context = context;
		this.type = type;
	}

	
	public ExecutionEnvironment getExecutionEnvironment() {
		return this.context;
	}
	
	public TypeInformation<T> getType() {
		return this.type;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Filter & Transformations
	// --------------------------------------------------------------------------------------------
	
	public <R> MapOperator<T, R> map(MapFunction<T, R> mapper) {
		return new MapOperator<T, R>(this, mapper);
	}
	
	public <R> FlatMapOperator<T, R> flatMap(FlatMapFunction<T, R> flatMapper) {
		return new FlatMapOperator<T, R>(this, flatMapper);
	}
	
	public FilterOperator<T> filter(FilterFunction<T> filter) {
		return new FilterOperator<T>(this, filter);
	}
	
	/**
	 * Selects a subset of fields of a Tuple to create new Tuples. 
	 * 
	 * @param fieldIndexes The field indexes select the fields of an input tuple that are kept in an output tuple.
	 * 					   The order of fields in the output tuple depends on the order of field indexes.
	 * @return Returns a Projection. Call the types() method with the correct number of class parameters 
	 *         to create a ProjectOperator. 
	 */
	public Projection<T> project(int... fieldIndexes) {
		
		if(!(this.getType() instanceof TupleTypeInfo)) {
			throw new UnsupportedOperationException("project() can only be applied to DataSets of Tuples.");
		}
		
		if(fieldIndexes.length == 0) {
			throw new IllegalArgumentException("project() needs to select at least one (1) field.");
		} else if(fieldIndexes.length > 22) {
			throw new IllegalArgumentException("project() may to select at most twenty-two (22) fields.");
		}
		
		int maxFieldIndex = ((TupleTypeInfo<?>)this.getType()).getArity();
		for(int i=0; i<fieldIndexes.length; i++) {
			if(fieldIndexes[i] > maxFieldIndex - 1) {
				throw new IndexOutOfBoundsException("Provided field index is out of bounds of input tuple.");
			}
		}
		
		return new Projection<T>(this, fieldIndexes);
	}
	
	/**
	 * Selects a subset of fields of a Tuple to create new Tuples. 
	 * 
	 * @param fieldMask The field mask indicates which fields of an input tuple that are kept ('1' or 'T') 
	 * 					in an output tuple and which are removed ('0', 'F').
	 * 				    The order of fields in the output tuple is the same as in the input tuple.
	 * @return Returns a Projection. Call the types() method with the correct number of class parameters 
	 *         to create a ProjectOperator. 
	 */
	public Projection<T> project(String fieldMask) {
		int[] fieldIndexes = new int[fieldMask.length()];
		int fieldCnt = 0;
		
		fieldMask = fieldMask.toUpperCase();
		for (int i = 0; i < fieldMask.length(); i++) {
			char c = fieldMask.charAt(i);
			if (c == '1' || c == 'T') {
				fieldIndexes[fieldCnt++] = i;
			} else if (c != '0' && c != 'F') {
				throw new IllegalArgumentException("Mask string may contain only '0' and '1'.");
			}
		}
		fieldIndexes = Arrays.copyOf(fieldIndexes, fieldCnt);
		
		return project(fieldIndexes);
	}
	
	/**
	 * Selects a subset of fields of a Tuple to create new Tuples. 
	 * 
	 * @param fieldMask The field flags indicates which fields of an input tuple that are kept (TRUE) 
	 * 					in an output tuple and which are removed (FALSE).
	 * 				    The order of fields in the output tuple is the same as in the input tuple.
	 * @return Returns a Projection. Call the types() method with the correct number of class parameters 
	 *         to create a ProjectOperator. 
	 */
	public Projection<T> project(boolean... fieldFlags) {
		int[] fieldIndexes = new int[fieldFlags.length];
		int fieldCnt = 0;
		
		for (int i = 0; i < fieldFlags.length; i++) {
			if (fieldFlags[i]) {
				fieldIndexes[fieldCnt++] = i;
			}
		}
		fieldIndexes = Arrays.copyOf(fieldIndexes, fieldCnt);
		
		return project(fieldIndexes);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Non-grouped aggregations
	// --------------------------------------------------------------------------------------------
	
	public AggregateOperator<T> aggregate(Aggregations agg, int field) {
		return new AggregateOperator<T>(this, agg, field);
	}
	
	public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
		return new ReduceOperator<T>(this, reducer);
	}
	
	public <R> ReduceGroupOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
		return new ReduceGroupOperator<T, R>(this, reducer);
	}
	
	// --------------------------------------------------------------------------------------------
	//  distinct
	// --------------------------------------------------------------------------------------------
	
	public <K extends Comparable<K>> DistinctOperator<T> distinct(KeySelector<T, K> keyExtractor) {
		return new DistinctOperator<T>(this, new Keys.SelectorFunctionKeys<T, K>(keyExtractor, getType()));
	}
	
//	public DistinctOperator<T> distinct(String fieldExpression) {
//		return new DistinctOperator<T>(this, new Keys.ExpressionKeys<T>(fieldExpression, getType()));
//	}
	
	public DistinctOperator<T> distinct(int... fields) {
		return new DistinctOperator<T>(this, new Keys.FieldPositionKeys<T>(fields, getType(), true));
	}
	
	// --------------------------------------------------------------------------------------------
	//  Grouping
	// --------------------------------------------------------------------------------------------

	public <K extends Comparable<K>> Grouping<T> groupBy(KeySelector<T, K> keyExtractor) {
		return new Grouping<T>(this, new Keys.SelectorFunctionKeys<T, K>(keyExtractor, getType()));
	}
	
//	public Grouping<T> groupBy(String fieldExpression) {
//		return new Grouping<T>(this, new Keys.ExpressionKeys<T>(fieldExpression, getType()));
//	}
	
	public Grouping<T> groupBy(int... fields) {
		return new Grouping<T>(this, new Keys.FieldPositionKeys<T>(fields, getType(), false));
	}
	
	// --------------------------------------------------------------------------------------------
	//  Grouping  Joining
	// --------------------------------------------------------------------------------------------
	
	public <R> JoinOperatorSets<T, R> join(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other);
	}
	
	public <R> JoinOperatorSets<T, R> joinWithTiny(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other, JoinHint.BROADCAST_HASH_SECOND);
	}
	
	public <R> JoinOperatorSets<T, R> joinWithHuge(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other, JoinHint.BROADCAST_HASH_FIRST);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Co-Grouping
	// --------------------------------------------------------------------------------------------

	public <R> CoGroupOperator.CoGroupOperatorSets<T, R> coGroup(DataSet<R> other) {
		return new CoGroupOperator.CoGroupOperatorSets<T, R>(this, other);
	}

	// --------------------------------------------------------------------------------------------
	//  Cross
	// --------------------------------------------------------------------------------------------

	public <R> CrossOperator.CrossOperatorSets<T, R> cross(DataSet<R> other) {
		return new CrossOperator.CrossOperatorSets<T, R>(this, other);
	}
	
	public <R> CrossOperator.CrossOperatorSets<T, R> crossWithTiny(DataSet<R> other) {
		return new CrossOperator.CrossOperatorSets<T, R>(this, other);
	}
	
	public <R> CrossOperator.CrossOperatorSets<T, R> crossWithHuge(DataSet<R> other) {
		return new CrossOperator.CrossOperatorSets<T, R>(this, other);
	}

	// --------------------------------------------------------------------------------------------
	//  Iterations
	// --------------------------------------------------------------------------------------------

	public IterativeDataSet<T> iterate(int maxIterations) {
		return new IterativeDataSet<T>(getExecutionEnvironment(), getType(), this, maxIterations);
	}

	// --------------------------------------------------------------------------------------------
	//  Union
	// --------------------------------------------------------------------------------------------

	// --------------------------------------------------------------------------------------------
	//  Top-K
	// --------------------------------------------------------------------------------------------
	
	// --------------------------------------------------------------------------------------------
	//  Result writing
	// --------------------------------------------------------------------------------------------
	
	public void writeAsText(String filePath) {
		output(new TextOutputFormat<T>(new Path(filePath)));
	}
	
	
	public void writeAsCsv(String filePath) {
		writeAsCsv(filePath, CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
	}
	
	public void writeAsCsv(String filePath, String rowDelimiter, String fieldDelimiter) {
		Validate.isTrue(this.type.isTupleType(), "The writeAsCsv() method can only be used on data sets of tuples.");
		internalWriteAsCsv(new Path(filePath), rowDelimiter, fieldDelimiter);
	}

	@SuppressWarnings("unchecked")
	private <X extends Tuple> void internalWriteAsCsv(Path filePath, String rowDelimiter, String fieldDelimiter) {
		output((OutputFormat<T>) new CsvOutputFormat<X>(filePath, rowDelimiter, fieldDelimiter));
	}
	
	
	public void print() {
		output(new PrintingOutputFormat<T>(false));
	}
	
	public void printToErr() {
		output(new PrintingOutputFormat<T>(true));
	}
	
	
	public void write(FileOutputFormat<T> outputFormat, String filePath) {
		Validate.notNull(filePath, "File path must not be null.");
		write(outputFormat, new Path(filePath));
	}
	
	public void write(FileOutputFormat<T> outputFormat, Path filePath) {
		Validate.notNull(filePath, "File path must not be null.");
		Validate.notNull(outputFormat, "The output format must not be null.");
		
		outputFormat.setOutputFilePath(filePath);
		output(outputFormat);
	}
	
	public DataSink<T> output(OutputFormat<T> outputFormat) {
		Validate.notNull(outputFormat);
		
		// configure the type if needed
		if (outputFormat instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) outputFormat).setInputType(this.type);
		}
		
		DataSink<T> sink = new DataSink<T>(this, outputFormat, this.type);
		this.context.registerDataSink(sink);
		return sink;
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	protected static void checkSameExecutionContext(DataSet<?> set1, DataSet<?> set2) {
		if (set1.context != set2.context)
			throw new IllegalArgumentException("The two inputs have different execution contexts.");
	}
}
