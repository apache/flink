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
import eu.stratosphere.api.java.operators.*;
import eu.stratosphere.api.java.operators.JoinOperator.JoinHint;
import eu.stratosphere.api.java.operators.JoinOperator.JoinOperatorSets;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.operators.MapOperator;
import eu.stratosphere.api.java.operators.ReduceGroupOperator;
import eu.stratosphere.api.java.operators.ReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple;
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
	
	public DistinctOperator<T> distinct(String fieldExpression) {
		return new DistinctOperator<T>(this, new Keys.ExpressionKeys<T>(fieldExpression, getType()));
	}
	
	public DistinctOperator<T> distinct(int... fields) {
		return new DistinctOperator<T>(this, new Keys.FieldPositionKeys<T>(fields, getType(), true));
	}
	
	// --------------------------------------------------------------------------------------------
	//  Grouping
	// --------------------------------------------------------------------------------------------

	public <K extends Comparable<K>> Grouping<T> groupBy(KeySelector<T, K> keyExtractor) {
		return new Grouping<T>(this, new Keys.SelectorFunctionKeys<T, K>(keyExtractor, getType()));
	}
	
	public Grouping<T> groupBy(String fieldExpression) {
		return new Grouping<T>(this, new Keys.ExpressionKeys<T>(fieldExpression, getType()));
	}
	
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
	//  Union
	// --------------------------------------------------------------------------------------------

	// --------------------------------------------------------------------------------------------
	//  Top-K
	// --------------------------------------------------------------------------------------------
	
	// --------------------------------------------------------------------------------------------
	//  Result writing
	// --------------------------------------------------------------------------------------------
	
	public void writeAsText(String path) {
		writeAsText(new Path(path));
	}
	
	public void writeAsText(Path filePath) {
		output(new TextOutputFormat<T>(filePath));
	}
	
	
	public void writeAsCsv(String filePath) {
		writeAsCsv(new Path(filePath));
	}
	
	public void writeAsCsv(Path filePath) {
		writeAsCsv(filePath, "\n", ",");
	}
	
	public void writeAsCsv(String filePath, String rowDelimiter, String fieldDelimiter) {
		writeAsCsv(new Path(filePath), rowDelimiter, fieldDelimiter);
	}
	
	public void writeAsCsv(Path filePath, String rowDelimiter, String fieldDelimiter) {
		internalWriteAsCsv(filePath, rowDelimiter, fieldDelimiter);
	}

	@SuppressWarnings("unchecked")
	private <X extends Tuple> void internalWriteAsCsv(Path filePath, String rowDelimiter,
	                                                    String fieldDelimiter) {
		output((OutputFormat<T>) new CsvOutputFormat<X>(filePath, rowDelimiter, fieldDelimiter));
	}
	
	
	public void print() {
		output(new PrintingOutputFormat<T>(false));
	}
	
	public void printToErr() {
		output(new PrintingOutputFormat<T>(true));
	}
	
	
	public void write(FileOutputFormat<T> outputFormat, String filePath) {
		if (filePath == null)
			throw new IllegalArgumentException("File path must not be null.");
		
		write(outputFormat, new Path(filePath));
	}
	
	public void write(FileOutputFormat<T> outputFormat, Path filePath) {
		if (filePath == null)
			throw new IllegalArgumentException("File path must not be null.");
		if (outputFormat == null)
			throw new IllegalArgumentException("The output format must not be null.");
		
		outputFormat.setOutputFilePath(filePath);
		output(outputFormat);
	}
	
	public DataSink<T> output(OutputFormat<T> outputFormat) {
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
