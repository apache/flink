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

import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.CoGroupFunction;
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
import eu.stratosphere.api.java.operators.CoGroupOperator.CoGroupOperatorSets;
import eu.stratosphere.api.java.operators.CrossOperator.CrossOperatorSets;
import eu.stratosphere.api.java.operators.CrossOperator;
import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.operators.FilterOperator;
import eu.stratosphere.api.java.operators.FlatMapOperator;
import eu.stratosphere.api.java.operators.Grouping;
import eu.stratosphere.api.java.operators.JoinOperator;
import eu.stratosphere.api.java.operators.JoinOperator.JoinHint;
import eu.stratosphere.api.java.operators.JoinOperator.JoinOperatorSets;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.operators.MapOperator;
import eu.stratosphere.api.java.operators.ProjectOperator;
import eu.stratosphere.api.java.operators.ProjectOperator.Projection;
import eu.stratosphere.api.java.operators.ReduceGroupOperator;
import eu.stratosphere.api.java.operators.ReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.InputTypeConfigurable;
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
	
	/**
	 * Applies a Map transformation on a {@link DataSet}.
	 * The transformation calls a {@link MapFunction} for each element of the DataSet.
	 * Each MapFunction call returns exactly one element.
	 * 
	 * @param mapper The MapFunction that is called for each element of the DataSet.
	 * @return A MapOperator that represents the transformed DataSet.
	 * 
	 * @see MapFunction
	 * @see MapOperator
	 * @see DataSet
	 */
	public <R> MapOperator<T, R> map(MapFunction<T, R> mapper) {
		return new MapOperator<T, R>(this, mapper);
	}
	
	/**
	 * Applies a FlatMap transformation on a {@link DataSet}.
	 * The transformation calls a {@link FlatMapFunction} for each element of the DataSet.
	 * Each FlatMapFunction call can return any number of elements including none.
	 * 
	 * @param flatMapper The FlatMapFunction that is called for each element of the DataSet. 
	 * @return A FlatMapOperator that represents the transformed DataSet.
	 * 
	 * @see FlatMapFunction
	 * @see FlatMapOperator
	 * @see DataSet
	 */
	public <R> FlatMapOperator<T, R> flatMap(FlatMapFunction<T, R> flatMapper) {
		return new FlatMapOperator<T, R>(this, flatMapper);
	}
	
	/**
	 * Applies a Filter transformation on a {@link DataSet}.
	 * The transformation calls a {@link FilterFunction} for each element of the DataSet 
	 * and retains only those element for which the function returns true. Elements for 
	 * which the function returns false are filtered. 
	 * 
	 * @param filter The FilterFunction that is called for each element of the DataSet.
	 * @return A FilterOperator that represents the filtered DataSet.
	 * 
	 * @see FilterFunction
	 * @see FilterOperator
	 * @see DataSet
	 */
	public FilterOperator<T> filter(FilterFunction<T> filter) {
		return new FilterOperator<T>(this, filter);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Projections
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Initiates a Project transformation on a {@link Tuple} {@link DataSet}.
	 * <b>Non-Tuple DataSets cannot be projected.</b></br>
	 * The transformation projects each Tuple of the DataSet onto a (sub)set of fields.</br>
	 * This method returns a {@link Projection} on which {@link Projection#types()} needs to 
	 *   be called to completed the transformation.
	 * 
	 * @param fieldIndexes The field indexes of the input tuples that are retained.
	 * 					   The order of fields in the output tuple corresponds to the order of field indexes.
	 * @return A Projection that needs to be converted into a {@link ProjectOperator} to complete the 
	 *           Project transformation by calling {@link Projection#types()}.
	 * 
	 * @see Tuple
	 * @see DataSet
	 * @see Projection
	 * @see ProjectOperator
	 */
	public Projection<T> project(int... fieldIndexes) {
		return new Projection<T>(this, fieldIndexes);
	}
	
	/**
	 * Initiates a Project transformation on a {@link Tuple} {@link DataSet}.
	 * <b>Non-Tuple DataSets cannot be projected.</b></br>
	 * The transformation projects each Tuple of the DataSet onto a (sub)set of fields.</br>
	 * This method returns a {@link Projection} on which {@link Projection#types()} needs to 
	 *   be called to completed the transformation.
	 * 
	 * @param fieldMask The field mask indicates the fields of an input tuple that are retained ('1' or 'T') 
	 * 					and that are removed ('0', 'F').
	 * 				    The order of fields in the output tuple is the same as in the input tuple.
	 * @return A Projection that needs to be converted into a {@link ProjectOperator} to complete the 
	 *           Project transformation by calling {@link Projection#types()}.
	 * 
	 * @see Tuple
	 * @see DataSet
	 * @see Projection
	 * @see ProjectOperator
	 */
	public Projection<T> project(String fieldMask) {
		return new Projection<T>(this, fieldMask);
	}
	
	/**
	 * Initiates a Project transformation on a {@link Tuple} {@link DataSet}.
	 * <b>Non-Tuple DataSets cannot be projected.</b></br>
	 * The transformation projects each Tuple of the DataSet onto a (sub)set of fields.</br>
	 * This method returns a {@link Projection} on which {@link Projection#types()} needs to 
	 *   be called to completed the transformation.
	 * 
	 * @param fieldMask The field flags indicates which fields of an input tuple that are retained (true) 
	 * 					and that are removed (false).
	 * 				    The order of fields in the output tuple is the same as in the input tuple.
	 * @return A Projection that needs to be converted into a {@link ProjectOperator} to complete the 
	 *           Project transformation by calling {@link Projection#types()}.
	 * 
	 * @see Tuple
	 * @see DataSet
	 * @see Projection
	 * @see ProjectOperator
	 */
	public Projection<T> project(boolean... fieldFlags) {
		return new Projection<T>(this, fieldFlags);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Non-grouped aggregations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Applies an Aggregate transformation on a non-grouped {@link Tuple} {@link DataSet}.
	 * <b>Non-Tuple DataSets cannot be aggregated.</b>
	 * The transformation applies a built-in {@link Aggregations Aggregation} on a specified field 
	 *   of a Tuple DataSet. Additional aggregation functions can be added to the resulting 
	 *   {@link AggregateOperator} by calling {@link AggregateOperator#and(Aggregations, int)}.
	 * 
	 * @param agg The built-in aggregation function that is computed.
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the aggregated DataSet. 
	 * 
	 * @see Tuple
	 * @see Aggregations
	 * @see AggregateOperator
	 * @see DataSet
	 */
	public AggregateOperator<T> aggregate(Aggregations agg, int field) {
		return new AggregateOperator<T>(this, agg, field);
	}
	
	/**
	 * Applies a Reduce transformation on a non-grouped {@link DataSet}.
	 * The transformation consecutively calls a {@link ReduceFunction} 
	 *   until only a single element remains which is the result of the transformation.
	 * A ReduceFunction combines two elements into one new element of the same type.
	 * 
	 * @param reducer The ReduceFunction that is applied on the DataSet.
	 * @return A ReduceOperator that represents the reduced DataSet.
	 * 
	 * @see ReduceFunction
	 * @see ReduceOperator
	 * @see DataSet
	 */
	public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
		return new ReduceOperator<T>(this, reducer);
	}
	
	/**
	 * Applies a GroupReduce transformation on a non-grouped {@link DataSet}.
	 * The transformation calls a {@link GroupReduceFunction} once with the full DataSet.
	 * The GroupReduceFunction can iterate over all elements of the DataSet and emit any
	 *   number of output elements including none.
	 * 
	 * @param reducer The GroupReduceFunction that is applied on the DataSet.
	 * @return A GroupReduceOperator that represents the reduced DataSet.
	 * 
	 * @see GroupReduceFunction
	 * @see GroupReduceOperator
	 * @see DataSet
	 */
	public <R> ReduceGroupOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
		return new ReduceGroupOperator<T, R>(this, reducer);
	}
	
	// --------------------------------------------------------------------------------------------
	//  distinct
	// --------------------------------------------------------------------------------------------
	
//	public <K extends Comparable<K>> DistinctOperator<T> distinct(KeySelector<T, K> keyExtractor) {
//		return new DistinctOperator<T>(this, new Keys.SelectorFunctionKeys<T, K>(keyExtractor, getType()));
//	}
	
//	public DistinctOperator<T> distinct(String fieldExpression) {
//		return new DistinctOperator<T>(this, new Keys.ExpressionKeys<T>(fieldExpression, getType()));
//	}
	
//	public DistinctOperator<T> distinct(int... fields) {
//		return new DistinctOperator<T>(this, new Keys.FieldPositionKeys<T>(fields, getType(), true));
//	}
	
	// --------------------------------------------------------------------------------------------
	//  Grouping
	// --------------------------------------------------------------------------------------------

	/**
	 * Groups a {@link DataSet} using a {@link KeySelector} function. 
	 * The KeySelector function is called for each element of the DataSet and extracts a single 
	 *   key value on which the DataSet is grouped. </br>
	 * This method returns a {@link Grouping} on which one of the following grouping transformation 
	 *   needs to be applied to obtain a transformed DataSet. 
	 * <ul>
	 *   <li>{@link Grouping#aggregate(Aggregations, int)}
	 *   <li>{@link Grouping#reduce(ReduceFunction)}
	 *   <li>{@link Grouping#reduceGroup(GroupReduceFunction)}
	 * </ul>
	 *  
	 * @param keyExtractor The KeySelector function which extracts the key values from the DataSet on which it is grouped. 
	 * @return A Grouping on which a transformation needs to be applied to obtain a transformed DataSet.
	 * 
	 * @see KeySelector
	 * @see Grouping
	 * @see AggregateOperator
	 * @see ReduceOperator
	 * @see GroupReduceOperator
	 * @see DataSet
	 */
	public <K extends Comparable<K>> Grouping<T> groupBy(KeySelector<T, K> keyExtractor) {
		return new Grouping<T>(this, new Keys.SelectorFunctionKeys<T, K>(keyExtractor, getType()));
	}
	
//	public Grouping<T> groupBy(String fieldExpression) {
//		return new Grouping<T>(this, new Keys.ExpressionKeys<T>(fieldExpression, getType()));
//	}
	
	/**
	 * Groups a {@link Tuple} {@link DataSet} using field position keys. 
	 * <b>Field position keys cannot be specified for non-Tuple DataSets.</b></br>
	 * The field position keys specify the fields of Tuples on which the DataSet is grouped.
	 * This method returns a {@link Grouping} on which one of the following grouping transformation 
	 *   needs to be applied to obtain a transformed DataSet. 
	 * <ul>
	 *   <li>{@link Grouping#aggregate(Aggregations, int)}
	 *   <li>{@link Grouping#reduce(ReduceFunction)}
	 *   <li>{@link Grouping#reduceGroup(GroupReduceFunction)}
	 * </ul> 
	 * 
	 * @param fields One or more field positions on which the DataSet will be grouped. 
	 * @return A Grouping on which a transformation needs to be applied to obtain a transformed DataSet.
	 * 
	 * @see Tuple
	 * @see Grouping
	 * @see AggregateOperator
	 * @see ReduceOperator
	 * @see GroupReduceOperator
	 * @see DataSet
	 */
	public Grouping<T> groupBy(int... fields) {
		return new Grouping<T>(this, new Keys.FieldPositionKeys<T>(fields, getType(), false));
	}
	
	// --------------------------------------------------------------------------------------------
	//  Joining
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Initiates a Join transformation. A Join transformation joins the elements of two 
	 *   {@link DataSet DataSets} on key equality and provides multiple ways to combine 
	 *   joining elements into one DataSet.</br>
	 * 
	 * This method returns a {@link JoinOperatorSets} on which 
	 *   {@link JoinOperatorSets#where()} needs to be called to define the join key of the first 
	 *   joining (i.e., this) DataSet.
	 *  
	 * @param other The other DataSet with which this DataSet is joined.
	 * @return A JoinOperatorSets to continue the definition of the Join transformation.
	 * 
	 * @see JoinOperatorSets
	 * @see JoinOperator
	 * @see DataSet
	 */
	public <R> JoinOperatorSets<T, R> join(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other);
	}
	
	/**
	 * Initiates a Join transformation. A Join transformation joins the elements of two 
	 *   {@link DataSet DataSets} on key equality and provides multiple ways to combine 
	 *   joining elements into one DataSet.</br>
	 * This method also gives the hint to the optimizer that the second DataSet to join is much
	 *   smaller than the first one.</br>
	 * This method returns a {@link JoinOperatorSets} on which 
	 *   {@link JoinOperatorSets#where()} needs to be called to define the join key of the first 
	 *   joining (i.e., this) DataSet.
	 *  
	 * @param other The other DataSet with which this DataSet is joined.
	 * @return A JoinOperatorSets to continue the definition of the Join transformation.
	 * 
	 * @see JoinOperatorSets
	 * @see JoinOperator
	 * @see DataSet
	 */
	public <R> JoinOperatorSets<T, R> joinWithTiny(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other, JoinHint.BROADCAST_HASH_SECOND);
	}
	
	/**
	 * Initiates a Join transformation. A Join transformation joins the elements of two 
	 *   {@link DataSet DataSets} on key equality and provides multiple ways to combine 
	 *   joining elements into one DataSet.</br>
	 * This method also gives the hint to the optimizer that the second DataSet to join is much
	 *   larger than the first one.</br>
	 * This method returns a {@link JoinOperatorSets JoinOperatorSet} on which 
	 *   {@link JoinOperatorSets#where()} needs to be called to define the join key of the first 
	 *   joining (i.e., this) DataSet.
	 *  
	 * @param other The other DataSet with which this DataSet is joined.
	 * @return A JoinOperatorSet to continue the definition of the Join transformation.
	 * 
	 * @see JoinOperatorSets
	 * @see JoinOperator
	 * @see DataSet
	 */
	public <R> JoinOperatorSets<T, R> joinWithHuge(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other, JoinHint.BROADCAST_HASH_FIRST);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Co-Grouping
	// --------------------------------------------------------------------------------------------

	/**
	 * Initiates a CoGroup transformation. A CoGroup transformation combines the elements of
	 *   two {@link DataSet DataSets} into one DataSet. It groups each DataSet individually on a key and 
	 *   gives groups of both DataSets with equal keys together into a {@link CoGroupFunction}.
	 *   If a DataSet has a group with no matching key in the other DataSet, the CoGroupFunction
	 *   is called with an empty group for the non-existing group.</br>
	 * The CoGroupFunction can iterate over the elements of both groups and return any number 
	 *   of elements including none.</br>
	 * This method returns a {@link CoGroupOperatorSets} on which 
	 *   {@link CoGroupOperatorSets#where(} needs to be called to define the grouping key of the first 
	 *   (i.e., this) DataSet.
	 * 
	 * @param other The other DataSet of the CoGroup transformation.
	 * @return A CoGroupOperatorSets to continue the definition of the CoGroup transformation.
	 * 
	 * @see CoGroupOperatorSets
	 * @see CoGroupOperator
	 * @see DataSet
	 */
	public <R> CoGroupOperator.CoGroupOperatorSets<T, R> coGroup(DataSet<R> other) {
		return new CoGroupOperator.CoGroupOperatorSets<T, R>(this, other);
	}

	// --------------------------------------------------------------------------------------------
	//  Cross
	// --------------------------------------------------------------------------------------------

	/**
	 * Initiates a Cross transformation. A Cross transformation combines the elements of two 
	 *   {@link DataSet DataSets} into one DataSet. It builds all pair combinations of elements of 
	 *   both DataSets, i.e., it builds a Cartesian product, and calls a {@link CrossFunction} for
	 *   each pair of elements.</br>
	 * The CrossFunction returns a exactly one element for each pair of input elements.</br>
	 * This method returns a {@link CrossOperatorSets} on which 
	 *   {@link CrossOperatorSets#with()} needs to be called to define the CrossFunction that 
	 *   is applied.
	 * 
	 * @param other The other DataSet with which this DataSet is crossed. 
	 * @return A CrossOperatorSets to continue the definition of the Cross transformation.
	 * 
	 * @see CrossFunction
	 * @see CrossOperatorSets
	 * @see DataSet
	 */
	public <R> CrossOperator.CrossOperatorSets<T, R> cross(DataSet<R> other) {
		return new CrossOperator.CrossOperatorSets<T, R>(this, other);
	}
	
	/**
	 * Initiates a Cross transformation. A Cross transformation combines the elements of two 
	 *   {@link DataSet DataSets} into one DataSet. It builds all pair combinations of elements of 
	 *   both DataSets, i.e., it builds a Cartesian product, and calls a {@link CrossFunction} for
	 *   each pair of elements.</br>
	 * The CrossFunction returns a exactly one element for each pair of input elements.</br>
	 * This method also gives the hint to the optimizer that the second DataSet to cross is much
	 *   smaller than the first one.</br>
	 * This method returns a {@link CrossOperatorSets CrossOperatorSet} on which 
	 *   {@link CrossOperatorSets#with()} needs to be called to define the CrossFunction that 
	 *   is applied.
	 * 
	 * @param other The other DataSet with which this DataSet is crossed. 
	 * @return A CrossOperatorSets to continue the definition of the Cross transformation.
	 * 
	 * @see CrossFunction
	 * @see CrossOperatorSets
	 * @see DataSet
	 */
	public <R> CrossOperator.CrossOperatorSets<T, R> crossWithTiny(DataSet<R> other) {
		return new CrossOperator.CrossOperatorSets<T, R>(this, other);
	}
	
	/**
	 * Initiates a Cross transformation. A Cross transformation combines the elements of two 
	 *   {@link DataSet DataSets} into one DataSet. It builds all pair combinations of elements of 
	 *   both DataSets, i.e., it builds a Cartesian product, and calls a {@link CrossFunction} for
	 *   each pair of elements.</br>
	 * The CrossFunction returns a exactly one element for each pair of input elements.</br>
	 * This method also gives the hint to the optimizer that the second DataSet to cross is much
	 *   larger than the first one.</br>
	 * This method returns a {@link CrossOperatorSets CrossOperatorSet} on which 
	 *   {@link CrossOperatorSets#with()} needs to be called to define the CrossFunction that 
	 *   is applied.
	 * 
	 * @param other The other DataSet with which this DataSet is crossed. 
	 * @return A CrossOperatorSets to continue the definition of the Cross transformation.
	 * 
	 * @see CrossFunction
	 * @see CrossOperatorSets
	 * @see DataSet
	 */
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
