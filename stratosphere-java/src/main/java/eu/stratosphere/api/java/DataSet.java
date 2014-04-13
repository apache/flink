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
import eu.stratosphere.api.java.functions.CrossFunction;
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
import eu.stratosphere.api.java.operators.CrossOperator;
import eu.stratosphere.api.java.operators.CrossOperator.CrossOperatorSets;
import eu.stratosphere.api.java.operators.CustomUnaryOperation;
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
import eu.stratosphere.api.java.operators.UnionOperator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.InputTypeConfigurable;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.core.fs.Path;

/**
 * A DataSet represents a collection of elements of the same type.<br/>
 * A DataSet can be transformed into another DataSet by applying a transformation as for example 
 * <ul>
 *   <li>{@link DataSet#map(MapFunction)},</li>
 *   <li>{@link DataSet#reduce(ReduceFunction)},</li>
 *   <li>{@link DataSet#join(DataSet)}, or</li>
 *   <li>{@link DataSet#coGroup(DataSet)}.</li>
 * </ul>
 *
 * @param <T> The type of the DataSet, i.e., the type of the elements of the DataSet.
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

	/**
	 * Returns the {@link ExecutionEnvironment} in which this DataSet is registered.
	 * 
	 * @return The ExecutionEnvironment in which this DataSet is registered.
	 * 
	 * @see ExecutionEnvironment
	 */
	public ExecutionEnvironment getExecutionEnvironment() {
		return this.context;
	}
	
	/**
	 * Returns the {@link TypeInformation} for the type of this DataSet.
	 * 
	 * @return The TypeInformation for the type of this DataSet.
	 * 
	 * @see TypeInformation
	 */
	public TypeInformation<T> getType() {
		return this.type;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Filter & Transformations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Applies a Map transformation on a {@link DataSet}.<br/>
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
	 * Applies a FlatMap transformation on a {@link DataSet}.<br/>
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
	 * Applies a Filter transformation on a {@link DataSet}.<br/>
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
	 * Initiates a Project transformation on a {@link Tuple} {@link DataSet}.<br/>
	 * <b>Note: Only Tuple DataSets can be projected.</b></br>
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
	
	// --------------------------------------------------------------------------------------------
	//  Non-grouped aggregations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Applies an Aggregate transformation on a non-grouped {@link Tuple} {@link DataSet}.<br/>
	 * <b>Note: Only Tuple DataSets can be aggregated.</b>
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
	 * Applies a Reduce transformation on a non-grouped {@link DataSet}.<br/>
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
	 * Applies a GroupReduce transformation on a non-grouped {@link DataSet}.<br/>
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
	 * Groups a {@link Tuple} {@link DataSet} using field position keys.<br/> 
	 * <b>Note: Field position keys only be specified for Tuple DataSets.</b></br>
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
	 * Initiates a Join transformation. <br/>
	 * A Join transformation joins the elements of two 
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
	 * Initiates a Join transformation. <br/>
	 * A Join transformation joins the elements of two 
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
	 * Initiates a Join transformation.<br/>
	 * A Join transformation joins the elements of two 
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
	 * Initiates a CoGroup transformation.<br/>
	 * A CoGroup transformation combines the elements of
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
	 * Initiates a Cross transformation.<br/>
	 * A Cross transformation combines the elements of two 
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
	 * Initiates a Cross transformation.<br/>
	 * A Cross transformation combines the elements of two 
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
	 * Initiates a Cross transformation.<br/>
	 * A Cross transformation combines the elements of two 
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

	/**
	 * Union with an other {@link Tuple} {@link DataSet} which must be of the same type.
	 * 
	 * @param other The other DataSet which is appended to the current DataSet via union .
	 * @return The result DataSet which is of the same type as the two inputs
	 */

	public UnionOperator<T> union(DataSet<T> other){
		return new UnionOperator<T>(this, other);
	}


	// --------------------------------------------------------------------------------------------
	//  Iterations
	// --------------------------------------------------------------------------------------------

	/**
	 * Initiates an iteration with a specified number of maximum iterations. <br/>
	 * The iteration needs to be closed by calling {@link IterativeDataSet#closeWith(DataSet)} with 
	 * a DataSet that succeeds this DataSet. 
	 * While iterating, the result of the DataSet with which the iteration is closed 
	 * will be fed back to this point. 
	 *  
	 * @param maxIterations The maximum number of iterations.
	 * @return An IterativeDataSet that needs to be closed by {@link IterativeDataSet#closeWith(DataSet)}.
	 * 
	 * @see IterativeDataSet
	 */
	public IterativeDataSet<T> iterate(int maxIterations) {
		return new IterativeDataSet<T>(getExecutionEnvironment(), getType(), this, maxIterations);
	}
	
	/**
	 * piped DataSet considered SolutionSet
	 */	
	public <R> DeltaIterativeDataSet<T, R> iterateDelta(DataSet<R> workset, int maxIterations, int keyPosition) {
		return iterateDelta(workset, maxIterations, new int[] {keyPosition});
	}
	
	public <R> DeltaIterativeDataSet<T, R> iterateDelta(DataSet<R> workset, int maxIterations, int [] keyPositions) {
		Keys.FieldPositionKeys<T> keys = new Keys.FieldPositionKeys<T>(keyPositions, getType(), false);
		return new DeltaIterativeDataSet<T, R>(getExecutionEnvironment(), getType(), this, workset, keys, maxIterations);
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Operators
	// -------------------------------------------------------------------------------------------
	
	public <X> DataSet<X> runOperation(CustomUnaryOperation<T, X> operation) {
		Validate.notNull(operation, "The custom operator must not be null.");
		operation.setInput(this);
		return operation.createOperator();
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
	
	/**
	 * Writes a DataSet as a text file to the specified location.<br/>
	 * For each element of the DataSet the result of {@link Object#toString()} is written.  
	 * 
	 * @param filePath The path pointing to the location the text file is written to. 
	 * 
	 * @see TextOutputFormat
	 */
	public void writeAsText(String filePath) {
		output(new TextOutputFormat<T>(new Path(filePath)));
	}
	
	/**
	 * Writes a {@link Tuple} DataSet as a CSV file to the specified location.<br/>
	 * <b>Note: Only a Tuple DataSet can written as a CSV file.</b><br/>
	 * For each Tuple field the result of {@link Object#toString()} is written.
	 * Tuple fields are separated by the default field delimiter {@link CsvOutputFormat.DEFAULT_FIELD_DELIMITER}.<br/>
	 * Tuples are are separated by the default line delimiter {@link CsvOutputFormat.DEFAULT_LINE_DELIMITER}.
	 * 
	 * @param filePath The path pointing to the location the CSV file is written to.
	 * 
	 * @see Tuple
	 * @see CsvOutputFormat
	 */
	public void writeAsCsv(String filePath) {
		writeAsCsv(filePath, CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
	}
	
	/**
	 * Writes a {@link Tuple} DataSet as a CSV file to the specified location with the specified field and line delimiters.<br/>
	 * <b>Note: Only a Tuple DataSet can written as a CSV file.</b><br/>
	 * For each Tuple field the result of {@link Object#toString()} is written.
	 * 
	 * @param filePath The path pointing to the location the CSV file is written to.
	 * @param rowDelimiter The row delimiter to separate Tuples.
	 * @param fieldDelimiter The field delimiter to separate Tuple fields.
	 * 
	 * @see Tuple
	 * @see CsvOutputFormat
	 */
	public void writeAsCsv(String filePath, String rowDelimiter, String fieldDelimiter) {
		Validate.isTrue(this.type.isTupleType(), "The writeAsCsv() method can only be used on data sets of tuples.");
		internalWriteAsCsv(new Path(filePath), rowDelimiter, fieldDelimiter);
	}

	@SuppressWarnings("unchecked")
	private <X extends Tuple> void internalWriteAsCsv(Path filePath, String rowDelimiter, String fieldDelimiter) {
		output((OutputFormat<T>) new CsvOutputFormat<X>(filePath, rowDelimiter, fieldDelimiter));
	}
	
	/**
	 * Writes a DataSet to the standard output stream (stdout).<br/>
	 * For each element of the DataSet the result of {@link Object#toString()} is written. 
	 */
	public void print() {
		output(new PrintingOutputFormat<T>(false));
	}
	
	/**
	 * Writes a DataSet to the standard error stream (stderr).<br/>
	 * For each element of the DataSet the result of {@link Object#toString()} is written.
	 */
	public void printToErr() {
		output(new PrintingOutputFormat<T>(true));
	}
	
	/**
	 * Writes a DataSet using a {@link FileOutputFormat} to a specified location.
	 * 
	 * @param outputFormat The FileOutputFormat to write the DataSet.
	 * @param filePath The path to the location where the DataSet is written.
	 * 
	 * @see FileOutputFormat
	 */
	public void write(FileOutputFormat<T> outputFormat, String filePath) {
		Validate.notNull(filePath, "File path must not be null.");
		Validate.notNull(outputFormat, "Output format must not be null.");

		outputFormat.setOutputFilePath(new Path(filePath));
		output(outputFormat);
	}
	
	/**
	 * Writes a DataSet using a {@link FileOutputFormat} to a specified location.
	 * 
	 * @param outputFormat The FileOutputFormat to write the DataSet.
	 * @param filePath The path to the location where the DataSet is written.
	 * @param writeMode The mode of writing, indicating whether to overwrite existing files.
	 * 
	 * @see FileOutputFormat
	 */
	public void write(FileOutputFormat<T> outputFormat, String filePath, WriteMode writeMode) {
		Validate.notNull(filePath, "File path must not be null.");
		Validate.notNull(writeMode, "Write mode must not be null.");
		Validate.notNull(outputFormat, "Output format must not be null.");

		outputFormat.setOutputFilePath(new Path(filePath));
		outputFormat.setWriteMode(writeMode);
		output(outputFormat);
	}
	
	/**
	 * Writes a DataSet using an {@link OutputFormat}.
	 * 
	 * @param outputFormat The OutputFormat to write the DataSet.
	 * @return The DataSink that writes the DataSet.
	 * 
	 * @see OutputFormat
	 * @see DataSink
	 */
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
