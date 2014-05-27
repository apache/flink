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
import eu.stratosphere.api.java.operators.CrossOperator;
import eu.stratosphere.api.java.operators.CrossOperator.DefaultCross;
import eu.stratosphere.api.java.operators.CustomUnaryOperation;
import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.operators.DistinctOperator;
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
import eu.stratosphere.api.java.operators.SortedGrouping;
import eu.stratosphere.api.java.operators.UnionOperator;
import eu.stratosphere.api.java.operators.UnsortedGrouping;
import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.InputTypeConfigurable;
import eu.stratosphere.types.TypeInformation;
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
		if (context == null) {
			throw new NullPointerException("context is null");
		}

		if (type == null) {
			throw new NullPointerException("type is null");
		}
		
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
		if (mapper == null) {
			throw new NullPointerException("Map function must not be null.");
		}
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
		if (flatMapper == null) {
			throw new NullPointerException("FlatMap function must not be null.");
		}
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
		if (filter == null) {
			throw new NullPointerException("Filter function must not be null.");
		}
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
	 * Syntactic sugar for aggregate (SUM, field)
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the summed DataSet.
	 *
	 * @see eu.stratosphere.api.java.operators.AggregateOperator
	 */
	public AggregateOperator<T> sum (int field) {
		return this.aggregate (Aggregations.SUM, field);
	}

	/**
	 * Syntactic sugar for aggregate (MAX, field)
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the max'ed DataSet.
	 *
	 * @see eu.stratosphere.api.java.operators.AggregateOperator
	 */
	public AggregateOperator<T> max (int field) {
		return this.aggregate (Aggregations.MAX, field);
	}

	/**
	 * Syntactic sugar for aggregate (MIN, field)
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the min'ed DataSet.
	 *
	 * @see eu.stratosphere.api.java.operators.AggregateOperator
	 */
	public AggregateOperator<T> min (int field) {
		return this.aggregate (Aggregations.MIN, field);
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
		if (reducer == null) {
			throw new NullPointerException("Reduce function must not be null.");
		}
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
		if (reducer == null) {
			throw new NullPointerException("GroupReduce function must not be null.");
		}
		return new ReduceGroupOperator<T, R>(this, reducer);
	}
	
	// --------------------------------------------------------------------------------------------
	//  distinct
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Returns a distinct set of a {@link DataSet} using a {@link KeySelector} function.
	 * <p/>
	 * The KeySelector function is called for each element of the DataSet and extracts a single key value on which the
	 * decision is made if two items are distinct or not.
	 *  
	 * @param keyExtractor The KeySelector function which extracts the key values from the DataSet on which the
	 *                     distinction of the DataSet is decided.
	 * @return A DistinctOperator that represents the distinct DataSet.
	 */
	public <K extends Comparable<K>> DistinctOperator<T> distinct(KeySelector<T, K> keyExtractor) {
		return new DistinctOperator<T>(this, new Keys.SelectorFunctionKeys<T, K>(keyExtractor, getType()));
	}
	
	/**
	 * Returns a distinct set of a {@link Tuple} {@link DataSet} using field position keys.
	 * <p/>
	 * The field position keys specify the fields of Tuples on which the decision is made if two Tuples are distinct or
	 * not.
	 * <p/>
	 * Note: Field position keys can only be specified for Tuple DataSets.
	 *
	 * @param fields One or more field positions on which the distinction of the DataSet is decided. 
	 * @return A DistinctOperator that represents the distinct DataSet.
	 */
	public DistinctOperator<T> distinct(int... fields) {
		return new DistinctOperator<T>(this, new Keys.FieldPositionKeys<T>(fields, getType(), true));
	}
	
	/**
	 * Returns a distinct set of a {@link Tuple} {@link DataSet} using all fields of the tuple.
	 * <p/>
	 * Note: This operator can only be applied to Tuple DataSets.
	 * 
	 * @return A DistinctOperator that represents the distinct DataSet.
	 */
	public DistinctOperator<T> distinct() {
		return new DistinctOperator<T>(this, null);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Grouping
	// --------------------------------------------------------------------------------------------

	/**
	 * Groups a {@link DataSet} using a {@link KeySelector} function. 
	 * The KeySelector function is called for each element of the DataSet and extracts a single 
	 *   key value on which the DataSet is grouped. </br>
	 * This method returns an {@link UnsortedGrouping} on which one of the following grouping transformation 
	 *   can be applied. 
	 * <ul>
	 *   <li>{@link UnsortedGrouping#sortGroup(int, eu.stratosphere.api.common.operators.Order)} to get a {@link SortedGrouping}. 
	 *   <li>{@link Grouping#aggregate(Aggregations, int)} to apply an Aggregate transformation.
	 *   <li>{@link Grouping#reduce(ReduceFunction)} to apply a Reduce transformation.
	 *   <li>{@link Grouping#reduceGroup(GroupReduceFunction)} to apply a GroupReduce transformation.
	 * </ul>
	 *  
	 * @param keyExtractor The KeySelector function which extracts the key values from the DataSet on which it is grouped. 
	 * @return An UnsortedGrouping on which a transformation needs to be applied to obtain a transformed DataSet.
	 * 
	 * @see KeySelector
	 * @see Grouping
	 * @see UnsortedGrouping
	 * @see SortedGrouping
	 * @see AggregateOperator
	 * @see ReduceOperator
	 * @see ReduceGroupOperator
	 * @see DataSet
	 */
	public <K extends Comparable<K>> UnsortedGrouping<T> groupBy(KeySelector<T, K> keyExtractor) {
		return new UnsortedGrouping<T>(this, new Keys.SelectorFunctionKeys<T, K>(keyExtractor, getType()));
	}
	
	/**
	 * Groups a {@link Tuple} {@link DataSet} using field position keys.<br/> 
	 * <b>Note: Field position keys only be specified for Tuple DataSets.</b></br>
	 * The field position keys specify the fields of Tuples on which the DataSet is grouped.
	 * This method returns an {@link UnsortedGrouping} on which one of the following grouping transformation 
	 *   can be applied. 
	 * <ul>
	 *   <li>{@link UnsortedGrouping#sortGroup(int, eu.stratosphere.api.common.operators.Order)} to get a {@link SortedGrouping}. 
	 *   <li>{@link Grouping#aggregate(Aggregations, int)} to apply an Aggregate transformation.
	 *   <li>{@link Grouping#reduce(ReduceFunction)} to apply a Reduce transformation.
	 *   <li>{@link Grouping#reduceGroup(GroupReduceFunction)} to apply a GroupReduce transformation.
	 * </ul> 
	 * 
	 * @param fields One or more field positions on which the DataSet will be grouped. 
	 * @return A Grouping on which a transformation needs to be applied to obtain a transformed DataSet.
	 * 
	 * @see Tuple
	 * @see Grouping
	 * @see UnsortedGrouping
	 * @see SortedGrouping
	 * @see AggregateOperator
	 * @see ReduceOperator
	 * @see ReduceGroupOperator
	 * @see DataSet
	 */
	public UnsortedGrouping<T> groupBy(int... fields) {
		return new UnsortedGrouping<T>(this, new Keys.FieldPositionKeys<T>(fields, getType(), false));
	}

	/**
	 * Groups a {@link DataSet} using field expressions. A field expression is either the name of a public field
	 * or a getter method with parentheses of the {@link DataSet}S underlying type. A dot can be used to drill down
	 * into objects, as in {@code "field1.getInnerField2()" }.
	 * This method returns an {@link UnsortedGrouping} on which one of the following grouping transformation
	 *   can be applied.
	 * <ul>
	 *   <li>{@link UnsortedGrouping#sortGroup(int, eu.stratosphere.api.common.operators.Order)} to get a {@link SortedGrouping}.
	 *   <li>{@link Grouping#aggregate(Aggregations, int)} to apply an Aggregate transformation.
	 *   <li>{@link Grouping#reduce(ReduceFunction)} to apply a Reduce transformation.
	 *   <li>{@link Grouping#reduceGroup(GroupReduceFunction)} to apply a GroupReduce transformation.
	 * </ul>
	 *
	 * @param fields One or more field expressions on which the DataSet will be grouped.
	 * @return A Grouping on which a transformation needs to be applied to obtain a transformed DataSet.
	 *
	 * @see Tuple
	 * @see Grouping
	 * @see UnsortedGrouping
	 * @see SortedGrouping
	 * @see AggregateOperator
	 * @see ReduceOperator
	 * @see ReduceGroupOperator
	 * @see DataSet
	 */
	public UnsortedGrouping<T> groupBy(String... fields) {
		return new UnsortedGrouping<T>(this, new Keys.ExpressionKeys<T>(fields, getType()));
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
	 * Continues a Join transformation and defines the {@link Tuple} fields of the second join 
	 * {@link DataSet} that should be used as join keys.<br/>
	 * <b>Note: Fields can only be selected as join keys on Tuple DataSets.</b><br/>
	 * 
	 * The resulting {@link DefaultJoin} wraps each pair of joining elements into a {@link Tuple2}, with 
	 * the element of the first input being the first field of the tuple and the element of the 
	 * second input being the second field of the tuple. 
	 * 
	 * @param fields The indexes of the Tuple fields of the second join DataSet that should be used as keys.
	 * @return A DefaultJoin that represents the joined DataSet.
	 */
	
	/**
	 * Initiates a Cross transformation.<br/>
	 * A Cross transformation combines the elements of two 
	 *   {@link DataSet DataSets} into one DataSet. It builds all pair combinations of elements of 
	 *   both DataSets, i.e., it builds a Cartesian product.
	 * 
	 * <p>
	 * The resulting {@link DefaultCross} wraps each pair of crossed elements into a {@link Tuple2}, with 
	 * the element of the first input being the first field of the tuple and the element of the 
	 * second input being the second field of the tuple.
	 * 
	 * <p>
	 * Call {@link DefaultCross.with(CrossFunction)} to define a {@link CrossFunction} which is called for
	 * each pair of crossed elements. The CrossFunction returns a exactly one element for each pair of input elements.</br>
	 * 
	 * @param other The other DataSet with which this DataSet is crossed. 
	 * @return A DefaultCross that returns a Tuple2 for each pair of crossed elements.
	 * 
	 * @see DefaultCross
	 * @see CrossFunction
	 * @see DataSet
	 * @see Tuple2
	 */
	public <R> CrossOperator.DefaultCross<T, R> cross(DataSet<R> other) {
		return new CrossOperator.DefaultCross<T, R>(this, other);
	}
	
	/**
	 * Initiates a Cross transformation.<br/>
	 * A Cross transformation combines the elements of two 
	 *   {@link DataSet DataSets} into one DataSet. It builds all pair combinations of elements of 
	 *   both DataSets, i.e., it builds a Cartesian product.
	 * This method also gives the hint to the optimizer that the second DataSet to cross is much
	 *   smaller than the first one.
	 *   
	 * <p>
	 * The resulting {@link DefaultCross} wraps each pair of crossed elements into a {@link Tuple2}, with 
	 * the element of the first input being the first field of the tuple and the element of the 
	 * second input being the second field of the tuple.
	 *   
	 * <p>
	 * Call {@link DefaultCross.with(CrossFunction)} to define a {@link CrossFunction} which is called for
	 * each pair of crossed elements. The CrossFunction returns a exactly one element for each pair of input elements.</br>
	 * 
	 * @param other The other DataSet with which this DataSet is crossed. 
	 * @return A DefaultCross that returns a Tuple2 for each pair of crossed elements.
	 * 
	 * @see DefaultCross
	 * @see CrossFunction
	 * @see DataSet
	 * @see Tuple2
	 */
	public <R> CrossOperator.DefaultCross<T, R> crossWithTiny(DataSet<R> other) {
		return new CrossOperator.DefaultCross<T, R>(this, other);
	}
	
	/**
	 * Initiates a Cross transformation.<br/>
	 * A Cross transformation combines the elements of two 
	 *   {@link DataSet DataSets} into one DataSet. It builds all pair combinations of elements of 
	 *   both DataSets, i.e., it builds a Cartesian product.
	 * This method also gives the hint to the optimizer that the second DataSet to cross is much
	 *   larger than the first one.
	 *   
	 * <p>
	 * The resulting {@link DefaultCross} wraps each pair of crossed elements into a {@link Tuple2}, with 
	 * the element of the first input being the first field of the tuple and the element of the 
	 * second input being the second field of the tuple.
	 *   
	 * <p>
	 * Call {@link DefaultCross.with(CrossFunction)} to define a {@link CrossFunction} which is called for
	 * each pair of crossed elements. The CrossFunction returns a exactly one element for each pair of input elements.</br>
	 * 
	 * @param other The other DataSet with which this DataSet is crossed. 
	 * @return A DefaultCross that returns a Tuple2 for each pair of crossed elements.
	 * 
	 * @see DefaultCross
	 * @see CrossFunction
	 * @see DataSet
	 * @see Tuple2
	 */
	public <R> CrossOperator.DefaultCross<T, R> crossWithHuge(DataSet<R> other) {
		return new CrossOperator.DefaultCross<T, R>(this, other);
	}

	// --------------------------------------------------------------------------------------------
	//  Iterations
	// --------------------------------------------------------------------------------------------

	/**
	 * Initiates an iterative part of the program that executes multiple times and feeds back data sets.
	 * The iterative part needs to be closed by calling {@link IterativeDataSet#closeWith(DataSet)}. The data set
	 * given to the {@code closeWith(DataSet)} method is the data set that will be fed back and used as the input
	 * to the next iteration. The return value of the {@code closeWith(DataSet)} method is the resulting
	 * data set after the iteration has terminated.
	 * <p>
	 * An example of an iterative computation is as follows:
	 *
	 * <pre>
	 * {@code
	 * DataSet<Double> input = ...;
	 * 
	 * DataSet<Double> startOfIteration = input.iterate(10);
	 * DataSet<Double> toBeFedBack = startOfIteration
	 *                               .map(new MyMapper())
	 *                               .groupBy(...).reduceGroup(new MyReducer());
	 * DataSet<Double> result = startOfIteration.closeWith(toBeFedBack);
	 * }
	 * </pre>
	 * <p>
	 * The iteration has a maximum number of times that it executes. A dynamic termination can be realized by using a
	 * termination criterion (see {@link IterativeDataSet#closeWith(DataSet, DataSet)}).
	 * 
	 * @param maxIterations The maximum number of times that the iteration is executed.
	 * @return An IterativeDataSet that marks the start of the iterative part and needs to be closed by
	 *         {@link IterativeDataSet#closeWith(DataSet)}.
	 * 
	 * @see eu.stratosphere.api.java.IterativeDataSet
	 */
	public IterativeDataSet<T> iterate(int maxIterations) {
		return new IterativeDataSet<T>(getExecutionEnvironment(), getType(), this, maxIterations);
	}
	
	/**
	 * Initiates a delta iteration. A delta iteration is similar to a regular iteration (as started by {@link #iterate(int)},
	 * but maintains state across the individual iteration steps. The Solution set, which represents the current state
	 * at the beginning of each iteration can be obtained via {@link DeltaIteration#getSolutionSet()} ()}.
	 * It can be be accessed by joining (or CoGrouping) with it. The DataSet that represents the workset of an iteration
	 * can be obtained via {@link DeltaIteration#getWorkset()}.
	 * The solution set is updated by producing a delta for it, which is merged into the solution set at the end of each
	 * iteration step.
	 * <p>
	 * The delta iteration must be closed by calling {@link DeltaIteration#closeWith(DataSet, DataSet)}. The two
	 * parameters are the delta for the solution set and the new workset (the data set that will be fed back).
	 * The return value of the {@code closeWith(DataSet, DataSet)} method is the resulting
	 * data set after the iteration has terminated. Delta iterations terminate when the feed back data set
	 * (the workset) is empty. In addition, a maximum number of steps is given as a fall back termination guard.
	 * <p>
	 * Elements in the solution set are uniquely identified by a key. When merging the solution set delta, contained elements
	 * with the same key are replaced.
	 * <p>
	 * <b>NOTE:</b> Delta iterations currently support only tuple valued data types. This restriction
	 * will be removed in the future. The key is specified by the tuple position.
	 * <p>
	 * A code example for a delta iteration is as follows
	 * <pre>
	 * {@code
	 * DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
	 *                                                  initialState.iterateDelta(initialFeedbakSet, 100, 0);
	 * 
	 * DataSet<Tuple2<Long, Long>> delta = iteration.groupBy(0).aggregate(Aggregations.AVG, 1)
	 *                                              .join(iteration.getSolutionSet()).where(0).equalTo(0)
	 *                                              .flatMap(new ProjectAndFilter());
	 *                                              
	 * DataSet<Tuple2<Long, Long>> feedBack = delta.join(someOtherSet).where(...).equalTo(...).with(...);
	 * 
	 * // close the delta iteration (delta and new workset are identical)
	 * DataSet<Tuple2<Long, Long>> result = iteration.closeWith(delta, feedBack);
	 * }
	 * </pre>
	 * 
	 * @param workset The initial version of the data set that is fed back to the next iteration step (the workset).
	 * @param maxIterations The maximum number of iteration steps, as a fall back safeguard.
	 * @param keyPositions The position of the tuple fields that is used as the key of the solution set.
	 * 
	 * @return The DeltaIteration that marks the start of a delta iteration.
	 * 
	 * @see DeltaIteration
	 */
	public <R> DeltaIteration<T, R> iterateDelta(DataSet<R> workset, int maxIterations, int... keyPositions) {
		Keys.FieldPositionKeys<T> keys = new Keys.FieldPositionKeys<T>(keyPositions, getType(), false);
		return new DeltaIteration<T, R>(getExecutionEnvironment(), getType(), this, workset, keys, maxIterations);
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Operators
	// -------------------------------------------------------------------------------------------
	

	/**
	 * Runs a {@link CustomUnaryOperation} on the data set. Custom operations are typically complex
	 * operators that are composed of multiple steps.
	 * 
	 * @param operation The operation to run.
	 * @return The data set produced by the operation.
	 */
	public <X> DataSet<X> runOperation(CustomUnaryOperation<T, X> operation) {
		Validate.notNull(operation, "The custom operator must not be null.");
		operation.setInput(this);
		return operation.createResult();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Union
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a union of this DataSet with an other DataSet. The other DataSet must be of the same data type.
	 * 
	 * @param other The other DataSet which is unioned with the current DataSet.
	 * @return The resulting DataSet.
	 */
	public UnionOperator<T> union(DataSet<T> other){
		return new UnionOperator<T>(this, other);
	}
	
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
	 * @return The DataSink that writes the DataSet.
	 * 
	 * @see TextOutputFormat
	 */
	public DataSink<T> writeAsText(String filePath) {
		return output(new TextOutputFormat<T>(new Path(filePath)));
	}
	
	/**
	 * Writes a DataSet as a text file to the specified location.<br/>
	 * For each element of the DataSet the result of {@link Object#toString()} is written.  
	 * 
	 * @param filePath The path pointing to the location the text file is written to.
	 * @param writeMode Control the behavior for existing files. Options are NO_OVERWRITE and OVERWRITE.
	 * @return The DataSink that writes the DataSet.
	 * 
	 * @see TextOutputFormat
	 */
	public DataSink<T> writeAsText(String filePath, WriteMode writeMode) {
		TextOutputFormat<T> tof = new TextOutputFormat<T>(new Path(filePath));
		tof.setWriteMode(writeMode);
		return output(tof);
	}
	
	/**
	 * Writes a {@link Tuple} DataSet as a CSV file to the specified location.<br/>
	 * <b>Note: Only a Tuple DataSet can written as a CSV file.</b><br/>
	 * For each Tuple field the result of {@link Object#toString()} is written.
	 * Tuple fields are separated by the default field delimiter {@link CsvOutputFormat.DEFAULT_FIELD_DELIMITER}.<br/>
	 * Tuples are are separated by the default line delimiter {@link CsvOutputFormat.DEFAULT_LINE_DELIMITER}.
	 * 
	 * @param filePath The path pointing to the location the CSV file is written to.
	 * @return The DataSink that writes the DataSet.
	 * 
	 * @see Tuple
	 * @see CsvOutputFormat
	 */
	public DataSink<T> writeAsCsv(String filePath) {
		return writeAsCsv(filePath, CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
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
	public DataSink<T> writeAsCsv(String filePath, String rowDelimiter, String fieldDelimiter) {
		return internalWriteAsCsv(new Path(filePath), rowDelimiter, fieldDelimiter, null);
	}

	/**
	 * Writes a {@link Tuple} DataSet as a CSV file to the specified location with the specified field and line delimiters.<br/>
	 * <b>Note: Only a Tuple DataSet can written as a CSV file.</b><br/>
	 * For each Tuple field the result of {@link Object#toString()} is written.
	 * 
	 * @param filePath The path pointing to the location the CSV file is written to.
	 * @param rowDelimiter The row delimiter to separate Tuples.
	 * @param fieldDelimiter The field delimiter to separate Tuple fields.
	 * @param writeMode Control the behavior for existing files. Options are NO_OVERWRITE and OVERWRITE.
	 * 
	 * @see Tuple
	 * @see CsvOutputFormat
	 */
	public DataSink<T> writeAsCsv(String filePath, String rowDelimiter, String fieldDelimiter, WriteMode writeMode) {
		return internalWriteAsCsv(new Path(filePath), rowDelimiter, fieldDelimiter, writeMode);
	}
	
	@SuppressWarnings("unchecked")
	private <X extends Tuple> DataSink<T> internalWriteAsCsv(Path filePath, String rowDelimiter, String fieldDelimiter, WriteMode wm) {
		Validate.isTrue(this.type.isTupleType(), "The writeAsCsv() method can only be used on data sets of tuples.");
		CsvOutputFormat<X> of = new CsvOutputFormat<X>(filePath, rowDelimiter, fieldDelimiter);
		if(wm != null) {
			of.setWriteMode(wm);
		}
		return output((OutputFormat<T>) of);
	}
	
	/**
	 * Writes a DataSet to the standard output stream (stdout).<br/>
	 * For each element of the DataSet the result of {@link Object#toString()} is written.
	 * 
	 *  @return The DataSink that writes the DataSet.
	 */
	public DataSink<T> print() {
		return output(new PrintingOutputFormat<T>(false));
	}
	
	/**
	 * Writes a DataSet to the standard error stream (stderr).<br/>
	 * For each element of the DataSet the result of {@link Object#toString()} is written.
	 * 
	 * @return The DataSink that writes the DataSet.
	 */
	public DataSink<T> printToErr() {
		return output(new PrintingOutputFormat<T>(true));
	}
	
	/**
	 * Writes a DataSet using a {@link FileOutputFormat} to a specified location.
	 * This method adds a data sink to the program.
	 * 
	 * @param outputFormat The FileOutputFormat to write the DataSet.
	 * @param filePath The path to the location where the DataSet is written.
	 * @return The DataSink that writes the DataSet.
	 * 
	 * @see FileOutputFormat
	 */
	public DataSink<T> write(FileOutputFormat<T> outputFormat, String filePath) {
		Validate.notNull(filePath, "File path must not be null.");
		Validate.notNull(outputFormat, "Output format must not be null.");

		outputFormat.setOutputFilePath(new Path(filePath));
		return output(outputFormat);
	}
	
	/**
	 * Writes a DataSet using a {@link FileOutputFormat} to a specified location.
	 * This method adds a data sink to the program.
	 * 
	 * @param outputFormat The FileOutputFormat to write the DataSet.
	 * @param filePath The path to the location where the DataSet is written.
	 * @param writeMode The mode of writing, indicating whether to overwrite existing files.
	 * @return The DataSink that writes the DataSet.
	 * 
	 * @see FileOutputFormat
	 */
	public DataSink<T> write(FileOutputFormat<T> outputFormat, String filePath, WriteMode writeMode) {
		Validate.notNull(filePath, "File path must not be null.");
		Validate.notNull(writeMode, "Write mode must not be null.");
		Validate.notNull(outputFormat, "Output format must not be null.");

		outputFormat.setOutputFilePath(new Path(filePath));
		outputFormat.setWriteMode(writeMode);
		return output(outputFormat);
	}
	
	/**
	 * Emits a DataSet using an {@link OutputFormat}. This method adds a data sink to the program.
	 * Programs may have multiple data sinks. A DataSet may also have multiple consumers (data sinks
	 * or transformations) at the same time.
	 * 
	 * @param outputFormat The OutputFormat to process the DataSet.
	 * @return The DataSink that processes the DataSet.
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
		if (set1.context != set2.context) {
			throw new IllegalArgumentException("The two inputs have different execution contexts.");
		}
	}
}
