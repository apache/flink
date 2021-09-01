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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase.PartitionMethod;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FirstReducer;
import org.apache.flink.api.java.functions.FormattingMapper;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.SelectByMaxFunction;
import org.apache.flink.api.java.functions.SelectByMinFunction;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.CoGroupOperator.CoGroupOperatorSets;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.operators.ProjectOperator.Projection;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A DataSet represents a collection of elements of the same type.
 *
 * <p>A DataSet can be transformed into another DataSet by applying a transformation as for example
 *
 * <ul>
 *   <li>{@link DataSet#map(org.apache.flink.api.common.functions.MapFunction)},
 *   <li>{@link DataSet#reduce(org.apache.flink.api.common.functions.ReduceFunction)},
 *   <li>{@link DataSet#join(DataSet)}, or
 *   <li>{@link DataSet#coGroup(DataSet)}.
 * </ul>
 *
 * @param <T> The type of the DataSet, i.e., the type of the elements of the DataSet.
 */
@Public
public abstract class DataSet<T> {

    protected final ExecutionEnvironment context;

    // NOTE: the type must not be accessed directly, but only via getType()
    private TypeInformation<T> type;

    private boolean typeUsed = false;

    protected DataSet(ExecutionEnvironment context, TypeInformation<T> typeInfo) {
        if (context == null) {
            throw new NullPointerException("context is null");
        }
        if (typeInfo == null) {
            throw new NullPointerException("typeInfo is null");
        }

        this.context = context;
        this.type = typeInfo;
    }

    /**
     * Returns the {@link ExecutionEnvironment} in which this DataSet is registered.
     *
     * @return The ExecutionEnvironment in which this DataSet is registered.
     * @see ExecutionEnvironment
     */
    public ExecutionEnvironment getExecutionEnvironment() {
        return this.context;
    }

    // --------------------------------------------------------------------------------------------
    //  Type Information handling
    // --------------------------------------------------------------------------------------------

    /**
     * Tries to fill in the type information. Type information can be filled in later when the
     * program uses a type hint. This method checks whether the type information has ever been
     * accessed before and does not allow modifications if the type was accessed already. This
     * ensures consistency by making sure different parts of the operation do not assume different
     * type information.
     *
     * @param typeInfo The type information to fill in.
     * @throws IllegalStateException Thrown, if the type information has been accessed before.
     */
    protected void fillInType(TypeInformation<T> typeInfo) {
        if (typeUsed) {
            throw new IllegalStateException(
                    "TypeInformation cannot be filled in for the type after it has been used. "
                            + "Please make sure that the type info hints are the first call after the transformation function, "
                            + "before any access to types or semantic properties, etc.");
        }
        this.type = typeInfo;
    }

    /**
     * Returns the {@link TypeInformation} for the type of this DataSet.
     *
     * @return The TypeInformation for the type of this DataSet.
     * @see TypeInformation
     */
    public TypeInformation<T> getType() {
        if (type instanceof MissingTypeInfo) {
            MissingTypeInfo typeInfo = (MissingTypeInfo) type;
            throw new InvalidTypesException(
                    "The return type of function '"
                            + typeInfo.getFunctionName()
                            + "' could not be determined automatically, due to type erasure. "
                            + "You can give type information hints by using the returns(...) method on the result of "
                            + "the transformation call, or by letting your function implement the 'ResultTypeQueryable' "
                            + "interface.",
                    typeInfo.getTypeException());
        }
        typeUsed = true;
        return this.type;
    }

    public <F> F clean(F f) {
        if (getExecutionEnvironment().getConfig().isClosureCleanerEnabled()) {
            ClosureCleaner.clean(
                    f, getExecutionEnvironment().getConfig().getClosureCleanerLevel(), true);
        } else {
            ClosureCleaner.ensureSerializable(f);
        }
        return f;
    }

    // --------------------------------------------------------------------------------------------
    //  Filter & Transformations
    // --------------------------------------------------------------------------------------------

    /**
     * Applies a Map transformation on this DataSet.
     *
     * <p>The transformation calls a {@link org.apache.flink.api.common.functions.MapFunction} for
     * each element of the DataSet. Each MapFunction call returns exactly one element.
     *
     * @param mapper The MapFunction that is called for each element of the DataSet.
     * @return A MapOperator that represents the transformed DataSet.
     * @see org.apache.flink.api.common.functions.MapFunction
     * @see org.apache.flink.api.common.functions.RichMapFunction
     * @see MapOperator
     */
    public <R> MapOperator<T, R> map(MapFunction<T, R> mapper) {
        if (mapper == null) {
            throw new NullPointerException("Map function must not be null.");
        }

        String callLocation = Utils.getCallLocationName();
        TypeInformation<R> resultType =
                TypeExtractor.getMapReturnTypes(mapper, getType(), callLocation, true);
        return new MapOperator<>(this, resultType, clean(mapper), callLocation);
    }

    /**
     * Applies a Map-style operation to the entire partition of the data. The function is called
     * once per parallel partition of the data, and the entire partition is available through the
     * given Iterator. The number of elements that each instance of the MapPartition function sees
     * is non deterministic and depends on the parallelism of the operation.
     *
     * <p>This function is intended for operations that cannot transform individual elements,
     * requires no grouping of elements. To transform individual elements, the use of {@code map()}
     * and {@code flatMap()} is preferable.
     *
     * @param mapPartition The MapPartitionFunction that is called for the full DataSet.
     * @return A MapPartitionOperator that represents the transformed DataSet.
     * @see MapPartitionFunction
     * @see MapPartitionOperator
     */
    public <R> MapPartitionOperator<T, R> mapPartition(MapPartitionFunction<T, R> mapPartition) {
        if (mapPartition == null) {
            throw new NullPointerException("MapPartition function must not be null.");
        }

        String callLocation = Utils.getCallLocationName();
        TypeInformation<R> resultType =
                TypeExtractor.getMapPartitionReturnTypes(
                        mapPartition, getType(), callLocation, true);
        return new MapPartitionOperator<>(this, resultType, clean(mapPartition), callLocation);
    }

    /**
     * Applies a FlatMap transformation on a {@link DataSet}.
     *
     * <p>The transformation calls a {@link
     * org.apache.flink.api.common.functions.RichFlatMapFunction} for each element of the DataSet.
     * Each FlatMapFunction call can return any number of elements including none.
     *
     * @param flatMapper The FlatMapFunction that is called for each element of the DataSet.
     * @return A FlatMapOperator that represents the transformed DataSet.
     * @see org.apache.flink.api.common.functions.RichFlatMapFunction
     * @see FlatMapOperator
     * @see DataSet
     */
    public <R> FlatMapOperator<T, R> flatMap(FlatMapFunction<T, R> flatMapper) {
        if (flatMapper == null) {
            throw new NullPointerException("FlatMap function must not be null.");
        }

        String callLocation = Utils.getCallLocationName();
        TypeInformation<R> resultType =
                TypeExtractor.getFlatMapReturnTypes(flatMapper, getType(), callLocation, true);
        return new FlatMapOperator<>(this, resultType, clean(flatMapper), callLocation);
    }

    /**
     * Applies a Filter transformation on a {@link DataSet}.
     *
     * <p>The transformation calls a {@link
     * org.apache.flink.api.common.functions.RichFilterFunction} for each element of the DataSet and
     * retains only those element for which the function returns true. Elements for which the
     * function returns false are filtered.
     *
     * @param filter The FilterFunction that is called for each element of the DataSet.
     * @return A FilterOperator that represents the filtered DataSet.
     * @see org.apache.flink.api.common.functions.RichFilterFunction
     * @see FilterOperator
     * @see DataSet
     */
    public FilterOperator<T> filter(FilterFunction<T> filter) {
        if (filter == null) {
            throw new NullPointerException("Filter function must not be null.");
        }
        return new FilterOperator<>(this, clean(filter), Utils.getCallLocationName());
    }

    // --------------------------------------------------------------------------------------------
    //  Projections
    // --------------------------------------------------------------------------------------------

    /**
     * Applies a Project transformation on a {@link Tuple} {@link DataSet}.
     *
     * <p><b>Note: Only Tuple DataSets can be projected using field indexes.</b>
     *
     * <p>The transformation projects each Tuple of the DataSet onto a (sub)set of fields.
     *
     * <p>Additional fields can be added to the projection by calling {@link
     * ProjectOperator#project(int[])}.
     *
     * <p><b>Note: With the current implementation, the Project transformation looses type
     * information.</b>
     *
     * @param fieldIndexes The field indexes of the input tuple that are retained. The order of
     *     fields in the output tuple corresponds to the order of field indexes.
     * @return A ProjectOperator that represents the projected DataSet.
     * @see Tuple
     * @see DataSet
     * @see ProjectOperator
     */
    public <OUT extends Tuple> ProjectOperator<?, OUT> project(int... fieldIndexes) {
        return new Projection<>(this, fieldIndexes).projectTupleX();
    }

    // --------------------------------------------------------------------------------------------
    //  Non-grouped aggregations
    // --------------------------------------------------------------------------------------------

    /**
     * Applies an Aggregate transformation on a non-grouped {@link Tuple} {@link DataSet}.
     *
     * <p><b>Note: Only Tuple DataSets can be aggregated.</b> The transformation applies a built-in
     * {@link Aggregations Aggregation} on a specified field of a Tuple DataSet. Additional
     * aggregation functions can be added to the resulting {@link AggregateOperator} by calling
     * {@link AggregateOperator#and(Aggregations, int)}.
     *
     * @param agg The built-in aggregation function that is computed.
     * @param field The index of the Tuple field on which the aggregation function is applied.
     * @return An AggregateOperator that represents the aggregated DataSet.
     * @see Tuple
     * @see Aggregations
     * @see AggregateOperator
     * @see DataSet
     */
    public AggregateOperator<T> aggregate(Aggregations agg, int field) {
        return new AggregateOperator<>(this, agg, field, Utils.getCallLocationName());
    }

    /**
     * Syntactic sugar for aggregate (SUM, field).
     *
     * @param field The index of the Tuple field on which the aggregation function is applied.
     * @return An AggregateOperator that represents the summed DataSet.
     * @see org.apache.flink.api.java.operators.AggregateOperator
     */
    public AggregateOperator<T> sum(int field) {
        return aggregate(Aggregations.SUM, field);
    }

    /**
     * Syntactic sugar for {@link #aggregate(Aggregations, int)} using {@link Aggregations#MAX} as
     * the aggregation function.
     *
     * <p><strong>Note:</strong> This operation is not to be confused with {@link #maxBy(int...)},
     * which selects one element with maximum value at the specified field positions.
     *
     * @param field The index of the Tuple field on which the aggregation function is applied.
     * @return An AggregateOperator that represents the max'ed DataSet.
     * @see #aggregate(Aggregations, int)
     * @see #maxBy(int...)
     */
    public AggregateOperator<T> max(int field) {
        return aggregate(Aggregations.MAX, field);
    }

    /**
     * Syntactic sugar for {@link #aggregate(Aggregations, int)} using {@link Aggregations#MIN} as
     * the aggregation function.
     *
     * <p><strong>Note:</strong> This operation is not to be confused with {@link #minBy(int...)},
     * which selects one element with the minimum value at the specified field positions.
     *
     * @param field The index of the Tuple field on which the aggregation function is applied.
     * @return An AggregateOperator that represents the min'ed DataSet.
     * @see #aggregate(Aggregations, int)
     * @see #minBy(int...)
     */
    public AggregateOperator<T> min(int field) {
        return aggregate(Aggregations.MIN, field);
    }

    /**
     * Convenience method to get the count (number of elements) of a DataSet.
     *
     * @return A long integer that represents the number of elements in the data set.
     */
    public long count() throws Exception {
        final String id = new AbstractID().toString();

        output(new Utils.CountHelper<T>(id)).name("count()");

        JobExecutionResult res = getExecutionEnvironment().execute();
        return res.<Long>getAccumulatorResult(id);
    }

    /**
     * Convenience method to get the elements of a DataSet as a List. As DataSet can contain a lot
     * of data, this method should be used with caution.
     *
     * @return A List containing the elements of the DataSet
     */
    public List<T> collect() throws Exception {
        final String id = new AbstractID().toString();
        final TypeSerializer<T> serializer =
                getType().createSerializer(getExecutionEnvironment().getConfig());

        this.output(new Utils.CollectHelper<>(id, serializer)).name("collect()");
        JobExecutionResult res = getExecutionEnvironment().execute();

        ArrayList<byte[]> accResult = res.getAccumulatorResult(id);
        if (accResult != null) {
            try {
                return SerializedListAccumulator.deserializeList(accResult, serializer);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Cannot find type class of collected data type.", e);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Serialization error while deserializing collected data", e);
            }
        } else {
            throw new RuntimeException("The call to collect() could not retrieve the DataSet.");
        }
    }

    /**
     * Applies a Reduce transformation on a non-grouped {@link DataSet}.
     *
     * <p>The transformation consecutively calls a {@link
     * org.apache.flink.api.common.functions.RichReduceFunction} until only a single element remains
     * which is the result of the transformation. A ReduceFunction combines two elements into one
     * new element of the same type.
     *
     * @param reducer The ReduceFunction that is applied on the DataSet.
     * @return A ReduceOperator that represents the reduced DataSet.
     * @see org.apache.flink.api.common.functions.RichReduceFunction
     * @see ReduceOperator
     * @see DataSet
     */
    public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
        if (reducer == null) {
            throw new NullPointerException("Reduce function must not be null.");
        }
        return new ReduceOperator<>(this, clean(reducer), Utils.getCallLocationName());
    }

    /**
     * Applies a GroupReduce transformation on a non-grouped {@link DataSet}.
     *
     * <p>The transformation calls a {@link
     * org.apache.flink.api.common.functions.RichGroupReduceFunction} once with the full DataSet.
     * The GroupReduceFunction can iterate over all elements of the DataSet and emit any number of
     * output elements including none.
     *
     * @param reducer The GroupReduceFunction that is applied on the DataSet.
     * @return A GroupReduceOperator that represents the reduced DataSet.
     * @see org.apache.flink.api.common.functions.RichGroupReduceFunction
     * @see org.apache.flink.api.java.operators.GroupReduceOperator
     * @see DataSet
     */
    public <R> GroupReduceOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
        if (reducer == null) {
            throw new NullPointerException("GroupReduce function must not be null.");
        }

        String callLocation = Utils.getCallLocationName();
        TypeInformation<R> resultType =
                TypeExtractor.getGroupReduceReturnTypes(reducer, getType(), callLocation, true);
        return new GroupReduceOperator<>(this, resultType, clean(reducer), callLocation);
    }

    /**
     * Applies a GroupCombineFunction on a non-grouped {@link DataSet}. A CombineFunction is similar
     * to a GroupReduceFunction but does not perform a full data exchange. Instead, the
     * CombineFunction calls the combine method once per partition for combining a group of results.
     * This operator is suitable for combining values into an intermediate format before doing a
     * proper groupReduce where the data is shuffled across the node for further reduction. The
     * GroupReduce operator can also be supplied with a combiner by implementing the RichGroupReduce
     * function. The combine method of the RichGroupReduce function demands input and output type to
     * be the same. The CombineFunction, on the other side, can have an arbitrary output type.
     *
     * @param combiner The GroupCombineFunction that is applied on the DataSet.
     * @return A GroupCombineOperator which represents the combined DataSet.
     */
    public <R> GroupCombineOperator<T, R> combineGroup(GroupCombineFunction<T, R> combiner) {
        if (combiner == null) {
            throw new NullPointerException("GroupCombine function must not be null.");
        }

        String callLocation = Utils.getCallLocationName();
        TypeInformation<R> resultType =
                TypeExtractor.getGroupCombineReturnTypes(combiner, getType(), callLocation, true);
        return new GroupCombineOperator<>(this, resultType, clean(combiner), callLocation);
    }

    /**
     * Selects an element with minimum value.
     *
     * <p>The minimum is computed over the specified fields in lexicographical order.
     *
     * <p><strong>Example 1</strong>: Given a data set with elements <code>[0, 1], [1, 0]</code>,
     * the results will be:
     *
     * <ul>
     *   <li><code>minBy(0)</code>: <code>[0, 1]</code>
     *   <li><code>minBy(1)</code>: <code>[1, 0]</code>
     * </ul>
     *
     * <p><strong>Example 2</strong>: Given a data set with elements <code>[0, 0], [0, 1]</code>,
     * the results will be:
     *
     * <ul>
     *   <li><code>minBy(0, 1)</code>: <code>[0, 0]</code>
     * </ul>
     *
     * <p>If multiple values with minimum value at the specified fields exist, a random one will be
     * picked.
     *
     * <p>Internally, this operation is implemented as a {@link ReduceFunction}.
     *
     * @param fields Field positions to compute the minimum over
     * @return A {@link ReduceOperator} representing the minimum
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ReduceOperator<T> minBy(int... fields) {
        if (!getType().isTupleType() || !(getType() instanceof TupleTypeInfo)) {
            throw new InvalidProgramException("DataSet#minBy(int...) only works on Tuple types.");
        }

        return new ReduceOperator<>(
                this,
                new SelectByMinFunction((TupleTypeInfo) getType(), fields),
                Utils.getCallLocationName());
    }

    /**
     * Selects an element with maximum value.
     *
     * <p>The maximum is computed over the specified fields in lexicographical order.
     *
     * <p><strong>Example 1</strong>: Given a data set with elements <code>[0, 1], [1, 0]</code>,
     * the results will be:
     *
     * <ul>
     *   <li><code>maxBy(0)</code>: <code>[1, 0]</code>
     *   <li><code>maxBy(1)</code>: <code>[0, 1]</code>
     * </ul>
     *
     * <p><strong>Example 2</strong>: Given a data set with elements <code>[0, 0], [0, 1]</code>,
     * the results will be:
     *
     * <ul>
     *   <li><code>maxBy(0, 1)</code>: <code>[0, 1]</code>
     * </ul>
     *
     * <p>If multiple values with maximum value at the specified fields exist, a random one will be
     * picked.
     *
     * <p>Internally, this operation is implemented as a {@link ReduceFunction}.
     *
     * @param fields Field positions to compute the maximum over
     * @return A {@link ReduceOperator} representing the maximum
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ReduceOperator<T> maxBy(int... fields) {
        if (!getType().isTupleType() || !(getType() instanceof TupleTypeInfo)) {
            throw new InvalidProgramException("DataSet#maxBy(int...) only works on Tuple types.");
        }

        return new ReduceOperator<>(
                this,
                new SelectByMaxFunction((TupleTypeInfo) getType(), fields),
                Utils.getCallLocationName());
    }

    /**
     * Returns a new set containing the first n elements in this {@link DataSet}.
     *
     * @param n The desired number of elements.
     * @return A ReduceGroupOperator that represents the DataSet containing the elements.
     */
    public GroupReduceOperator<T, T> first(int n) {
        if (n < 1) {
            throw new InvalidProgramException("Parameter n of first(n) must be at least 1.");
        }

        return reduceGroup(new FirstReducer<T>(n));
    }

    // --------------------------------------------------------------------------------------------
    //  distinct
    // --------------------------------------------------------------------------------------------

    /**
     * Returns a distinct set of a {@link DataSet} using a {@link KeySelector} function.
     *
     * <p>The KeySelector function is called for each element of the DataSet and extracts a single
     * key value on which the decision is made if two items are distinct or not.
     *
     * @param keyExtractor The KeySelector function which extracts the key values from the DataSet
     *     on which the distinction of the DataSet is decided.
     * @return A DistinctOperator that represents the distinct DataSet.
     */
    public <K> DistinctOperator<T> distinct(KeySelector<T, K> keyExtractor) {
        TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keyExtractor, getType());
        return new DistinctOperator<>(
                this,
                new Keys.SelectorFunctionKeys<>(keyExtractor, getType(), keyType),
                Utils.getCallLocationName());
    }

    /**
     * Returns a distinct set of a {@link Tuple} {@link DataSet} using field position keys.
     *
     * <p>The field position keys specify the fields of Tuples on which the decision is made if two
     * Tuples are distinct or not.
     *
     * <p>Note: Field position keys can only be specified for Tuple DataSets.
     *
     * @param fields One or more field positions on which the distinction of the DataSet is decided.
     * @return A DistinctOperator that represents the distinct DataSet.
     */
    public DistinctOperator<T> distinct(int... fields) {
        return new DistinctOperator<>(
                this, new Keys.ExpressionKeys<>(fields, getType()), Utils.getCallLocationName());
    }

    /**
     * Returns a distinct set of a {@link DataSet} using expression keys.
     *
     * <p>The field expression keys specify the fields of a {@link
     * org.apache.flink.api.common.typeutils.CompositeType} (e.g., Tuple or Pojo type) on which the
     * decision is made if two elements are distinct or not. In case of a {@link
     * org.apache.flink.api.common.typeinfo.AtomicType}, only the wildcard expression ("*") is
     * valid.
     *
     * @param fields One or more field expressions on which the distinction of the DataSet is
     *     decided.
     * @return A DistinctOperator that represents the distinct DataSet.
     */
    public DistinctOperator<T> distinct(String... fields) {
        return new DistinctOperator<>(
                this, new Keys.ExpressionKeys<>(fields, getType()), Utils.getCallLocationName());
    }

    /**
     * Returns a distinct set of a {@link DataSet}.
     *
     * <p>If the input is a {@link org.apache.flink.api.common.typeutils.CompositeType} (Tuple or
     * Pojo type), distinct is performed on all fields and each field must be a key type
     *
     * @return A DistinctOperator that represents the distinct DataSet.
     */
    public DistinctOperator<T> distinct() {
        return new DistinctOperator<>(this, null, Utils.getCallLocationName());
    }

    // --------------------------------------------------------------------------------------------
    //  Grouping
    // --------------------------------------------------------------------------------------------

    /**
     * Groups a {@link DataSet} using a {@link KeySelector} function. The KeySelector function is
     * called for each element of the DataSet and extracts a single key value on which the DataSet
     * is grouped.
     *
     * <p>This method returns an {@link UnsortedGrouping} on which one of the following grouping
     * transformation can be applied.
     *
     * <ul>
     *   <li>{@link UnsortedGrouping#sortGroup(int, org.apache.flink.api.common.operators.Order)} to
     *       get a {@link SortedGrouping}.
     *   <li>{@link UnsortedGrouping#aggregate(Aggregations, int)} to apply an Aggregate
     *       transformation.
     *   <li>{@link UnsortedGrouping#reduce(org.apache.flink.api.common.functions.ReduceFunction)}
     *       to apply a Reduce transformation.
     *   <li>{@link
     *       UnsortedGrouping#reduceGroup(org.apache.flink.api.common.functions.GroupReduceFunction)}
     *       to apply a GroupReduce transformation.
     * </ul>
     *
     * @param keyExtractor The {@link KeySelector} function which extracts the key values from the
     *     DataSet on which it is grouped.
     * @return An {@link UnsortedGrouping} on which a transformation needs to be applied to obtain a
     *     transformed DataSet.
     * @see KeySelector
     * @see UnsortedGrouping
     * @see AggregateOperator
     * @see ReduceOperator
     * @see org.apache.flink.api.java.operators.GroupReduceOperator
     * @see DataSet
     */
    public <K> UnsortedGrouping<T> groupBy(KeySelector<T, K> keyExtractor) {
        TypeInformation<K> keyType = TypeExtractor.getKeySelectorTypes(keyExtractor, getType());
        return new UnsortedGrouping<>(
                this, new Keys.SelectorFunctionKeys<>(clean(keyExtractor), getType(), keyType));
    }

    /**
     * Groups a {@link Tuple} {@link DataSet} using field position keys.
     *
     * <p><b>Note: Field position keys only be specified for Tuple DataSets.</b>
     *
     * <p>The field position keys specify the fields of Tuples on which the DataSet is grouped. This
     * method returns an {@link UnsortedGrouping} on which one of the following grouping
     * transformation can be applied.
     *
     * <ul>
     *   <li>{@link UnsortedGrouping#sortGroup(int, org.apache.flink.api.common.operators.Order)} to
     *       get a {@link SortedGrouping}.
     *   <li>{@link UnsortedGrouping#aggregate(Aggregations, int)} to apply an Aggregate
     *       transformation.
     *   <li>{@link UnsortedGrouping#reduce(org.apache.flink.api.common.functions.ReduceFunction)}
     *       to apply a Reduce transformation.
     *   <li>{@link
     *       UnsortedGrouping#reduceGroup(org.apache.flink.api.common.functions.GroupReduceFunction)}
     *       to apply a GroupReduce transformation.
     * </ul>
     *
     * @param fields One or more field positions on which the DataSet will be grouped.
     * @return An {@link UnsortedGrouping} on which a transformation needs to be applied to obtain a
     *     transformed DataSet.
     * @see Tuple
     * @see UnsortedGrouping
     * @see AggregateOperator
     * @see ReduceOperator
     * @see org.apache.flink.api.java.operators.GroupReduceOperator
     * @see DataSet
     */
    public UnsortedGrouping<T> groupBy(int... fields) {
        return new UnsortedGrouping<>(this, new Keys.ExpressionKeys<>(fields, getType()));
    }

    /**
     * Groups a {@link DataSet} using field expressions. A field expression is either the name of a
     * public field or a getter method with parentheses of the {@link DataSet}S underlying type. A
     * dot can be used to drill down into objects, as in {@code "field1.getInnerField2()" }. This
     * method returns an {@link UnsortedGrouping} on which one of the following grouping
     * transformation can be applied.
     *
     * <ul>
     *   <li>{@link UnsortedGrouping#sortGroup(int, org.apache.flink.api.common.operators.Order)} to
     *       get a {@link SortedGrouping}.
     *   <li>{@link UnsortedGrouping#aggregate(Aggregations, int)} to apply an Aggregate
     *       transformation.
     *   <li>{@link UnsortedGrouping#reduce(org.apache.flink.api.common.functions.ReduceFunction)}
     *       to apply a Reduce transformation.
     *   <li>{@link
     *       UnsortedGrouping#reduceGroup(org.apache.flink.api.common.functions.GroupReduceFunction)}
     *       to apply a GroupReduce transformation.
     * </ul>
     *
     * @param fields One or more field expressions on which the DataSet will be grouped.
     * @return An {@link UnsortedGrouping} on which a transformation needs to be applied to obtain a
     *     transformed DataSet.
     * @see Tuple
     * @see UnsortedGrouping
     * @see AggregateOperator
     * @see ReduceOperator
     * @see org.apache.flink.api.java.operators.GroupReduceOperator
     * @see DataSet
     */
    public UnsortedGrouping<T> groupBy(String... fields) {
        return new UnsortedGrouping<>(this, new Keys.ExpressionKeys<>(fields, getType()));
    }

    // --------------------------------------------------------------------------------------------
    //  Joining
    // --------------------------------------------------------------------------------------------

    /**
     * Initiates a Join transformation.
     *
     * <p>A Join transformation joins the elements of two {@link DataSet DataSets} on key equality
     * and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>This method returns a {@link JoinOperatorSets} on which one of the {@code where} methods
     * can be called to define the join key of the first joining (i.e., this) DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @return A JoinOperatorSets to continue the definition of the Join transformation.
     * @see JoinOperatorSets
     * @see DataSet
     */
    public <R> JoinOperatorSets<T, R> join(DataSet<R> other) {
        return new JoinOperatorSets<>(this, other);
    }

    /**
     * Initiates a Join transformation.
     *
     * <p>A Join transformation joins the elements of two {@link DataSet DataSets} on key equality
     * and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>This method returns a {@link JoinOperatorSets} on which one of the {@code where} methods
     * can be called to define the join key of the first joining (i.e., this) DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @param strategy The strategy that should be used execute the join. If {@code null} is given,
     *     then the optimizer will pick the join strategy.
     * @return A JoinOperatorSets to continue the definition of the Join transformation.
     * @see JoinOperatorSets
     * @see DataSet
     */
    public <R> JoinOperatorSets<T, R> join(DataSet<R> other, JoinHint strategy) {
        return new JoinOperatorSets<>(this, other, strategy);
    }

    /**
     * Initiates a Join transformation.
     *
     * <p>A Join transformation joins the elements of two {@link DataSet DataSets} on key equality
     * and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>This method also gives the hint to the optimizer that the second DataSet to join is much
     * smaller than the first one.
     *
     * <p>This method returns a {@link JoinOperatorSets} on which {@link
     * JoinOperatorSets#where(String...)} needs to be called to define the join key of the first
     * joining (i.e., this) DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @return A JoinOperatorSets to continue the definition of the Join transformation.
     * @see JoinOperatorSets
     * @see DataSet
     */
    public <R> JoinOperatorSets<T, R> joinWithTiny(DataSet<R> other) {
        return new JoinOperatorSets<>(this, other, JoinHint.BROADCAST_HASH_SECOND);
    }

    /**
     * Initiates a Join transformation.
     *
     * <p>A Join transformation joins the elements of two {@link DataSet DataSets} on key equality
     * and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>This method also gives the hint to the optimizer that the second DataSet to join is much
     * larger than the first one.
     *
     * <p>This method returns a {@link JoinOperatorSets} on which one of the {@code where} methods
     * can be called to define the join key of the first joining (i.e., this) DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @return A JoinOperatorSet to continue the definition of the Join transformation.
     * @see JoinOperatorSets
     * @see DataSet
     */
    public <R> JoinOperatorSets<T, R> joinWithHuge(DataSet<R> other) {
        return new JoinOperatorSets<>(this, other, JoinHint.BROADCAST_HASH_FIRST);
    }

    /**
     * Initiates a Left Outer Join transformation.
     *
     * <p>An Outer Join transformation joins two elements of two {@link DataSet DataSets} on key
     * equality and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>Elements of the <b>left</b> DataSet (i.e. {@code this}) that do not have a matching
     * element on the other side are joined with {@code null} and emitted to the resulting DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @return A JoinOperatorSet to continue the definition of the Join transformation.
     * @see org.apache.flink.api.java.operators.join.JoinOperatorSetsBase
     * @see DataSet
     */
    public <R> JoinOperatorSetsBase<T, R> leftOuterJoin(DataSet<R> other) {
        return new JoinOperatorSetsBase<>(
                this, other, JoinHint.OPTIMIZER_CHOOSES, JoinType.LEFT_OUTER);
    }

    /**
     * Initiates a Left Outer Join transformation.
     *
     * <p>An Outer Join transformation joins two elements of two {@link DataSet DataSets} on key
     * equality and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>Elements of the <b>left</b> DataSet (i.e. {@code this}) that do not have a matching
     * element on the other side are joined with {@code null} and emitted to the resulting DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @param strategy The strategy that should be used execute the join. If {@code null} is given,
     *     then the optimizer will pick the join strategy.
     * @return A JoinOperatorSet to continue the definition of the Join transformation.
     * @see org.apache.flink.api.java.operators.join.JoinOperatorSetsBase
     * @see DataSet
     */
    public <R> JoinOperatorSetsBase<T, R> leftOuterJoin(DataSet<R> other, JoinHint strategy) {
        switch (strategy) {
            case OPTIMIZER_CHOOSES:
            case REPARTITION_SORT_MERGE:
            case REPARTITION_HASH_FIRST:
            case REPARTITION_HASH_SECOND:
            case BROADCAST_HASH_SECOND:
                return new JoinOperatorSetsBase<>(this, other, strategy, JoinType.LEFT_OUTER);
            default:
                throw new InvalidProgramException(
                        "Invalid JoinHint for LeftOuterJoin: " + strategy);
        }
    }

    /**
     * Initiates a Right Outer Join transformation.
     *
     * <p>An Outer Join transformation joins two elements of two {@link DataSet DataSets} on key
     * equality and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>Elements of the <b>right</b> DataSet (i.e. {@code other}) that do not have a matching
     * element on {@code this} side are joined with {@code null} and emitted to the resulting
     * DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @return A JoinOperatorSet to continue the definition of the Join transformation.
     * @see org.apache.flink.api.java.operators.join.JoinOperatorSetsBase
     * @see DataSet
     */
    public <R> JoinOperatorSetsBase<T, R> rightOuterJoin(DataSet<R> other) {
        return new JoinOperatorSetsBase<>(
                this, other, JoinHint.OPTIMIZER_CHOOSES, JoinType.RIGHT_OUTER);
    }

    /**
     * Initiates a Right Outer Join transformation.
     *
     * <p>An Outer Join transformation joins two elements of two {@link DataSet DataSets} on key
     * equality and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>Elements of the <b>right</b> DataSet (i.e. {@code other}) that do not have a matching
     * element on {@code this} side are joined with {@code null} and emitted to the resulting
     * DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @param strategy The strategy that should be used execute the join. If {@code null} is given,
     *     then the optimizer will pick the join strategy.
     * @return A JoinOperatorSet to continue the definition of the Join transformation.
     * @see org.apache.flink.api.java.operators.join.JoinOperatorSetsBase
     * @see DataSet
     */
    public <R> JoinOperatorSetsBase<T, R> rightOuterJoin(DataSet<R> other, JoinHint strategy) {
        switch (strategy) {
            case OPTIMIZER_CHOOSES:
            case REPARTITION_SORT_MERGE:
            case REPARTITION_HASH_FIRST:
            case REPARTITION_HASH_SECOND:
            case BROADCAST_HASH_FIRST:
                return new JoinOperatorSetsBase<>(this, other, strategy, JoinType.RIGHT_OUTER);
            default:
                throw new InvalidProgramException(
                        "Invalid JoinHint for RightOuterJoin: " + strategy);
        }
    }

    /**
     * Initiates a Full Outer Join transformation.
     *
     * <p>An Outer Join transformation joins two elements of two {@link DataSet DataSets} on key
     * equality and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>Elements of <b>both</b> DataSets that do not have a matching element on the opposing side
     * are joined with {@code null} and emitted to the resulting DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @return A JoinOperatorSet to continue the definition of the Join transformation.
     * @see org.apache.flink.api.java.operators.join.JoinOperatorSetsBase
     * @see DataSet
     */
    public <R> JoinOperatorSetsBase<T, R> fullOuterJoin(DataSet<R> other) {
        return new JoinOperatorSetsBase<>(
                this, other, JoinHint.OPTIMIZER_CHOOSES, JoinType.FULL_OUTER);
    }

    /**
     * Initiates a Full Outer Join transformation.
     *
     * <p>An Outer Join transformation joins two elements of two {@link DataSet DataSets} on key
     * equality and provides multiple ways to combine joining elements into one DataSet.
     *
     * <p>Elements of <b>both</b> DataSets that do not have a matching element on the opposing side
     * are joined with {@code null} and emitted to the resulting DataSet.
     *
     * @param other The other DataSet with which this DataSet is joined.
     * @param strategy The strategy that should be used execute the join. If {@code null} is given,
     *     then the optimizer will pick the join strategy.
     * @return A JoinOperatorSet to continue the definition of the Join transformation.
     * @see org.apache.flink.api.java.operators.join.JoinOperatorSetsBase
     * @see DataSet
     */
    public <R> JoinOperatorSetsBase<T, R> fullOuterJoin(DataSet<R> other, JoinHint strategy) {
        switch (strategy) {
            case OPTIMIZER_CHOOSES:
            case REPARTITION_SORT_MERGE:
            case REPARTITION_HASH_FIRST:
            case REPARTITION_HASH_SECOND:
                return new JoinOperatorSetsBase<>(this, other, strategy, JoinType.FULL_OUTER);
            default:
                throw new InvalidProgramException(
                        "Invalid JoinHint for FullOuterJoin: " + strategy);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Co-Grouping
    // --------------------------------------------------------------------------------------------

    /**
     * Initiates a CoGroup transformation.
     *
     * <p>A CoGroup transformation combines the elements of two {@link DataSet DataSets} into one
     * DataSet. It groups each DataSet individually on a key and gives groups of both DataSets with
     * equal keys together into a {@link org.apache.flink.api.common.functions.RichCoGroupFunction}.
     * If a DataSet has a group with no matching key in the other DataSet, the CoGroupFunction is
     * called with an empty group for the non-existing group.
     *
     * <p>The CoGroupFunction can iterate over the elements of both groups and return any number of
     * elements including none.
     *
     * <p>This method returns a {@link CoGroupOperatorSets} on which one of the {@code where}
     * methods can be called to define the join key of the first joining (i.e., this) DataSet.
     *
     * @param other The other DataSet of the CoGroup transformation.
     * @return A CoGroupOperatorSets to continue the definition of the CoGroup transformation.
     * @see CoGroupOperatorSets
     * @see CoGroupOperator
     * @see DataSet
     */
    public <R> CoGroupOperator.CoGroupOperatorSets<T, R> coGroup(DataSet<R> other) {
        return new CoGroupOperator.CoGroupOperatorSets<>(this, other);
    }

    // --------------------------------------------------------------------------------------------
    //  Cross
    // --------------------------------------------------------------------------------------------

    /**
     * Continues a Join transformation and defines the {@link Tuple} fields of the second join
     * {@link DataSet} that should be used as join keys.
     *
     * <p><b>Note: Fields can only be selected as join keys on Tuple DataSets.</b>
     *
     * <p>The resulting {@link DefaultJoin} wraps each pair of joining elements into a {@link
     * Tuple2}, with the element of the first input being the first field of the tuple and the
     * element of the second input being the second field of the tuple.
     *
     * @param fields The indexes of the Tuple fields of the second join DataSet that should be used
     *     as keys.
     * @return A DefaultJoin that represents the joined DataSet.
     */

    /**
     * Initiates a Cross transformation.
     *
     * <p>A Cross transformation combines the elements of two {@link DataSet DataSets} into one
     * DataSet. It builds all pair combinations of elements of both DataSets, i.e., it builds a
     * Cartesian product.
     *
     * <p>The resulting {@link org.apache.flink.api.java.operators.CrossOperator.DefaultCross} wraps
     * each pair of crossed elements into a {@link Tuple2}, with the element of the first input
     * being the first field of the tuple and the element of the second input being the second field
     * of the tuple.
     *
     * <p>Call {@link
     * org.apache.flink.api.java.operators.CrossOperator.DefaultCross#with(org.apache.flink.api.common.functions.CrossFunction)}
     * to define a {@link org.apache.flink.api.common.functions.CrossFunction} which is called for
     * each pair of crossed elements. The CrossFunction returns a exactly one element for each pair
     * of input elements.
     *
     * @param other The other DataSet with which this DataSet is crossed.
     * @return A DefaultCross that returns a Tuple2 for each pair of crossed elements.
     * @see org.apache.flink.api.java.operators.CrossOperator.DefaultCross
     * @see org.apache.flink.api.common.functions.CrossFunction
     * @see DataSet
     * @see Tuple2
     */
    public <R> CrossOperator.DefaultCross<T, R> cross(DataSet<R> other) {
        return new CrossOperator.DefaultCross<>(
                this, other, CrossHint.OPTIMIZER_CHOOSES, Utils.getCallLocationName());
    }

    /**
     * Initiates a Cross transformation.
     *
     * <p>A Cross transformation combines the elements of two {@link DataSet DataSets} into one
     * DataSet. It builds all pair combinations of elements of both DataSets, i.e., it builds a
     * Cartesian product. This method also gives the hint to the optimizer that the second DataSet
     * to cross is much smaller than the first one.
     *
     * <p>The resulting {@link org.apache.flink.api.java.operators.CrossOperator.DefaultCross} wraps
     * each pair of crossed elements into a {@link Tuple2}, with the element of the first input
     * being the first field of the tuple and the element of the second input being the second field
     * of the tuple.
     *
     * <p>Call {@link
     * org.apache.flink.api.java.operators.CrossOperator.DefaultCross#with(org.apache.flink.api.common.functions.CrossFunction)}
     * to define a {@link org.apache.flink.api.common.functions.CrossFunction} which is called for
     * each pair of crossed elements. The CrossFunction returns a exactly one element for each pair
     * of input elements.
     *
     * @param other The other DataSet with which this DataSet is crossed.
     * @return A DefaultCross that returns a Tuple2 for each pair of crossed elements.
     * @see org.apache.flink.api.java.operators.CrossOperator.DefaultCross
     * @see org.apache.flink.api.common.functions.CrossFunction
     * @see DataSet
     * @see Tuple2
     */
    public <R> CrossOperator.DefaultCross<T, R> crossWithTiny(DataSet<R> other) {
        return new CrossOperator.DefaultCross<>(
                this, other, CrossHint.SECOND_IS_SMALL, Utils.getCallLocationName());
    }

    /**
     * Initiates a Cross transformation.
     *
     * <p>A Cross transformation combines the elements of two {@link DataSet DataSets} into one
     * DataSet. It builds all pair combinations of elements of both DataSets, i.e., it builds a
     * Cartesian product. This method also gives the hint to the optimizer that the second DataSet
     * to cross is much larger than the first one.
     *
     * <p>The resulting {@link org.apache.flink.api.java.operators.CrossOperator.DefaultCross} wraps
     * each pair of crossed elements into a {@link Tuple2}, with the element of the first input
     * being the first field of the tuple and the element of the second input being the second field
     * of the tuple.
     *
     * <p>Call {@link
     * org.apache.flink.api.java.operators.CrossOperator.DefaultCross#with(org.apache.flink.api.common.functions.CrossFunction)}
     * to define a {@link org.apache.flink.api.common.functions.CrossFunction} which is called for
     * each pair of crossed elements. The CrossFunction returns a exactly one element for each pair
     * of input elements.
     *
     * @param other The other DataSet with which this DataSet is crossed.
     * @return A DefaultCross that returns a Tuple2 for each pair of crossed elements.
     * @see org.apache.flink.api.java.operators.CrossOperator.DefaultCross
     * @see org.apache.flink.api.common.functions.CrossFunction
     * @see DataSet
     * @see Tuple2
     */
    public <R> CrossOperator.DefaultCross<T, R> crossWithHuge(DataSet<R> other) {
        return new CrossOperator.DefaultCross<>(
                this, other, CrossHint.FIRST_IS_SMALL, Utils.getCallLocationName());
    }

    // --------------------------------------------------------------------------------------------
    //  Iterations
    // --------------------------------------------------------------------------------------------

    /**
     * Initiates an iterative part of the program that executes multiple times and feeds back data
     * sets. The iterative part needs to be closed by calling {@link
     * org.apache.flink.api.java.operators.IterativeDataSet#closeWith(DataSet)}. The data set given
     * to the {@code closeWith(DataSet)} method is the data set that will be fed back and used as
     * the input to the next iteration. The return value of the {@code closeWith(DataSet)} method is
     * the resulting data set after the iteration has terminated.
     *
     * <p>An example of an iterative computation is as follows:
     *
     * <pre>{@code
     * DataSet<Double> input = ...;
     *
     * DataSet<Double> startOfIteration = input.iterate(10);
     * DataSet<Double> toBeFedBack = startOfIteration
     *                               .map(new MyMapper())
     *                               .groupBy(...).reduceGroup(new MyReducer());
     * DataSet<Double> result = startOfIteration.closeWith(toBeFedBack);
     * }</pre>
     *
     * <p>The iteration has a maximum number of times that it executes. A dynamic termination can be
     * realized by using a termination criterion (see {@link
     * org.apache.flink.api.java.operators.IterativeDataSet#closeWith(DataSet, DataSet)}).
     *
     * @param maxIterations The maximum number of times that the iteration is executed.
     * @return An IterativeDataSet that marks the start of the iterative part and needs to be closed
     *     by {@link org.apache.flink.api.java.operators.IterativeDataSet#closeWith(DataSet)}.
     * @see org.apache.flink.api.java.operators.IterativeDataSet
     */
    public IterativeDataSet<T> iterate(int maxIterations) {
        return new IterativeDataSet<>(getExecutionEnvironment(), getType(), this, maxIterations);
    }

    /**
     * Initiates a delta iteration. A delta iteration is similar to a regular iteration (as started
     * by {@link #iterate(int)}, but maintains state across the individual iteration steps. The
     * Solution set, which represents the current state at the beginning of each iteration can be
     * obtained via {@link org.apache.flink.api.java.operators.DeltaIteration#getSolutionSet()} ()}.
     * It can be be accessed by joining (or CoGrouping) with it. The DataSet that represents the
     * workset of an iteration can be obtained via {@link
     * org.apache.flink.api.java.operators.DeltaIteration#getWorkset()}. The solution set is updated
     * by producing a delta for it, which is merged into the solution set at the end of each
     * iteration step.
     *
     * <p>The delta iteration must be closed by calling {@link
     * org.apache.flink.api.java.operators.DeltaIteration#closeWith(DataSet, DataSet)}. The two
     * parameters are the delta for the solution set and the new workset (the data set that will be
     * fed back). The return value of the {@code closeWith(DataSet, DataSet)} method is the
     * resulting data set after the iteration has terminated. Delta iterations terminate when the
     * feed back data set (the workset) is empty. In addition, a maximum number of steps is given as
     * a fall back termination guard.
     *
     * <p>Elements in the solution set are uniquely identified by a key. When merging the solution
     * set delta, contained elements with the same key are replaced.
     *
     * <p><b>NOTE:</b> Delta iterations currently support only tuple valued data types. This
     * restriction will be removed in the future. The key is specified by the tuple position.
     *
     * <p>A code example for a delta iteration is as follows
     *
     * <pre>{@code
     * DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
     *                                                  initialState.iterateDelta(initialFeedbackSet, 100, 0);
     *
     * DataSet<Tuple2<Long, Long>> delta = iteration.groupBy(0).aggregate(Aggregations.AVG, 1)
     *                                              .join(iteration.getSolutionSet()).where(0).equalTo(0)
     *                                              .flatMap(new ProjectAndFilter());
     *
     * DataSet<Tuple2<Long, Long>> feedBack = delta.join(someOtherSet).where(...).equalTo(...).with(...);
     *
     * // close the delta iteration (delta and new workset are identical)
     * DataSet<Tuple2<Long, Long>> result = iteration.closeWith(delta, feedBack);
     * }</pre>
     *
     * @param workset The initial version of the data set that is fed back to the next iteration
     *     step (the workset).
     * @param maxIterations The maximum number of iteration steps, as a fall back safeguard.
     * @param keyPositions The position of the tuple fields that is used as the key of the solution
     *     set.
     * @return The DeltaIteration that marks the start of a delta iteration.
     * @see org.apache.flink.api.java.operators.DeltaIteration
     */
    public <R> DeltaIteration<T, R> iterateDelta(
            DataSet<R> workset, int maxIterations, int... keyPositions) {
        Preconditions.checkNotNull(workset);
        Preconditions.checkNotNull(keyPositions);

        Keys.ExpressionKeys<T> keys = new Keys.ExpressionKeys<>(keyPositions, getType());
        return new DeltaIteration<>(
                getExecutionEnvironment(), getType(), this, workset, keys, maxIterations);
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
        Preconditions.checkNotNull(operation, "The custom operator must not be null.");
        operation.setInput(this);
        return operation.createResult();
    }

    // --------------------------------------------------------------------------------------------
    //  Union
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a union of this DataSet with an other DataSet. The other DataSet must be of the same
     * data type.
     *
     * @param other The other DataSet which is unioned with the current DataSet.
     * @return The resulting DataSet.
     */
    public UnionOperator<T> union(DataSet<T> other) {
        return new UnionOperator<>(this, other, Utils.getCallLocationName());
    }

    // --------------------------------------------------------------------------------------------
    //  Partitioning
    // --------------------------------------------------------------------------------------------

    /**
     * Hash-partitions a DataSet on the specified key fields.
     *
     * <p><b>Important:</b>This operation shuffles the whole DataSet over the network and can take
     * significant amount of time.
     *
     * @param fields The field indexes on which the DataSet is hash-partitioned.
     * @return The partitioned DataSet.
     */
    public PartitionOperator<T> partitionByHash(int... fields) {
        return new PartitionOperator<>(
                this,
                PartitionMethod.HASH,
                new Keys.ExpressionKeys<>(fields, getType()),
                Utils.getCallLocationName());
    }

    /**
     * Hash-partitions a DataSet on the specified key fields.
     *
     * <p><b>Important:</b>This operation shuffles the whole DataSet over the network and can take
     * significant amount of time.
     *
     * @param fields The field expressions on which the DataSet is hash-partitioned.
     * @return The partitioned DataSet.
     */
    public PartitionOperator<T> partitionByHash(String... fields) {
        return new PartitionOperator<>(
                this,
                PartitionMethod.HASH,
                new Keys.ExpressionKeys<>(fields, getType()),
                Utils.getCallLocationName());
    }

    /**
     * Partitions a DataSet using the specified KeySelector.
     *
     * <p><b>Important:</b>This operation shuffles the whole DataSet over the network and can take
     * significant amount of time.
     *
     * @param keyExtractor The KeyExtractor with which the DataSet is hash-partitioned.
     * @return The partitioned DataSet.
     * @see KeySelector
     */
    public <K extends Comparable<K>> PartitionOperator<T> partitionByHash(
            KeySelector<T, K> keyExtractor) {
        final TypeInformation<K> keyType =
                TypeExtractor.getKeySelectorTypes(keyExtractor, getType());
        return new PartitionOperator<>(
                this,
                PartitionMethod.HASH,
                new Keys.SelectorFunctionKeys<>(clean(keyExtractor), this.getType(), keyType),
                Utils.getCallLocationName());
    }

    /**
     * Range-partitions a DataSet on the specified key fields.
     *
     * <p><b>Important:</b>This operation requires an extra pass over the DataSet to compute the
     * range boundaries and shuffles the whole DataSet over the network. This can take significant
     * amount of time.
     *
     * @param fields The field indexes on which the DataSet is range-partitioned.
     * @return The partitioned DataSet.
     */
    public PartitionOperator<T> partitionByRange(int... fields) {
        return new PartitionOperator<>(
                this,
                PartitionMethod.RANGE,
                new Keys.ExpressionKeys<>(fields, getType()),
                Utils.getCallLocationName());
    }

    /**
     * Range-partitions a DataSet on the specified key fields.
     *
     * <p><b>Important:</b>This operation requires an extra pass over the DataSet to compute the
     * range boundaries and shuffles the whole DataSet over the network. This can take significant
     * amount of time.
     *
     * @param fields The field expressions on which the DataSet is range-partitioned.
     * @return The partitioned DataSet.
     */
    public PartitionOperator<T> partitionByRange(String... fields) {
        return new PartitionOperator<>(
                this,
                PartitionMethod.RANGE,
                new Keys.ExpressionKeys<>(fields, getType()),
                Utils.getCallLocationName());
    }

    /**
     * Range-partitions a DataSet using the specified KeySelector.
     *
     * <p><b>Important:</b>This operation requires an extra pass over the DataSet to compute the
     * range boundaries and shuffles the whole DataSet over the network. This can take significant
     * amount of time.
     *
     * @param keyExtractor The KeyExtractor with which the DataSet is range-partitioned.
     * @return The partitioned DataSet.
     * @see KeySelector
     */
    public <K extends Comparable<K>> PartitionOperator<T> partitionByRange(
            KeySelector<T, K> keyExtractor) {
        final TypeInformation<K> keyType =
                TypeExtractor.getKeySelectorTypes(keyExtractor, getType());
        return new PartitionOperator<>(
                this,
                PartitionMethod.RANGE,
                new Keys.SelectorFunctionKeys<>(clean(keyExtractor), this.getType(), keyType),
                Utils.getCallLocationName());
    }

    /**
     * Partitions a tuple DataSet on the specified key fields using a custom partitioner. This
     * method takes the key position to partition on, and a partitioner that accepts the key type.
     *
     * <p>Note: This method works only on single field keys.
     *
     * @param partitioner The partitioner to assign partitions to keys.
     * @param field The field index on which the DataSet is to partitioned.
     * @return The partitioned DataSet.
     */
    public <K> PartitionOperator<T> partitionCustom(Partitioner<K> partitioner, int field) {
        return new PartitionOperator<>(
                this,
                new Keys.ExpressionKeys<>(new int[] {field}, getType()),
                clean(partitioner),
                Utils.getCallLocationName());
    }

    /**
     * Partitions a POJO DataSet on the specified key fields using a custom partitioner. This method
     * takes the key expression to partition on, and a partitioner that accepts the key type.
     *
     * <p>Note: This method works only on single field keys.
     *
     * @param partitioner The partitioner to assign partitions to keys.
     * @param field The field index on which the DataSet is to partitioned.
     * @return The partitioned DataSet.
     */
    public <K> PartitionOperator<T> partitionCustom(Partitioner<K> partitioner, String field) {
        return new PartitionOperator<>(
                this,
                new Keys.ExpressionKeys<>(new String[] {field}, getType()),
                clean(partitioner),
                Utils.getCallLocationName());
    }

    /**
     * Partitions a DataSet on the key returned by the selector, using a custom partitioner. This
     * method takes the key selector to get the key to partition on, and a partitioner that accepts
     * the key type.
     *
     * <p>Note: This method works only on single field keys, i.e. the selector cannot return tuples
     * of fields.
     *
     * @param partitioner The partitioner to assign partitions to keys.
     * @param keyExtractor The KeyExtractor with which the DataSet is partitioned.
     * @return The partitioned DataSet.
     * @see KeySelector
     */
    public <K extends Comparable<K>> PartitionOperator<T> partitionCustom(
            Partitioner<K> partitioner, KeySelector<T, K> keyExtractor) {
        final TypeInformation<K> keyType =
                TypeExtractor.getKeySelectorTypes(keyExtractor, getType());
        return new PartitionOperator<>(
                this,
                new Keys.SelectorFunctionKeys<>(keyExtractor, getType(), keyType),
                clean(partitioner),
                Utils.getCallLocationName());
    }

    /**
     * Enforces a re-balancing of the DataSet, i.e., the DataSet is evenly distributed over all
     * parallel instances of the following task. This can help to improve performance in case of
     * heavy data skew and compute intensive operations.
     *
     * <p><b>Important:</b>This operation shuffles the whole DataSet over the network and can take
     * significant amount of time.
     *
     * @return The re-balanced DataSet.
     */
    public PartitionOperator<T> rebalance() {
        return new PartitionOperator<>(
                this, PartitionMethod.REBALANCE, Utils.getCallLocationName());
    }

    // --------------------------------------------------------------------------------------------
    //  Sorting
    // --------------------------------------------------------------------------------------------

    /**
     * Locally sorts the partitions of the DataSet on the specified field in the specified order.
     * DataSet can be sorted on multiple fields by chaining sortPartition() calls.
     *
     * @param field The field index on which the DataSet is sorted.
     * @param order The order in which the DataSet is sorted.
     * @return The DataSet with sorted local partitions.
     */
    public SortPartitionOperator<T> sortPartition(int field, Order order) {
        return new SortPartitionOperator<>(this, field, order, Utils.getCallLocationName());
    }

    /**
     * Locally sorts the partitions of the DataSet on the specified field in the specified order.
     * DataSet can be sorted on multiple fields by chaining sortPartition() calls.
     *
     * @param field The field expression referring to the field on which the DataSet is sorted.
     * @param order The order in which the DataSet is sorted.
     * @return The DataSet with sorted local partitions.
     */
    public SortPartitionOperator<T> sortPartition(String field, Order order) {
        return new SortPartitionOperator<>(this, field, order, Utils.getCallLocationName());
    }

    /**
     * Locally sorts the partitions of the DataSet on the extracted key in the specified order. The
     * DataSet can be sorted on multiple values by returning a tuple from the KeySelector.
     *
     * <p>Note that no additional sort keys can be appended to a KeySelector sort keys. To sort the
     * partitions by multiple values using KeySelector, the KeySelector must return a tuple
     * consisting of the values.
     *
     * @param keyExtractor The KeySelector function which extracts the key values from the DataSet
     *     on which the DataSet is sorted.
     * @param order The order in which the DataSet is sorted.
     * @return The DataSet with sorted local partitions.
     */
    public <K> SortPartitionOperator<T> sortPartition(KeySelector<T, K> keyExtractor, Order order) {
        final TypeInformation<K> keyType =
                TypeExtractor.getKeySelectorTypes(keyExtractor, getType());
        return new SortPartitionOperator<>(
                this,
                new Keys.SelectorFunctionKeys<>(clean(keyExtractor), getType(), keyType),
                order,
                Utils.getCallLocationName());
    }

    // --------------------------------------------------------------------------------------------
    //  Top-K
    // --------------------------------------------------------------------------------------------

    // --------------------------------------------------------------------------------------------
    //  Result writing
    // --------------------------------------------------------------------------------------------

    /**
     * Writes a DataSet as text file(s) to the specified location.
     *
     * <p>For each element of the DataSet the result of {@link Object#toString()} is written.<br>
     * <br>
     * <span class="strong">Output files and directories</span><br>
     * What output how writeAsText() method produces is depending on other circumstance
     *
     * <ul>
     *   <li>A directory is created and multiple files are written underneath. (Default behavior)
     *       <br>
     *       This sink creates a directory called "path1", and files "1", "2" ... are writen
     *       underneath depending on <a
     *       href="https://flink.apache.org/faq.html#what-is-the-parallelism-how-do-i-set-it">parallelism</a>
     *       <pre>{@code .
     * └── path1/
     *     ├── 1
     *     ├── 2
     *     └── ...}</pre>
     *       Code Example
     *       <pre>{@code dataset.writeAsText("file:///path1");}</pre>
     *   <li>A single file called "path1" is created when parallelism is set to 1
     *       <pre>{@code .
     * └── path1 }</pre>
     *       Code Example
     *       <pre>{@code // Parallelism is set to only this particular operation
     * dataset.writeAsText("file:///path1").setParallelism(1);
     *
     * // This will creates the same effect but note all operators' parallelism are set to one
     * env.setParallelism(1);
     * ...
     * dataset.writeAsText("file:///path1"); }</pre>
     *   <li>A directory is always created when <a
     *       href="https://nightlies.apache.org/flink/flink-docs-master/setup/config.html#file-systems">fs.output.always-create-directory</a>
     *       is set to true in flink-conf.yaml file, even when parallelism is set to 1.
     *       <pre>{@code .
     * └── path1/
     *     └── 1 }</pre>
     *       Code Example
     *       <pre>{@code // fs.output.always-create-directory = true
     * dataset.writeAsText("file:///path1").setParallelism(1); }</pre>
     * </ul>
     *
     * @param filePath The path pointing to the location the text file or files under the directory
     *     is written to.
     * @return The DataSink that writes the DataSet.
     * @see TextOutputFormat
     */
    public DataSink<T> writeAsText(String filePath) {
        return output(new TextOutputFormat<T>(new Path(filePath)));
    }

    /**
     * Writes a DataSet as text file(s) to the specified location.
     *
     * <p>For each element of the DataSet the result of {@link Object#toString()} is written.
     *
     * @param filePath The path pointing to the location the text file is written to.
     * @param writeMode Control the behavior for existing files. Options are NO_OVERWRITE and
     *     OVERWRITE.
     * @return The DataSink that writes the DataSet.
     * @see TextOutputFormat
     * @see DataSet#writeAsText(String) Output files and directories
     */
    public DataSink<T> writeAsText(String filePath, WriteMode writeMode) {
        TextOutputFormat<T> tof = new TextOutputFormat<>(new Path(filePath));
        tof.setWriteMode(writeMode);
        return output(tof);
    }

    /**
     * Writes a DataSet as text file(s) to the specified location.
     *
     * <p>For each element of the DataSet the result of {@link TextFormatter#format(Object)} is
     * written.
     *
     * @param filePath The path pointing to the location the text file is written to.
     * @param formatter formatter that is applied on every element of the DataSet.
     * @return The DataSink that writes the DataSet.
     * @see TextOutputFormat
     * @see DataSet#writeAsText(String) Output files and directories
     */
    public DataSink<String> writeAsFormattedText(String filePath, TextFormatter<T> formatter) {
        return map(new FormattingMapper<>(clean(formatter))).writeAsText(filePath);
    }

    /**
     * Writes a DataSet as text file(s) to the specified location.
     *
     * <p>For each element of the DataSet the result of {@link TextFormatter#format(Object)} is
     * written.
     *
     * @param filePath The path pointing to the location the text file is written to.
     * @param writeMode Control the behavior for existing files. Options are NO_OVERWRITE and
     *     OVERWRITE.
     * @param formatter formatter that is applied on every element of the DataSet.
     * @return The DataSink that writes the DataSet.
     * @see TextOutputFormat
     * @see DataSet#writeAsText(String) Output files and directories
     */
    public DataSink<String> writeAsFormattedText(
            String filePath, WriteMode writeMode, TextFormatter<T> formatter) {
        return map(new FormattingMapper<>(clean(formatter))).writeAsText(filePath, writeMode);
    }

    /**
     * Writes a {@link Tuple} DataSet as CSV file(s) to the specified location.
     *
     * <p><b>Note: Only a Tuple DataSet can written as a CSV file.</b>
     *
     * <p>For each Tuple field the result of {@link Object#toString()} is written. Tuple fields are
     * separated by the default field delimiter {@code "comma" (,)}.
     *
     * <p>Tuples are are separated by the newline character ({@code \n}).
     *
     * @param filePath The path pointing to the location the CSV file is written to.
     * @return The DataSink that writes the DataSet.
     * @see Tuple
     * @see CsvOutputFormat
     * @see DataSet#writeAsText(String) Output files and directories
     */
    public DataSink<T> writeAsCsv(String filePath) {
        return writeAsCsv(
                filePath,
                CsvOutputFormat.DEFAULT_LINE_DELIMITER,
                CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
    }

    /**
     * Writes a {@link Tuple} DataSet as CSV file(s) to the specified location.
     *
     * <p><b>Note: Only a Tuple DataSet can written as a CSV file.</b>
     *
     * <p>For each Tuple field the result of {@link Object#toString()} is written. Tuple fields are
     * separated by the default field delimiter {@code "comma" (,)}.
     *
     * <p>Tuples are are separated by the newline character ({@code \n}).
     *
     * @param filePath The path pointing to the location the CSV file is written to.
     * @param writeMode The behavior regarding existing files. Options are NO_OVERWRITE and
     *     OVERWRITE.
     * @return The DataSink that writes the DataSet.
     * @see Tuple
     * @see CsvOutputFormat
     * @see DataSet#writeAsText(String) Output files and directories
     */
    public DataSink<T> writeAsCsv(String filePath, WriteMode writeMode) {
        return internalWriteAsCsv(
                new Path(filePath),
                CsvOutputFormat.DEFAULT_LINE_DELIMITER,
                CsvOutputFormat.DEFAULT_FIELD_DELIMITER,
                writeMode);
    }

    /**
     * Writes a {@link Tuple} DataSet as CSV file(s) to the specified location with the specified
     * field and line delimiters.
     *
     * <p><b>Note: Only a Tuple DataSet can written as a CSV file.</b>
     *
     * <p>For each Tuple field the result of {@link Object#toString()} is written.
     *
     * @param filePath The path pointing to the location the CSV file is written to.
     * @param rowDelimiter The row delimiter to separate Tuples.
     * @param fieldDelimiter The field delimiter to separate Tuple fields.
     * @see Tuple
     * @see CsvOutputFormat
     * @see DataSet#writeAsText(String) Output files and directories
     */
    public DataSink<T> writeAsCsv(String filePath, String rowDelimiter, String fieldDelimiter) {
        return internalWriteAsCsv(new Path(filePath), rowDelimiter, fieldDelimiter, null);
    }

    /**
     * Writes a {@link Tuple} DataSet as CSV file(s) to the specified location with the specified
     * field and line delimiters.
     *
     * <p><b>Note: Only a Tuple DataSet can written as a CSV file.</b> For each Tuple field the
     * result of {@link Object#toString()} is written.
     *
     * @param filePath The path pointing to the location the CSV file is written to.
     * @param rowDelimiter The row delimiter to separate Tuples.
     * @param fieldDelimiter The field delimiter to separate Tuple fields.
     * @param writeMode The behavior regarding existing files. Options are NO_OVERWRITE and
     *     OVERWRITE.
     * @see Tuple
     * @see CsvOutputFormat
     * @see DataSet#writeAsText(String) Output files and directories
     */
    public DataSink<T> writeAsCsv(
            String filePath, String rowDelimiter, String fieldDelimiter, WriteMode writeMode) {
        return internalWriteAsCsv(new Path(filePath), rowDelimiter, fieldDelimiter, writeMode);
    }

    @SuppressWarnings("unchecked")
    private <X extends Tuple> DataSink<T> internalWriteAsCsv(
            Path filePath, String rowDelimiter, String fieldDelimiter, WriteMode wm) {
        Preconditions.checkArgument(
                getType().isTupleType(),
                "The writeAsCsv() method can only be used on data sets of tuples.");
        CsvOutputFormat<X> of = new CsvOutputFormat<>(filePath, rowDelimiter, fieldDelimiter);
        if (wm != null) {
            of.setWriteMode(wm);
        }
        return output((OutputFormat<T>) of);
    }

    /**
     * Prints the elements in a DataSet to the standard output stream {@link System#out} of the JVM
     * that calls the print() method. For programs that are executed in a cluster, this method needs
     * to gather the contents of the DataSet back to the client, to print it there.
     *
     * <p>The string written for each element is defined by the {@link Object#toString()} method.
     *
     * <p>This method immediately triggers the program execution, similar to the {@link #collect()}
     * and {@link #count()} methods.
     *
     * @see #printToErr()
     * @see #printOnTaskManager(String)
     */
    public void print() throws Exception {
        List<T> elements = collect();
        for (T e : elements) {
            System.out.println(e);
        }
    }

    /**
     * Prints the elements in a DataSet to the standard error stream {@link System#err} of the JVM
     * that calls the print() method. For programs that are executed in a cluster, this method needs
     * to gather the contents of the DataSet back to the client, to print it there.
     *
     * <p>The string written for each element is defined by the {@link Object#toString()} method.
     *
     * <p>This method immediately triggers the program execution, similar to the {@link #collect()}
     * and {@link #count()} methods.
     *
     * @see #print()
     * @see #printOnTaskManager(String)
     */
    public void printToErr() throws Exception {
        List<T> elements = collect();
        for (T e : elements) {
            System.err.println(e);
        }
    }

    /**
     * Writes a DataSet to the standard output streams (stdout) of the TaskManagers that execute the
     * program (or more specifically, the data sink operators). On a typical cluster setup, the data
     * will appear in the TaskManagers' <i>.out</i> files.
     *
     * <p>To print the data to the console or stdout stream of the client process instead, use the
     * {@link #print()} method.
     *
     * <p>For each element of the DataSet the result of {@link Object#toString()} is written.
     *
     * @param prefix The string to prefix each line of the output with. This helps identifying
     *     outputs from different printing sinks.
     * @return The DataSink operator that writes the DataSet.
     * @see #print()
     */
    public DataSink<T> printOnTaskManager(String prefix) {
        return output(new PrintingOutputFormat<T>(prefix, false));
    }

    /**
     * Writes a DataSet to the standard output stream (stdout).
     *
     * <p>For each element of the DataSet the result of {@link Object#toString()} is written.
     *
     * @param sinkIdentifier The string to prefix the output with.
     * @return The DataSink that writes the DataSet.
     * @deprecated Use {@link #printOnTaskManager(String)} instead.
     */
    @Deprecated
    @PublicEvolving
    public DataSink<T> print(String sinkIdentifier) {
        return output(new PrintingOutputFormat<T>(sinkIdentifier, false));
    }

    /**
     * Writes a DataSet to the standard error stream (stderr).
     *
     * <p>For each element of the DataSet the result of {@link Object#toString()} is written.
     *
     * @param sinkIdentifier The string to prefix the output with.
     * @return The DataSink that writes the DataSet.
     * @deprecated Use {@link #printOnTaskManager(String)} instead, or the {@link
     *     PrintingOutputFormat}.
     */
    @Deprecated
    @PublicEvolving
    public DataSink<T> printToErr(String sinkIdentifier) {
        return output(new PrintingOutputFormat<T>(sinkIdentifier, true));
    }

    /**
     * Writes a DataSet using a {@link FileOutputFormat} to a specified location. This method adds a
     * data sink to the program.
     *
     * @param outputFormat The FileOutputFormat to write the DataSet.
     * @param filePath The path to the location where the DataSet is written.
     * @return The DataSink that writes the DataSet.
     * @see FileOutputFormat
     */
    public DataSink<T> write(FileOutputFormat<T> outputFormat, String filePath) {
        Preconditions.checkNotNull(filePath, "File path must not be null.");
        Preconditions.checkNotNull(outputFormat, "Output format must not be null.");

        outputFormat.setOutputFilePath(new Path(filePath));
        return output(outputFormat);
    }

    /**
     * Writes a DataSet using a {@link FileOutputFormat} to a specified location. This method adds a
     * data sink to the program.
     *
     * @param outputFormat The FileOutputFormat to write the DataSet.
     * @param filePath The path to the location where the DataSet is written.
     * @param writeMode The mode of writing, indicating whether to overwrite existing files.
     * @return The DataSink that writes the DataSet.
     * @see FileOutputFormat
     */
    public DataSink<T> write(
            FileOutputFormat<T> outputFormat, String filePath, WriteMode writeMode) {
        Preconditions.checkNotNull(filePath, "File path must not be null.");
        Preconditions.checkNotNull(writeMode, "Write mode must not be null.");
        Preconditions.checkNotNull(outputFormat, "Output format must not be null.");

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
     * @see OutputFormat
     * @see DataSink
     */
    public DataSink<T> output(OutputFormat<T> outputFormat) {
        Preconditions.checkNotNull(outputFormat);

        // configure the type if needed
        if (outputFormat instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) outputFormat).setInputType(getType(), context.getConfig());
        }

        DataSink<T> sink = new DataSink<>(this, outputFormat, getType());
        this.context.registerDataSink(sink);
        return sink;
    }

    // --------------------------------------------------------------------------------------------
    //  Utilities
    // --------------------------------------------------------------------------------------------

    protected static void checkSameExecutionContext(DataSet<?> set1, DataSet<?> set2) {
        if (set1.getExecutionEnvironment() != set2.getExecutionEnvironment()) {
            throw new IllegalArgumentException("The two inputs have different execution contexts.");
        }
    }
}
