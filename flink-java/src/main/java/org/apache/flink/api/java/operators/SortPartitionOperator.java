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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.SortPartitionOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * This operator represents a DataSet with locally sorted partitions.
 *
 * @param <T> The type of the DataSet with locally sorted partitions.
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@Public
public class SortPartitionOperator<T> extends SingleInputOperator<T, T, SortPartitionOperator<T>> {

    private List<Keys<T>> keys;

    private List<Order> orders;

    private final String sortLocationName;

    private boolean useKeySelector;

    private SortPartitionOperator(DataSet<T> dataSet, String sortLocationName) {
        super(dataSet, dataSet.getType());

        keys = new ArrayList<>();
        orders = new ArrayList<>();
        this.sortLocationName = sortLocationName;
    }

    public SortPartitionOperator(
            DataSet<T> dataSet, int sortField, Order sortOrder, String sortLocationName) {
        this(dataSet, sortLocationName);
        this.useKeySelector = false;

        ensureSortableKey(sortField);

        keys.add(new Keys.ExpressionKeys<>(sortField, getType()));
        orders.add(sortOrder);
    }

    public SortPartitionOperator(
            DataSet<T> dataSet, String sortField, Order sortOrder, String sortLocationName) {
        this(dataSet, sortLocationName);
        this.useKeySelector = false;

        ensureSortableKey(sortField);

        keys.add(new Keys.ExpressionKeys<>(sortField, getType()));
        orders.add(sortOrder);
    }

    public <K> SortPartitionOperator(
            DataSet<T> dataSet,
            Keys.SelectorFunctionKeys<T, K> sortKey,
            Order sortOrder,
            String sortLocationName) {
        this(dataSet, sortLocationName);
        this.useKeySelector = true;

        ensureSortableKey(sortKey);

        keys.add(sortKey);
        orders.add(sortOrder);
    }

    /** Returns whether using key selector or not. */
    public boolean useKeySelector() {
        return useKeySelector;
    }

    /**
     * Appends an additional sort order with the specified field in the specified order to the local
     * partition sorting of the DataSet.
     *
     * @param field The field index of the additional sort order of the local partition sorting.
     * @param order The order of the additional sort order of the local partition sorting.
     * @return The DataSet with sorted local partitions.
     */
    public SortPartitionOperator<T> sortPartition(int field, Order order) {
        if (useKeySelector) {
            throw new InvalidProgramException(
                    "Expression keys cannot be appended after a KeySelector");
        }

        ensureSortableKey(field);
        keys.add(new Keys.ExpressionKeys<>(field, getType()));
        orders.add(order);

        return this;
    }

    /**
     * Appends an additional sort order with the specified field in the specified order to the local
     * partition sorting of the DataSet.
     *
     * @param field The field expression referring to the field of the additional sort order of the
     *     local partition sorting.
     * @param order The order of the additional sort order of the local partition sorting.
     * @return The DataSet with sorted local partitions.
     */
    public SortPartitionOperator<T> sortPartition(String field, Order order) {
        if (useKeySelector) {
            throw new InvalidProgramException(
                    "Expression keys cannot be appended after a KeySelector");
        }

        ensureSortableKey(field);
        keys.add(new Keys.ExpressionKeys<>(field, getType()));
        orders.add(order);

        return this;
    }

    public <K> SortPartitionOperator<T> sortPartition(KeySelector<T, K> keyExtractor, Order order) {
        throw new InvalidProgramException("KeySelector cannot be chained.");
    }

    private void ensureSortableKey(int field) throws InvalidProgramException {
        if (!Keys.ExpressionKeys.isSortKey(field, getType())) {
            throw new InvalidProgramException("Selected sort key is not a sortable type");
        }
    }

    private void ensureSortableKey(String field) throws InvalidProgramException {
        if (!Keys.ExpressionKeys.isSortKey(field, getType())) {
            throw new InvalidProgramException("Selected sort key is not a sortable type");
        }
    }

    private <K> void ensureSortableKey(Keys.SelectorFunctionKeys<T, K> sortKey) {
        if (!sortKey.getKeyType().isSortKeyType()) {
            throw new InvalidProgramException("Selected sort key is not a sortable type");
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Translation
    // --------------------------------------------------------------------------------------------

    protected org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?>
            translateToDataFlow(Operator<T> input) {

        String name = "Sort at " + sortLocationName;

        if (useKeySelector) {
            return translateToDataFlowWithKeyExtractor(
                    input, (Keys.SelectorFunctionKeys<T, ?>) keys.get(0), orders.get(0), name);
        }

        // flatten sort key positions
        List<Integer> allKeyPositions = new ArrayList<>();
        List<Order> allOrders = new ArrayList<>();
        for (int i = 0, length = keys.size(); i < length; i++) {
            int[] sortKeyPositions = keys.get(i).computeLogicalKeyPositions();
            Order order = orders.get(i);

            for (int sortKeyPosition : sortKeyPositions) {
                allKeyPositions.add(sortKeyPosition);
                allOrders.add(order);
            }
        }

        Ordering partitionOrdering = new Ordering();
        for (int i = 0, length = allKeyPositions.size(); i < length; i++) {
            partitionOrdering.appendOrdering(allKeyPositions.get(i), null, allOrders.get(i));
        }

        // distinguish between partition types
        UnaryOperatorInformation<T, T> operatorInfo =
                new UnaryOperatorInformation<>(getType(), getType());
        SortPartitionOperatorBase<T> noop =
                new SortPartitionOperatorBase<>(operatorInfo, partitionOrdering, name);
        noop.setInput(input);
        if (this.getParallelism() < 0) {
            // use parallelism of input if not explicitly specified
            noop.setParallelism(input.getParallelism());
        } else {
            // use explicitly specified parallelism
            noop.setParallelism(this.getParallelism());
        }

        return noop;
    }

    private <K>
            org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?>
                    translateToDataFlowWithKeyExtractor(
                            Operator<T> input,
                            Keys.SelectorFunctionKeys<T, K> keys,
                            Order order,
                            String name) {
        TypeInformation<Tuple2<K, T>> typeInfoWithKey = KeyFunctions.createTypeWithKey(keys);
        Keys.ExpressionKeys<Tuple2<K, T>> newKey = new Keys.ExpressionKeys<>(0, typeInfoWithKey);

        Operator<Tuple2<K, T>> keyedInput = KeyFunctions.appendKeyExtractor(input, keys);

        int[] sortKeyPositions = newKey.computeLogicalKeyPositions();
        Ordering partitionOrdering = new Ordering();
        for (int keyPosition : sortKeyPositions) {
            partitionOrdering.appendOrdering(keyPosition, null, order);
        }

        // distinguish between partition types
        UnaryOperatorInformation<Tuple2<K, T>, Tuple2<K, T>> operatorInfo =
                new UnaryOperatorInformation<>(typeInfoWithKey, typeInfoWithKey);
        SortPartitionOperatorBase<Tuple2<K, T>> noop =
                new SortPartitionOperatorBase<>(operatorInfo, partitionOrdering, name);
        noop.setInput(keyedInput);
        if (this.getParallelism() < 0) {
            // use parallelism of input if not explicitly specified
            noop.setParallelism(input.getParallelism());
        } else {
            // use explicitly specified parallelism
            noop.setParallelism(this.getParallelism());
        }

        return KeyFunctions.appendKeyRemover(noop, keys);
    }
}
