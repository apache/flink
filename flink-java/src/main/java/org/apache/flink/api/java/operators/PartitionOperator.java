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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Keys.SelectorFunctionKeys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase.PartitionMethod;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * This operator represents a partitioning.
 *
 * @param <T> The type of the data being partitioned.
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@Public
public class PartitionOperator<T> extends SingleInputOperator<T, T, PartitionOperator<T>> {

    private final Keys<T> pKeys;
    private final PartitionMethod pMethod;
    private final String partitionLocationName;
    private final Partitioner<?> customPartitioner;
    private final DataDistribution distribution;
    private Order[] orders;

    public PartitionOperator(
            DataSet<T> input,
            PartitionMethod pMethod,
            Keys<T> pKeys,
            String partitionLocationName) {
        this(input, pMethod, pKeys, null, null, null, partitionLocationName);
    }

    public PartitionOperator(
            DataSet<T> input,
            PartitionMethod pMethod,
            Keys<T> pKeys,
            DataDistribution distribution,
            String partitionLocationName) {
        this(input, pMethod, pKeys, null, null, distribution, partitionLocationName);
    }

    public PartitionOperator(
            DataSet<T> input, PartitionMethod pMethod, String partitionLocationName) {
        this(input, pMethod, null, null, null, null, partitionLocationName);
    }

    public PartitionOperator(
            DataSet<T> input,
            Keys<T> pKeys,
            Partitioner<?> customPartitioner,
            String partitionLocationName) {
        this(
                input,
                PartitionMethod.CUSTOM,
                pKeys,
                customPartitioner,
                null,
                null,
                partitionLocationName);
    }

    public <P> PartitionOperator(
            DataSet<T> input,
            Keys<T> pKeys,
            Partitioner<P> customPartitioner,
            TypeInformation<P> partitionerTypeInfo,
            String partitionLocationName) {
        this(
                input,
                PartitionMethod.CUSTOM,
                pKeys,
                customPartitioner,
                partitionerTypeInfo,
                null,
                partitionLocationName);
    }

    private <P> PartitionOperator(
            DataSet<T> input,
            PartitionMethod pMethod,
            Keys<T> pKeys,
            Partitioner<P> customPartitioner,
            TypeInformation<P> partitionerTypeInfo,
            DataDistribution distribution,
            String partitionLocationName) {
        super(input, input.getType());

        Preconditions.checkNotNull(pMethod);
        Preconditions.checkArgument(
                pKeys != null || pMethod == PartitionMethod.REBALANCE,
                "Partitioning requires keys");
        Preconditions.checkArgument(
                pMethod != PartitionMethod.CUSTOM || customPartitioner != null,
                "Custom partitioning requires a partitioner.");
        Preconditions.checkArgument(
                distribution == null || pMethod == PartitionMethod.RANGE,
                "Customized data distribution is only necessary for range partition.");

        if (distribution != null) {
            Preconditions.checkArgument(
                    pKeys.getNumberOfKeyFields() <= distribution.getNumberOfFields(),
                    "The distribution must provide at least as many fields as flat key fields are specified.");
            Preconditions.checkArgument(
                    Arrays.equals(
                            pKeys.getKeyFieldTypes(),
                            Arrays.copyOfRange(
                                    distribution.getKeyTypes(), 0, pKeys.getNumberOfKeyFields())),
                    "The types of the flat key fields must be equal to the types of the fields of the distribution.");
        }

        if (customPartitioner != null) {
            pKeys.validateCustomPartitioner(customPartitioner, partitionerTypeInfo);
        }

        this.pMethod = pMethod;
        this.pKeys = pKeys;
        this.partitionLocationName = partitionLocationName;
        this.customPartitioner = customPartitioner;
        this.distribution = distribution;
    }

    /**
     * Sets the order of keys for range partitioning. NOTE: Only valid for {@link
     * PartitionMethod#RANGE}.
     *
     * @param orders array of orders for each specified partition key
     * @return The partitioneOperator with properly set orders for given keys
     */
    @PublicEvolving
    public PartitionOperator<T> withOrders(Order... orders) {
        Preconditions.checkState(
                pMethod == PartitionMethod.RANGE,
                "Orders cannot be applied for %s partition " + "method",
                pMethod);
        Preconditions.checkArgument(
                pKeys.getOriginalKeyFieldTypes().length == orders.length,
                "The number of key " + "fields and orders should be the same.");
        this.orders = orders;

        return this;
    }

    // --------------------------------------------------------------------------------------------
    //  Properties
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the custom partitioner from this partitioning.
     *
     * @return The custom partitioner.
     */
    @Internal
    public Partitioner<?> getCustomPartitioner() {
        return customPartitioner;
    }

    // --------------------------------------------------------------------------------------------
    //  Translation
    // --------------------------------------------------------------------------------------------

    protected org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?>
            translateToDataFlow(Operator<T> input) {

        String name = "Partition at " + partitionLocationName;

        // distinguish between partition types
        if (pMethod == PartitionMethod.REBALANCE) {

            UnaryOperatorInformation<T, T> operatorInfo =
                    new UnaryOperatorInformation<>(getType(), getType());
            PartitionOperatorBase<T> rebalancedInput =
                    new PartitionOperatorBase<>(operatorInfo, pMethod, name);
            rebalancedInput.setInput(input);
            rebalancedInput.setParallelism(getParallelism());

            return rebalancedInput;
        } else if (pMethod == PartitionMethod.HASH
                || pMethod == PartitionMethod.CUSTOM
                || pMethod == PartitionMethod.RANGE) {

            if (pKeys instanceof Keys.ExpressionKeys) {

                int[] logicalKeyPositions = pKeys.computeLogicalKeyPositions();
                UnaryOperatorInformation<T, T> operatorInfo =
                        new UnaryOperatorInformation<>(getType(), getType());
                PartitionOperatorBase<T> partitionedInput =
                        new PartitionOperatorBase<>(
                                operatorInfo, pMethod, logicalKeyPositions, name);
                partitionedInput.setInput(input);
                partitionedInput.setParallelism(getParallelism());
                partitionedInput.setDistribution(distribution);
                partitionedInput.setCustomPartitioner(customPartitioner);
                partitionedInput.setOrdering(computeOrdering(pKeys, orders));

                return partitionedInput;
            } else if (pKeys instanceof Keys.SelectorFunctionKeys) {

                @SuppressWarnings("unchecked")
                Keys.SelectorFunctionKeys<T, ?> selectorKeys =
                        (Keys.SelectorFunctionKeys<T, ?>) pKeys;
                return translateSelectorFunctionPartitioner(
                        selectorKeys,
                        pMethod,
                        name,
                        input,
                        getParallelism(),
                        customPartitioner,
                        orders);
            } else {
                throw new UnsupportedOperationException("Unrecognized key type.");
            }

        } else {
            throw new UnsupportedOperationException(
                    "Unsupported partitioning method: " + pMethod.name());
        }
    }

    private static <T> Ordering computeOrdering(Keys<T> pKeys, Order[] orders) {
        Ordering ordering = new Ordering();
        final int[] logicalKeyPositions = pKeys.computeLogicalKeyPositions();

        if (orders == null) {
            for (int key : logicalKeyPositions) {
                ordering.appendOrdering(key, null, Order.ASCENDING);
            }
        } else {
            final TypeInformation<?>[] originalKeyFieldTypes = pKeys.getOriginalKeyFieldTypes();
            int index = 0;
            for (int i = 0; i < originalKeyFieldTypes.length; i++) {
                final int typeTotalFields = originalKeyFieldTypes[i].getTotalFields();
                for (int j = index; j < index + typeTotalFields; j++) {
                    ordering.appendOrdering(logicalKeyPositions[j], null, orders[i]);
                }
                index += typeTotalFields;
            }
        }

        return ordering;
    }

    @SuppressWarnings("unchecked")
    private static <T, K>
            org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?>
                    translateSelectorFunctionPartitioner(
                            SelectorFunctionKeys<T, ?> rawKeys,
                            PartitionMethod pMethod,
                            String name,
                            Operator<T> input,
                            int partitionDop,
                            Partitioner<?> customPartitioner,
                            Order[] orders) {
        final SelectorFunctionKeys<T, K> keys = (SelectorFunctionKeys<T, K>) rawKeys;
        TypeInformation<Tuple2<K, T>> typeInfoWithKey = KeyFunctions.createTypeWithKey(keys);

        Operator<Tuple2<K, T>> keyedInput = KeyFunctions.appendKeyExtractor(input, keys);

        PartitionOperatorBase<Tuple2<K, T>> keyedPartitionedInput =
                new PartitionOperatorBase<>(
                        new UnaryOperatorInformation<>(typeInfoWithKey, typeInfoWithKey),
                        pMethod,
                        new int[] {0},
                        name);
        keyedPartitionedInput.setInput(keyedInput);
        keyedPartitionedInput.setCustomPartitioner(customPartitioner);
        keyedPartitionedInput.setParallelism(partitionDop);
        keyedPartitionedInput.setOrdering(
                new Ordering(0, null, orders != null ? orders[0] : Order.ASCENDING));

        return KeyFunctions.appendKeyRemover(keyedPartitionedInput, keys);
    }
}
