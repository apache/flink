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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.NoOpFunction;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;

import java.util.List;

/** @param <IN> The input and result type. */
@Internal
public class PartitionOperatorBase<IN> extends SingleInputOperator<IN, IN, NoOpFunction> {

    public static enum PartitionMethod {
        REBALANCE,
        HASH,
        RANGE,
        CUSTOM;
    }

    // --------------------------------------------------------------------------------------------

    private final PartitionMethod partitionMethod;

    private Partitioner<?> customPartitioner;

    private DataDistribution distribution;

    private Ordering ordering;

    public PartitionOperatorBase(
            UnaryOperatorInformation<IN, IN> operatorInfo,
            PartitionMethod pMethod,
            int[] keys,
            String name) {
        super(
                new UserCodeObjectWrapper<NoOpFunction>(new NoOpFunction()),
                operatorInfo,
                keys,
                name);
        this.partitionMethod = pMethod;
    }

    public PartitionOperatorBase(
            UnaryOperatorInformation<IN, IN> operatorInfo, PartitionMethod pMethod, String name) {
        super(new UserCodeObjectWrapper<NoOpFunction>(new NoOpFunction()), operatorInfo, name);
        this.partitionMethod = pMethod;
    }

    // --------------------------------------------------------------------------------------------

    public PartitionMethod getPartitionMethod() {
        return this.partitionMethod;
    }

    public Partitioner<?> getCustomPartitioner() {
        return customPartitioner;
    }

    public DataDistribution getDistribution() {
        return this.distribution;
    }

    public void setOrdering(Ordering ordering) {
        this.ordering = ordering;
    }

    public Ordering getOrdering() {
        return ordering;
    }

    public void setDistribution(DataDistribution distribution) {
        this.distribution = distribution;
    }

    public void setCustomPartitioner(Partitioner<?> customPartitioner) {
        if (customPartitioner != null) {
            int[] keys = getKeyColumns(0);
            if (keys == null || keys.length == 0) {
                throw new IllegalArgumentException(
                        "Cannot use custom partitioner for a non-grouped GroupReduce (AllGroupReduce)");
            }
            if (keys.length > 1) {
                throw new IllegalArgumentException(
                        "Cannot use the key partitioner for composite keys (more than one key field)");
            }
        }
        this.customPartitioner = customPartitioner;
    }

    @Override
    public SingleInputSemanticProperties getSemanticProperties() {
        return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected List<IN> executeOnCollections(
            List<IN> inputData, RuntimeContext runtimeContext, ExecutionConfig executionConfig) {
        return inputData;
    }
}
