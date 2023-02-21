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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The factory class for {@link DynamicFilteringDataCollectorOperator}. */
public class DynamicFilteringDataCollectorOperatorFactory
        extends AbstractStreamOperatorFactory<Object>
        implements CoordinatedOperatorFactory<Object> {

    private final Set<String> dynamicFilteringDataListenerIDs = new HashSet<>();
    private final RowType dynamicFilteringFieldType;
    private final List<Integer> dynamicFilteringFieldIndices;
    private final long threshold;

    public DynamicFilteringDataCollectorOperatorFactory(
            RowType dynamicFilteringFieldType,
            List<Integer> dynamicFilteringFieldIndices,
            long threshold) {
        this.dynamicFilteringFieldType = checkNotNull(dynamicFilteringFieldType);
        this.dynamicFilteringFieldIndices = checkNotNull(dynamicFilteringFieldIndices);
        this.threshold = threshold;
    }

    @Override
    public <T extends StreamOperator<Object>> T createStreamOperator(
            StreamOperatorParameters<Object> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();
        OperatorEventGateway operatorEventGateway =
                eventDispatcher.getOperatorEventGateway(operatorId);

        DynamicFilteringDataCollectorOperator operator =
                new DynamicFilteringDataCollectorOperator(
                        dynamicFilteringFieldType,
                        dynamicFilteringFieldIndices,
                        threshold,
                        operatorEventGateway);

        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());

        // today's lunch is generics spaghetti
        @SuppressWarnings("unchecked")
        final T castedOperator = (T) operator;

        return castedOperator;
    }

    public void registerDynamicFilteringDataListenerID(String id) {
        this.dynamicFilteringDataListenerIDs.add(checkNotNull(id));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return DynamicFilteringDataCollectorOperator.class;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new DynamicFilteringDataCollectorOperatorCoordinator.Provider(
                operatorID, new ArrayList<>(dynamicFilteringDataListenerIDs));
    }
}
