/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.adaptive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.planner.loader.PlannerModule;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adaptive join factory.
 *
 * <p>Note: This class will hold an {@link AdaptiveJoin} and serve as a proxy class to provide an
 * interface externally. Due to runtime access visibility constraints with the table-planner module,
 * the {@link AdaptiveJoin} object will be serialized during the Table Planner phase and will only
 * be lazily deserialized before the dynamic generation of the JobGraph.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class AdaptiveJoinOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
        implements AdaptiveJoin {
    private static final long serialVersionUID = 1L;

    private final byte[] adaptiveJoinSerialized;

    @Nullable private transient AdaptiveJoin adaptiveJoin;

    @Nullable private StreamOperatorFactory<OUT> finalFactory;

    public AdaptiveJoinOperatorFactory(byte[] adaptiveJoinSerialized) {
        this.adaptiveJoinSerialized = checkNotNull(adaptiveJoinSerialized);
    }

    @Override
    public StreamOperatorFactory<?> genOperatorFactory(
            ClassLoader classLoader, ReadableConfig config) {
        checkAndLazyInitialize();
        this.finalFactory =
                (StreamOperatorFactory<OUT>) adaptiveJoin.genOperatorFactory(classLoader, config);
        return this.finalFactory;
    }

    @Override
    public FlinkJoinType getJoinType() {
        checkAndLazyInitialize();
        return adaptiveJoin.getJoinType();
    }

    @Override
    public void markAsBroadcastJoin(boolean canBeBroadcast, boolean leftIsBuild) {
        checkAndLazyInitialize();
        adaptiveJoin.markAsBroadcastJoin(canBeBroadcast, leftIsBuild);
    }

    @Override
    public boolean shouldReorderInputs() {
        checkAndLazyInitialize();
        return adaptiveJoin.shouldReorderInputs();
    }

    private void checkAndLazyInitialize() {
        if (this.adaptiveJoin == null) {
            lazyInitialize();
        }
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        checkNotNull(
                finalFactory,
                String.format(
                        "The OperatorFactory of task [%s] have not been initialized.",
                        parameters.getContainingTask()));
        if (finalFactory instanceof AbstractStreamOperatorFactory) {
            ((AbstractStreamOperatorFactory<OUT>) finalFactory)
                    .setProcessingTimeService(processingTimeService);
        }
        StreamOperator<OUT> operator = finalFactory.createStreamOperator(parameters);
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        throw new UnsupportedOperationException(
                "The method should not be invoked in the "
                        + "adaptive join operator for batch jobs.");
    }

    private void lazyInitialize() {
        if (!tryInitializeAdaptiveJoin(Thread.currentThread().getContextClassLoader())) {
            boolean isSuccess =
                    tryInitializeAdaptiveJoin(
                            PlannerModule.getInstance().getSubmoduleClassLoader());
            if (!isSuccess) {
                throw new RuntimeException(
                        "Failed to deserialize AdaptiveJoin instance. "
                                + "Please check whether the flink-table-planner-loader.jar is in the classpath.");
            }
        }
    }

    private boolean tryInitializeAdaptiveJoin(ClassLoader classLoader) {
        try {
            this.adaptiveJoin =
                    InstantiationUtil.deserializeObject(adaptiveJoinSerialized, classLoader);
        } catch (ClassNotFoundException | IOException e) {
            return false;
        }

        return true;
    }
}
