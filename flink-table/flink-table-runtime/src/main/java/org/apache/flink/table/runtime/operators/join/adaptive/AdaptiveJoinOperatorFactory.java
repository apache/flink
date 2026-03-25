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
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
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
    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveJoinOperatorFactory.class);

    private static final long serialVersionUID = 1L;

    private final byte[] adaptiveJoinGeneratorSerialized;

    @Nullable private StreamOperatorFactory<OUT> finalFactory;

    private final boolean checkClassLoaderLeak;

    private final FlinkJoinType joinType;

    private final boolean originIsSortMergeJoin;

    private final boolean originalLeftIsBuild;

    private boolean leftIsBuild;

    private boolean isBroadcastJoin;

    public AdaptiveJoinOperatorFactory(
            byte[] adaptiveJoinGeneratorSerialized,
            FlinkJoinType joinType,
            boolean originIsSortMergeJoin,
            boolean leftIsBuild,
            boolean checkClassLoaderLeak) {
        this.adaptiveJoinGeneratorSerialized = checkNotNull(adaptiveJoinGeneratorSerialized);
        this.joinType = joinType;
        this.originIsSortMergeJoin = originIsSortMergeJoin;
        this.leftIsBuild = leftIsBuild;
        this.originalLeftIsBuild = leftIsBuild;
        this.checkClassLoaderLeak = checkClassLoaderLeak;
    }

    @Override
    public StreamOperatorFactory<?> genOperatorFactory(
            ClassLoader userClassLoader, ReadableConfig config) {
        // In some IT/E2E tests, plannerModule may be null, so we handle it specially to avoid
        // breaking these tests.
        PlannerModule plannerModule = null;
        try {
            plannerModule = PlannerModule.getInstance();
        } catch (Throwable throwable) {
            LOG.warn(
                    "Failed to get PlannerModule instance, may cause adaptive join deserialization failure.",
                    throwable);
        }

        ClassLoader classLoader =
                plannerModule == null
                        ? userClassLoader
                        : FlinkUserCodeClassLoaders.parentFirst(
                                plannerModule.getSubmoduleClassLoader().getURLs(),
                                userClassLoader,
                                NOOP_EXCEPTION_HANDLER,
                                checkClassLoaderLeak);

        AdaptiveJoinGenerator adaptiveJoinGenerator;
        try {
            adaptiveJoinGenerator =
                    InstantiationUtil.deserializeObject(
                            adaptiveJoinGeneratorSerialized, classLoader);
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(
                    "Failed to deserialize AdaptiveJoinGenerator instance. "
                            + "Please check whether the flink-table-planner-loader.jar is in the classpath.",
                    e);
        }
        this.finalFactory =
                (StreamOperatorFactory<OUT>)
                        adaptiveJoinGenerator.genOperatorFactory(
                                classLoader,
                                config,
                                joinType,
                                originIsSortMergeJoin,
                                isBroadcastJoin,
                                leftIsBuild);
        return this.finalFactory;
    }

    @Override
    public FlinkJoinType getJoinType() {
        return joinType;
    }

    @Override
    public void markAsBroadcastJoin(boolean canBroadcast, boolean leftIsBuild) {
        this.isBroadcastJoin = canBroadcast;
        this.leftIsBuild = leftIsBuild;
    }

    @Override
    public boolean shouldReorderInputs() {
        // Sort merge join requires the left side to be read first if the broadcast threshold is not
        // met.
        if (!isBroadcastJoin && originIsSortMergeJoin) {
            return false;
        }

        if (leftIsBuild != originalLeftIsBuild) {
            LOG.info(
                    "The build side of the adaptive join has been updated. Compile phase build side: {}, Runtime build side: {}.",
                    originalLeftIsBuild ? "left" : "right",
                    leftIsBuild ? "left" : "right");
        }
        return !leftIsBuild;
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
}
