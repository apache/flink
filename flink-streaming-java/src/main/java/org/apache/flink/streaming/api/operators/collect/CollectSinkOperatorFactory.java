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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.time.Duration;

/** The Factory class for {@link CollectSinkOperator}. */
public class CollectSinkOperatorFactory<IN> extends SimpleUdfStreamOperatorFactory<Object>
        implements CoordinatedOperatorFactory<Object> {

    private static final long serialVersionUID = 1L;

    private final CollectSinkOperator<IN> operator;
    private final int socketTimeoutMillis;

    public CollectSinkOperatorFactory(TypeSerializer<IN> serializer, String accumulatorName) {
        this(
                serializer,
                accumulatorName,
                MAX_BATCH_SIZE.defaultValue(),
                SOCKET_TIMEOUT.defaultValue());
    }

    public CollectSinkOperatorFactory(
            TypeSerializer<IN> serializer,
            String accumulatorName,
            MemorySize maxBatchSize,
            Duration socketTimeout) {
        super(new CollectSinkOperator<>(serializer, maxBatchSize.getBytes(), accumulatorName));
        this.operator = (CollectSinkOperator<IN>) getOperator();
        this.socketTimeoutMillis = (int) socketTimeout.toMillis();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Object>> T createStreamOperator(
            StreamOperatorParameters<Object> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

        operator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorId));
        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        eventDispatcher.registerEventHandler(operatorId, operator);

        return (T) operator;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        operator.getOperatorIdFuture().complete(operatorID);
        return new CollectSinkOperatorCoordinator.Provider(operatorID, socketTimeoutMillis);
    }

    // these configs are rarely used, so we're not exposing them to the user by docs
    public static final ConfigOption<MemorySize> MAX_BATCH_SIZE =
            ConfigOptions.key("collect-sink.batch-size.max")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(2));
    public static final ConfigOption<Duration> SOCKET_TIMEOUT =
            ConfigOptions.key("collect-sink.socket-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10));
}
