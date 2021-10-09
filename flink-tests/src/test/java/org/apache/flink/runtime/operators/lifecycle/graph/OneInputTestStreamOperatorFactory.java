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

package org.apache.flink.runtime.operators.lifecycle.graph;

import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

class OneInputTestStreamOperatorFactory
        implements OneInputStreamOperatorFactory<TestDataElement, TestDataElement>,
                ProcessingTimeServiceAware {
    private ChainingStrategy strategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;
    private ProcessingTimeService processingTimeService;
    private final String operatorID;
    private final TestEventQueue eventQueue;

    OneInputTestStreamOperatorFactory(String operatorID, TestEventQueue eventQueue) {
        this.operatorID = operatorID;
        this.eventQueue = eventQueue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<TestDataElement>> T createStreamOperator(
            StreamOperatorParameters<TestDataElement> parameters) {
        OneInputTestStreamOperator operator =
                new OneInputTestStreamOperator(operatorID, eventQueue);
        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        operator.setProcessingTimeService(processingTimeService);
        return (T) operator;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return strategy;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return OneInputTestStreamOperator.class;
    }

    @Override
    public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        this.processingTimeService = processingTimeService;
    }
}
