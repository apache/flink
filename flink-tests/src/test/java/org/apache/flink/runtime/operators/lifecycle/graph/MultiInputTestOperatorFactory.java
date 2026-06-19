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
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

class MultiInputTestOperatorFactory implements StreamOperatorFactory<TestDataElement> {
    private final int numInputs;
    private final TestEventQueue eventQueue;
    private ChainingStrategy strategy;
    private final String operatorId;

    public MultiInputTestOperatorFactory(
            int numInputs, TestEventQueue eventQueue, String operatorId) {
        this.numInputs = numInputs;
        this.eventQueue = eventQueue;
        this.operatorId = operatorId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<TestDataElement>> T createStreamOperator(
            StreamOperatorParameters<TestDataElement> parameters) {
        return (T) new MultiInputTestOperator(numInputs, parameters, eventQueue, operatorId);
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
    public Class<? extends StreamOperator<?>> getStreamOperatorClass(ClassLoader classLoader) {
        return MultiInputTestOperator.class;
    }
}
