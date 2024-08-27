/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for idle {@link SourceOperator}. */
@SuppressWarnings("serial")
class SourceOperatorIdleTest {

    @Nullable private SourceOperatorTestContext context;
    @Nullable private SourceOperator<Integer, MockSourceSplit> operator;

    @BeforeEach
    void setup() throws Exception {
        context = new SourceOperatorTestContext();
        operator = context.getOperator();
    }

    @AfterEach
    void tearDown() throws Exception {
        context.close();
        context = null;
        operator = null;
    }

    @Test
    void testSameAvailabilityFuture() throws Exception {
        operator.initializeState(context.createStateContext());
        operator.open();
        operator.emitNext(new CollectingDataOutput<>());
        final CompletableFuture<?> initialFuture = operator.getAvailableFuture();
        assertThat(initialFuture).isNotDone();
        final CompletableFuture<?> secondFuture = operator.getAvailableFuture();
        assertThat(initialFuture).isNotSameAs(AvailabilityProvider.AVAILABLE);
        assertThat(secondFuture).isSameAs(initialFuture);
    }
}
