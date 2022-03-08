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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

/** Unit test for idle {@link SourceOperator}. */
@SuppressWarnings("serial")
public class SourceOperatorIdleTest {

    @Nullable private SourceOperatorTestContext context;
    @Nullable private SourceOperator<Integer, MockSourceSplit> operator;

    @Before
    public void setup() throws Exception {
        context = new SourceOperatorTestContext();
        operator = context.getOperator();
    }

    @After
    public void tearDown() throws Exception {
        context.close();
        context = null;
        operator = null;
    }

    @Test
    public void testSameAvailabilityFuture() throws Exception {
        operator.initializeState(context.createStateContext());
        operator.open();
        operator.emitNext(new CollectingDataOutput<>());
        final CompletableFuture<?> initialFuture = operator.getAvailableFuture();
        assertFalse(initialFuture.isDone());
        final CompletableFuture<?> secondFuture = operator.getAvailableFuture();
        assertThat(initialFuture, not(sameInstance(AvailabilityProvider.AVAILABLE)));
        assertThat(secondFuture, sameInstance(initialFuture));
    }
}
