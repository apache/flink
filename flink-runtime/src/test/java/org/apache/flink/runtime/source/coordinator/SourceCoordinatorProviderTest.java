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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link SourceCoordinatorProvider}. */
public class SourceCoordinatorProviderTest {

    private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);
    private static final int NUM_SPLITS = 10;

    private SourceCoordinatorProvider<MockSourceSplit> provider;

    @Before
    public void setup() {
        provider =
                new SourceCoordinatorProvider<>(
                        "SourceCoordinatorProviderTest",
                        OPERATOR_ID,
                        new MockSource(Boundedness.BOUNDED, NUM_SPLITS),
                        1);
    }

    @Test
    public void testCreate() throws Exception {
        OperatorCoordinator coordinator =
                provider.create(new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SPLITS));
        assertTrue(coordinator instanceof RecreateOnResetOperatorCoordinator);
    }

    @Test
    public void testCheckpointAndReset() throws Exception {
        final OperatorCoordinator.Context context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SPLITS);
        final RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context);
        final SourceCoordinator<?, ?> sourceCoordinator =
                (SourceCoordinator<?, ?>) coordinator.getInternalCoordinator();

        // Start the coordinator.
        coordinator.start();
        // register reader 0 and take a checkpoint.
        coordinator.handleEventFromOperator(0, new ReaderRegistrationEvent(0, "location"));
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        coordinator.checkpointCoordinator(0L, future);
        byte[] bytes = future.get();

        // Register reader 1.
        coordinator.handleEventFromOperator(1, new ReaderRegistrationEvent(1, "location"));
        // Wait until the coordinator context is updated with registration of reader 1.
        while (sourceCoordinator.getContext().registeredReaders().size() < 2) {
            Thread.sleep(1);
        }

        // reset the coordinator to the checkpoint which only contains reader 0.
        coordinator.resetToCheckpoint(0L, bytes);
        final SourceCoordinator<?, ?> restoredSourceCoordinator =
                (SourceCoordinator<?, ?>) coordinator.getInternalCoordinator();
        assertNotEquals(
                "The restored source coordinator should be a different instance",
                restoredSourceCoordinator,
                sourceCoordinator);
        // FLINK-21452: do not (re)store registered readers
        assertEquals(
                "There should be no registered reader.",
                0,
                restoredSourceCoordinator.getContext().registeredReaders().size());
    }

    @Test
    public void testCallAsyncExceptionFailsJob() throws Exception {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SPLITS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context);
        SourceCoordinator<?, ?> sourceCoordinator =
                (SourceCoordinator<?, ?>) coordinator.getInternalCoordinator();
        sourceCoordinator
                .getContext()
                .callAsync(
                        () -> null,
                        (ignored, e) -> {
                            throw new RuntimeException();
                        });
        CommonTestUtils.waitUtil(
                context::isJobFailed,
                Duration.ofSeconds(10L),
                "The job did not fail before timeout.");
    }
}
