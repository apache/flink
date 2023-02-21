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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** {@link CheckpointInProgressRequest} test. */
class CheckpointInProgressRequestTest {

    /**
     * Tests that a request can only be cancelled once. This is important for requests to write data
     * to prevent double recycling of their buffers.
     */
    @Test
    void testNoCancelTwice() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        CyclicBarrier barrier = new CyclicBarrier(10);
        CheckpointInProgressRequest request = cancelCountingRequest(counter, barrier);
        Thread[] threads = new Thread[barrier.getParties()];
        for (int i = 0; i < barrier.getParties(); i++) {
            threads[i] =
                    new Thread(
                            () -> {
                                try {
                                    request.cancel(new RuntimeException("test"));
                                } catch (Exception e) {
                                    fail(e.getMessage());
                                }
                                await(barrier);
                            });
        }
        for (int i = 0; i < barrier.getParties(); i++) {
            threads[i].start();
            threads[i].join();
        }

        assertThat(counter).hasValue(1);
    }

    private CheckpointInProgressRequest cancelCountingRequest(
            AtomicInteger cancelCounter, CyclicBarrier cb) {
        return new CheckpointInProgressRequest(
                "test",
                new JobVertexID(),
                0,
                1L,
                unused -> {},
                unused -> {
                    cancelCounter.incrementAndGet();
                    await(cb);
                });
    }

    private void await(CyclicBarrier cb) {
        if (cb.getNumberWaiting() == 0) {
            // skip waiting if waited inside cancel
            return;
        }
        try {
            cb.await();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
