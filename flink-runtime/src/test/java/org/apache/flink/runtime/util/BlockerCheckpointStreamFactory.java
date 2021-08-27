/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** {@link CheckpointStreamFactory} for tests that allows for testing cancellation in async IO. */
@VisibleForTesting
@Internal
public class BlockerCheckpointStreamFactory implements CheckpointStreamFactory {

    protected final int maxSize;
    protected volatile int afterNumberInvocations;
    protected volatile OneShotLatch blocker;
    protected volatile OneShotLatch waiter;

    protected final Set<BlockingCheckpointOutputStream> allCreatedStreams;

    public Set<BlockingCheckpointOutputStream> getAllCreatedStreams() {
        return allCreatedStreams;
    }

    public BlockerCheckpointStreamFactory(int maxSize) {
        this.maxSize = maxSize;
        this.allCreatedStreams = new HashSet<>();
    }

    public void setAfterNumberInvocations(int afterNumberInvocations) {
        this.afterNumberInvocations = afterNumberInvocations;
    }

    public void setBlockerLatch(OneShotLatch latch) {
        this.blocker = latch;
    }

    public void setWaiterLatch(OneShotLatch latch) {
        this.waiter = latch;
    }

    public OneShotLatch getBlockerLatch() {
        return blocker;
    }

    public OneShotLatch getWaiterLatch() {
        return waiter;
    }

    @Override
    public CheckpointStateOutputStream createCheckpointStateOutputStream(
            CheckpointedStateScope scope) throws IOException {

        BlockingCheckpointOutputStream blockingStream =
                new BlockingCheckpointOutputStream(
                        new MemCheckpointStreamFactory.MemoryCheckpointOutputStream(maxSize),
                        waiter,
                        blocker,
                        afterNumberInvocations);

        allCreatedStreams.add(blockingStream);

        return blockingStream;
    }
}
