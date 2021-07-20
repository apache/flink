/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;

/** Tests for {@link RecoveredInputChannel}. */
public class RecoveredInputChannelTest {

    @Test(expected = IllegalStateException.class)
    public void testConversionOnlyPossibleAfterConsumed() throws IOException {
        buildChannel().toInputChannel();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRequestPartitionsImpossible() {
        buildChannel().requestSubpartition(0);
    }

    @Test(expected = CheckpointException.class)
    public void testCheckpointStartImpossible() throws CheckpointException {
        buildChannel().checkpointStarted(new CheckpointBarrier(0L, 0L, unaligned(getDefault())));
    }

    private RecoveredInputChannel buildChannel() {
        try {
            return new RecoveredInputChannel(
                    new SingleInputGateBuilder().build(),
                    0,
                    new ResultPartitionID(),
                    0,
                    0,
                    new SimpleCounter(),
                    new SimpleCounter(),
                    10) {
                @Override
                protected InputChannel toInputChannelInternal() {
                    throw new AssertionError("channel conversion succeeded");
                }
            };
        } catch (Exception e) {
            throw new AssertionError("channel creation failed", e);
        }
    }
}
