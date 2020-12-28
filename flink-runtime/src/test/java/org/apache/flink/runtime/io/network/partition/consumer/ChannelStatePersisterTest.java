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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** {@link ChannelStatePersister} test. */
public class ChannelStatePersisterTest {

    @Test
    public void testNewBarrierNotOverwrittenByStopPersisting() throws IOException {
        ChannelStatePersister persister =
                new ChannelStatePersister(ChannelStateWriter.NO_OP, new InputChannelInfo(0, 0));

        persister.checkForBarrier(barrier(1L));
        persister.startPersisting(1L, Collections.emptyList());

        // meanwhile, checkpoint coordinator timed out the 1st checkpoint and started the 2nd
        // now task thread is picking up the barrier and aborts the 1st:
        persister.checkForBarrier(barrier(2L));
        persister.stopPersisting(1L);

        assertTrue(persister.hasBarrierReceived());
    }

    @Test
    public void testNewBarrierNotOverwrittenByCheckForBarrier() throws IOException {
        ChannelStatePersister persister =
                new ChannelStatePersister(ChannelStateWriter.NO_OP, new InputChannelInfo(0, 0));

        persister.startPersisting(1L, Collections.emptyList());
        persister.startPersisting(2L, Collections.emptyList());

        assertFalse(persister.checkForBarrier(barrier(1L)).isPresent());

        assertFalse(persister.hasBarrierReceived());
    }

    private static Buffer barrier(long id) throws IOException {
        return EventSerializer.toBuffer(
                new CheckpointBarrier(id, 1L, CheckpointOptions.forCheckpointWithDefaultLocation()),
                true);
    }
}
