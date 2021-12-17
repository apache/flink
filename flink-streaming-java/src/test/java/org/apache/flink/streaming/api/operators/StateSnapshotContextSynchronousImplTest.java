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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/** Tests for {@link StateSnapshotContextSynchronousImpl}. */
public class StateSnapshotContextSynchronousImplTest extends TestLogger {

    private StateSnapshotContextSynchronousImpl snapshotContext;

    @Before
    public void setUp() throws Exception {
        CloseableRegistry closableRegistry = new CloseableRegistry();
        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(1024);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 2);
        this.snapshotContext =
                new StateSnapshotContextSynchronousImpl(
                        42, 4711, streamFactory, keyGroupRange, closableRegistry);
    }

    @Test
    public void testMetaData() {
        assertEquals(42, snapshotContext.getCheckpointId());
        assertEquals(4711, snapshotContext.getCheckpointTimestamp());
    }

    @Test
    public void testCreateRawKeyedStateOutput() throws Exception {
        KeyedStateCheckpointOutputStream stream = snapshotContext.getRawKeyedOperatorStateOutput();
        Assert.assertNotNull(stream);
    }

    @Test
    public void testCreateRawOperatorStateOutput() throws Exception {
        OperatorStateCheckpointOutputStream stream = snapshotContext.getRawOperatorStateOutput();
        Assert.assertNotNull(stream);
    }

    /**
     * Tests that closing the StateSnapshotContextSynchronousImpl will also close the associated
     * output streams.
     */
    @Test
    public void testStreamClosingWhenClosing() throws Exception {
        long checkpointId = 42L;
        long checkpointTimestamp = 1L;

        CheckpointStreamFactory.CheckpointStateOutputStream outputStream1 =
                mock(CheckpointStreamFactory.CheckpointStateOutputStream.class);
        CheckpointStreamFactory.CheckpointStateOutputStream outputStream2 =
                mock(CheckpointStreamFactory.CheckpointStateOutputStream.class);

        CheckpointStreamFactory streamFactory = mock(CheckpointStreamFactory.class);
        when(streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE))
                .thenReturn(outputStream1, outputStream2);

        InsightCloseableRegistry closableRegistry = new InsightCloseableRegistry();

        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 2);

        StateSnapshotContextSynchronousImpl context =
                new StateSnapshotContextSynchronousImpl(
                        checkpointId,
                        checkpointTimestamp,
                        streamFactory,
                        keyGroupRange,
                        closableRegistry);

        // creating the output streams
        context.getRawKeyedOperatorStateOutput();
        context.getRawOperatorStateOutput();

        verify(streamFactory, times(2))
                .createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);

        assertEquals(2, closableRegistry.size());
        assertTrue(closableRegistry.contains(outputStream1));
        assertTrue(closableRegistry.contains(outputStream2));

        context.getKeyedStateStreamFuture().run();
        context.getOperatorStateStreamFuture().run();

        verify(outputStream1).closeAndGetHandle();
        verify(outputStream2).closeAndGetHandle();

        assertEquals(0, closableRegistry.size());
    }

    @Test
    public void testStreamClosingExceptionally() throws Exception {
        long checkpointId = 42L;
        long checkpointTimestamp = 1L;

        CheckpointStreamFactory.CheckpointStateOutputStream outputStream1 =
                mock(CheckpointStreamFactory.CheckpointStateOutputStream.class);
        CheckpointStreamFactory.CheckpointStateOutputStream outputStream2 =
                mock(CheckpointStreamFactory.CheckpointStateOutputStream.class);

        CheckpointStreamFactory streamFactory = mock(CheckpointStreamFactory.class);
        when(streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE))
                .thenReturn(outputStream1, outputStream2);

        InsightCloseableRegistry closableRegistry = new InsightCloseableRegistry();

        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 2);

        StateSnapshotContextSynchronousImpl context =
                new StateSnapshotContextSynchronousImpl(
                        checkpointId,
                        checkpointTimestamp,
                        streamFactory,
                        keyGroupRange,
                        closableRegistry);

        // creating the output streams
        context.getRawKeyedOperatorStateOutput();
        context.getRawOperatorStateOutput();

        verify(streamFactory, times(2))
                .createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);

        assertEquals(2, closableRegistry.size());
        assertTrue(closableRegistry.contains(outputStream1));
        assertTrue(closableRegistry.contains(outputStream2));

        context.closeExceptionally();

        verify(outputStream1).close();
        verify(outputStream2).close();

        assertEquals(0, closableRegistry.size());
    }

    static final class InsightCloseableRegistry extends CloseableRegistry {
        public int size() {
            return getNumberOfRegisteredCloseables();
        }

        public boolean contains(Closeable closeable) {
            return isCloseableRegistered(closeable);
        }
    }
}
