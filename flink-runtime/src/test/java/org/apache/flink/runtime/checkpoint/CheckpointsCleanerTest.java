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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.CompletedCheckpointStoreTest.TestCompletedCheckpoint;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Test;

import java.util.Collections;

import static org.apache.flink.runtime.checkpoint.CompletedCheckpointStoreTest.createCheckpoint;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** {@link CheckpointsCleaner} test. */
public class CheckpointsCleanerTest {

    @Test
    public void testNotCleanCheckpointInUse() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
        TestCompletedCheckpoint cp1 = createCheckpoint(1, sharedStateRegistry);
        checkpointsCleaner.addSubsumedCheckpoint(cp1);
        TestCompletedCheckpoint cp2 = createCheckpoint(2, sharedStateRegistry);
        checkpointsCleaner.addSubsumedCheckpoint(cp2);
        TestCompletedCheckpoint cp3 = createCheckpoint(3, sharedStateRegistry);
        checkpointsCleaner.addSubsumedCheckpoint(cp3);
        checkpointsCleaner.cleanSubsumedCheckpoints(
                3, Collections.singleton(1L), () -> {}, Executors.directExecutor());
        // cp 1 is in use, shouldn't discard.
        assertFalse(cp1.isDiscarded());
        assertTrue(cp2.isDiscarded());
    }

    @Test
    public void testNotCleanHigherCheckpoint() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
        TestCompletedCheckpoint cp1 = createCheckpoint(1, sharedStateRegistry);
        checkpointsCleaner.addSubsumedCheckpoint(cp1);
        TestCompletedCheckpoint cp2 = createCheckpoint(2, sharedStateRegistry);
        checkpointsCleaner.addSubsumedCheckpoint(cp2);
        TestCompletedCheckpoint cp3 = createCheckpoint(3, sharedStateRegistry);
        checkpointsCleaner.addSubsumedCheckpoint(cp3);
        checkpointsCleaner.cleanSubsumedCheckpoints(
                2, Collections.emptySet(), () -> {}, Executors.directExecutor());
        assertTrue(cp1.isDiscarded());
        // cp2 is the lowest checkpoint that is still valid, shouldn't discard.
        assertFalse(cp2.isDiscarded());
        // cp3 is higher than cp2, shouldn't discard.
        assertFalse(cp3.isDiscarded());
    }
}
