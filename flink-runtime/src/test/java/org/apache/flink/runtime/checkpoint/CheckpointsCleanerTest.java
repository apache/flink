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

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.flink.runtime.checkpoint.CompletedCheckpointStoreTest.createCheckpoint;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link CheckpointsCleaner} test. */
class CheckpointsCleanerTest {

    @Test
    void testNotCleanCheckpointInUse() {
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
        assertThat(cp1.isDiscarded()).isFalse();
        assertThat(cp2.isDiscarded()).isTrue();
    }

    @Test
    void testNotCleanHigherCheckpoint() {
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
        assertThat(cp1.isDiscarded()).isTrue();
        // cp2 is the lowest checkpoint that is still valid, shouldn't discard.
        assertThat(cp2.isDiscarded()).isFalse();
        // cp3 is higher than cp2, shouldn't discard.
        assertThat(cp3.isDiscarded()).isFalse();
    }
}
