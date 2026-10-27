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

package org.apache.flink.state.api;

import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.state.api.runtime.SavepointLoader;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link StateTableUtils} that do not require a running Flink cluster. */
public class StateTableUtilsTest {

    // -------------------------------------------------------------------------
    // getOperatorIdentifiers — filters operators without keyed state
    // -------------------------------------------------------------------------

    @Test
    public void testGetOperatorIdentifiersFiltersEmptyOperators() {
        OperatorState opState1 = new OperatorState(null, null, new OperatorID(1L, 2L), 1, 128);
        OperatorState opState2 = new OperatorState(null, null, new OperatorID(3L, 4L), 2, 128);

        // No subtasks means no keyed state, regardless of how many such operators are present.
        List<List<OperatorState>> cases =
                Arrays.asList(
                        Collections.singletonList(opState1),
                        Collections.emptyList(),
                        Arrays.asList(opState1, opState2));

        for (List<OperatorState> operators : cases) {
            CheckpointMetadata metadata =
                    new CheckpointMetadata(1L, operators, Collections.emptyList());
            List<OperatorIdentifier> ids = StateTableUtils.getOperatorIdentifiers(metadata);

            assertNotNull(ids);
            assertTrue(
                    ids.isEmpty(),
                    "Operators without keyed state should be filtered out, input: " + operators);
        }
    }

    // -------------------------------------------------------------------------
    // detectStateBackendType — reads real checkpoint metadata produced by other tests
    // -------------------------------------------------------------------------

    /**
     * Checkpoint (native format) taken with the HashMap state backend, committed as a fixture for
     * {@code StatefulJobSnapshotMigrationITCase} in flink-tests. Native-format keyed state handles
     * retain their backend-specific type, so every keyed operator here must resolve to {@link
     * StateBackendLoader#HASHMAP_STATE_BACKEND_NAME}.
     */
    private static final String HASHMAP_CHECKPOINT_DIR =
            "../../flink-tests/src/test/resources/"
                    + "new-stateful-udf-migration-itcase-flink1.20-hashmap-checkpoint";

    /**
     * Checkpoint (native format) taken with the RocksDB state backend, committed as a fixture for
     * {@code StatefulJobSnapshotMigrationITCase} in flink-tests. Must resolve to {@link
     * StateBackendLoader#ROCKSDB_STATE_BACKEND_NAME}.
     */
    private static final String ROCKSDB_CHECKPOINT_DIR =
            "../../flink-tests/src/test/resources/"
                    + "new-stateful-udf-migration-itcase-flink2.1-rocksdb-checkpoint";

    @Test
    public void testDetectStateBackendTypeFromHashMapCheckpoint() throws IOException {
        assertAllKeyedOperatorsDetectAs(
                HASHMAP_CHECKPOINT_DIR, StateBackendLoader.HASHMAP_STATE_BACKEND_NAME);
    }

    @Test
    public void testDetectStateBackendTypeFromRocksDBCheckpoint() throws IOException {
        assertAllKeyedOperatorsDetectAs(
                ROCKSDB_CHECKPOINT_DIR, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME);
    }

    /**
     * Loads the checkpoint metadata at {@code checkpointDir} and asserts that every operator
     * carrying keyed state resolves to exactly {@code expectedType}, and that at least one operator
     * did so (i.e. the fixture actually exercises the detection logic).
     */
    private static void assertAllKeyedOperatorsDetectAs(String checkpointDir, String expectedType)
            throws IOException {
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(checkpointDir);
        assertFalse(metadata.getOperatorStates().isEmpty());

        int operatorsWithKeyedState = 0;
        for (OperatorState opState : metadata.getOperatorStates()) {
            Optional<String> detected = StateTableUtils.detectStateBackendType(opState);
            if (detected.isEmpty()) {
                continue;
            }
            operatorsWithKeyedState++;
            assertEquals(expectedType, detected.get(), "operator " + opState.getOperatorID());
        }
        assertTrue(
                operatorsWithKeyedState > 0,
                "Expected at least one operator with keyed state in " + checkpointDir);
    }
}
