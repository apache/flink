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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.state.forst.ForStTestUtils.createKeyedStateBackend;
import static org.junit.Assert.fail;

/** Tests for {@link ForStListState}. */
public class ForStStateMigrationTest extends ForStStateTestBase {

    @Test
    void testFortMapStateKeySchemaChanged() throws Exception {
        MapStateDescriptor<Integer, String> descriptorV1 =
                new MapStateDescriptor<>(
                        "testState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);

        MapStateDescriptor<String, String> descriptorV2 =
                new MapStateDescriptor<>(
                        "testState", StringSerializer.INSTANCE, StringSerializer.INSTANCE);

        MapState<Integer, String> mapState =
                keyedBackend.createState(1, IntSerializer.INSTANCE, descriptorV1);
        setCurrentContext("test", "test");
        for (int i = 0; i < 10; i++) {
            mapState.asyncPut(i, String.valueOf(i));
        }
        drain();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                keyedBackend.snapshot(
                        1L,
                        System.currentTimeMillis(),
                        env.getCheckpointStorageAccess()
                                .resolveCheckpointStorageLocation(
                                        1L, CheckpointStorageLocationReference.getDefault()),
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        if (!snapshot.isDone()) {
            snapshot.run();
        }
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
        KeyedStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();
        IOUtils.closeQuietly(keyedBackend);
        keyedBackend.dispose();

        FileSystem.initialize(new Configuration(), null);
        Configuration configuration = new Configuration();
        ForStStateBackend forStStateBackend =
                new ForStStateBackend().configure(configuration, null);
        keyedBackend =
                createKeyedStateBackend(
                        forStStateBackend,
                        env,
                        StringSerializer.INSTANCE,
                        Collections.singletonList(stateHandle));
        keyedBackend.setup(aec);
        try {
            keyedBackend.createState(1, IntSerializer.INSTANCE, descriptorV2);
            fail("Expected a state migration exception.");
        } catch (Exception e) {
            if (CommonTestUtils.containsCause(e, StateMigrationException.class)) {
                // StateMigrationException expected
            } else {
                throw e;
            }
        }
    }
}
