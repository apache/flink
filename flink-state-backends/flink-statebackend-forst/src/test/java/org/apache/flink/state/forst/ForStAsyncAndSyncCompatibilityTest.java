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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.v2.StateDescriptorUtils;
import org.apache.flink.state.forst.sync.ForStSyncKeyedStateBackend;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.state.forst.ForStStateTestBase.getMockEnvironment;
import static org.apache.flink.state.forst.ForStTestUtils.createKeyedStateBackend;
import static org.apache.flink.state.forst.ForStTestUtils.createSyncKeyedStateBackend;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Compatibility test for {@link ForStKeyedStateBackend} and {@link ForStSyncKeyedStateBackend}. */
class ForStAsyncAndSyncCompatibilityTest {
    protected ForStStateBackend forStStateBackend;
    protected AsyncExecutionController<String> aec;
    protected MailboxExecutor mailboxExecutor;

    protected RecordContext<String> context;

    protected MockEnvironment env;

    @BeforeEach
    public void setup(@TempDir File temporaryFolder) throws IOException {
        FileSystem.initialize(new Configuration(), null);
        Configuration configuration = new Configuration();
        configuration.set(ForStOptions.PRIMARY_DIRECTORY, temporaryFolder.toURI().toString());
        forStStateBackend = new ForStStateBackend().configure(configuration, null);

        env = getMockEnvironment(temporaryFolder);

        mailboxExecutor =
                new MailboxExecutorImpl(
                        new TaskMailboxImpl(), 0, StreamTaskActionExecutor.IMMEDIATE);
    }

    @Test
    void testForStTransFromAsyncToSync() throws Exception {
        ForStKeyedStateBackend<String> keyedBackend =
                setUpAsyncKeyedStateBackend(Collections.emptyList());
        MapStateDescriptor<Integer, String> descriptor =
                new MapStateDescriptor<>(
                        "testState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);

        MapState<Integer, String> asyncMapState =
                keyedBackend.createState(1, IntSerializer.INSTANCE, descriptor);

        context = aec.buildContext("testRecord", "testKey");
        context.retain();
        aec.setCurrentContext(context);
        asyncMapState.asyncPut(1, "1");
        context.release();
        aec.drainInflightRecords(0);

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
        ForStSyncKeyedStateBackend<String> syncKeyedStateBackend =
                createSyncKeyedStateBackend(
                        forStStateBackend,
                        env,
                        StringSerializer.INSTANCE,
                        Collections.singletonList(stateHandle));

        try {
            org.apache.flink.api.common.state.MapState<Integer, String> syncMapState =
                    syncKeyedStateBackend.getOrCreateKeyedState(
                            IntSerializer.INSTANCE,
                            StateDescriptorUtils.transformFromV2ToV1(descriptor));
            fail();

            syncKeyedStateBackend.setCurrentKey("testKey");
            ((InternalKvState) syncKeyedStateBackend).setCurrentNamespace(1);
            assertThat(syncMapState.get(1)).isEqualTo("1");
        } catch (Exception e) {
            // Currently, ForStStateBackend does not support switching from Async to Sync, so this
            // exception will be caught here
            assertThat(e).isInstanceOf(ClassCastException.class);
            assertThat(e.getMessage())
                    .contains(
                            "org.apache.flink.runtime.state.v2.RegisteredKeyAndUserKeyValueStateBackendMetaInfo cannot be cast to class org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo");

        } finally {
            IOUtils.closeQuietly(syncKeyedStateBackend);
        }
    }

    @Test
    void testForStTransFromSyncToAsync() throws Exception {
        ForStSyncKeyedStateBackend<String> keyedBackend =
                createSyncKeyedStateBackend(
                        forStStateBackend, env, StringSerializer.INSTANCE, Collections.emptyList());
        org.apache.flink.api.common.state.MapStateDescriptor<Integer, String> descriptor =
                new org.apache.flink.api.common.state.MapStateDescriptor<>(
                        "testState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        org.apache.flink.api.common.state.MapState<Integer, String> mapState =
                keyedBackend.getOrCreateKeyedState(IntSerializer.INSTANCE, descriptor);
        keyedBackend.setCurrentKey("testKey");
        ((InternalKvState) mapState).setCurrentNamespace(1);
        mapState.put(1, "1");

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

        ForStKeyedStateBackend<String> asyncKeyedStateBackend =
                setUpAsyncKeyedStateBackend(Collections.singletonList(stateHandle));

        MapStateDescriptor<Integer, String> newStateDescriptor =
                new MapStateDescriptor<>(
                        "testState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        try {
            MapState<Integer, String> asyncMapState =
                    asyncKeyedStateBackend.createState(
                            1, IntSerializer.INSTANCE, newStateDescriptor);
            fail();

            context = aec.buildContext("testRecord", "testKey");
            context.retain();
            aec.setCurrentContext(context);
            asyncMapState
                    .asyncGet(1)
                    .thenAccept(
                            value -> {
                                assertThat(value).isEqualTo("1");
                            });
            context.release();
            aec.drainInflightRecords(0);
        } catch (Exception e) {
            // Currently, ForStStateBackend does not support switching from Sync to Async, so this
            // exception will be caught here
            assertThat(e).isInstanceOf(ClassCastException.class);
            assertThat(e.getMessage())
                    .contains(
                            "org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo cannot be cast to class org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo");
        } finally {
            IOUtils.closeQuietly(asyncKeyedStateBackend);
        }
    }

    private ForStKeyedStateBackend setUpAsyncKeyedStateBackend(
            Collection<KeyedStateHandle> stateHandles) throws IOException {
        ForStKeyedStateBackend<String> keyedStateBackend =
                createKeyedStateBackend(
                        forStStateBackend, env, StringSerializer.INSTANCE, stateHandles);
        aec =
                new AsyncExecutionController<>(
                        mailboxExecutor,
                        (a, b) -> {},
                        keyedStateBackend.createStateExecutor(),
                        new DeclarationManager(),
                        1,
                        100,
                        0,
                        1,
                        null,
                        null);
        keyedStateBackend.setup(aec);
        return keyedStateBackend;
    }
}
