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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.runtime.util.BlockingCheckpointOutputStream;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.Snapshot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/** Tests for the partitioned state part of {@link EmbeddedRocksDBStateBackend}. */
@ExtendWith(ParameterizedTestExtension.class)
public class EmbeddedRocksDBStateBackendTest
        extends StateBackendTestBase<EmbeddedRocksDBStateBackend> {

    @TempDir private static java.nio.file.Path tempFolder;
    private OneShotLatch blocker;
    private OneShotLatch waiter;
    private BlockerCheckpointStreamFactory testStreamFactory;
    private RocksDBKeyedStateBackend<Integer> keyedStateBackend;
    private List<RocksObject> allCreatedCloseables;
    private ValueState<Integer> testState1;
    private ValueState<String> testState2;

    @Parameters
    public static List<Object[]> modes() {
        return Arrays.asList(
                new Object[][] {
                    {
                        true,
                        (SupplierWithException<CheckpointStorage, IOException>)
                                JobManagerCheckpointStorage::new
                    },
                    {
                        false,
                        (SupplierWithException<CheckpointStorage, IOException>)
                                () -> {
                                    String checkpointPath =
                                            TempDirUtils.newFolder(tempFolder).toURI().toString();
                                    return new FileSystemCheckpointStorage(
                                            new Path(checkpointPath), 0, -1);
                                }
                    }
                });
    }

    @Parameter(value = 0)
    public boolean enableIncrementalCheckpointing;

    @Parameter(value = 1)
    public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    // Store it because we need it for the cleanup test.
    private String dbPath;
    private RocksDB db = null;
    private ColumnFamilyHandle defaultCFHandle = null;
    private RocksDBStateUploader rocksDBStateUploader = null;
    private final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();

    public void prepareRocksDB() throws Exception {
        String dbPath =
                RocksDBKeyedStateBackendBuilder.getInstanceRocksDBPath(
                                TempDirUtils.newFolder(tempFolder))
                        .getAbsolutePath();
        ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();

        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
        db =
                RocksDBOperationUtils.openDB(
                        dbPath,
                        Collections.emptyList(),
                        columnFamilyHandles,
                        columnOptions,
                        optionsContainer.getDbOptions());
        defaultCFHandle = columnFamilyHandles.remove(0);
    }

    @Override
    protected ConfigurableStateBackend getStateBackend() throws IOException {
        dbPath = TempDirUtils.newFolder(tempFolder).getAbsolutePath();
        EmbeddedRocksDBStateBackend backend =
                new EmbeddedRocksDBStateBackend(enableIncrementalCheckpointing);
        Configuration configuration = new Configuration();
        configuration.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
        backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
        backend.setDbStoragePath(dbPath);
        return backend;
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        return storageSupplier.get();
    }

    @Override
    protected boolean isSerializerPresenceRequiredOnRestore() {
        return false;
    }

    @Override
    protected boolean supportsAsynchronousSnapshots() {
        return true;
    }

    @Override
    protected boolean isSafeToReuseKVState() {
        return true;
    }

    // small safety net for instance cleanups, so that no native objects are left
    @AfterEach
    public void cleanupRocksDB() {
        if (keyedStateBackend != null) {
            IOUtils.closeQuietly(keyedStateBackend);
            keyedStateBackend.dispose();
        }
        IOUtils.closeQuietly(defaultCFHandle);
        IOUtils.closeQuietly(db);
        IOUtils.closeQuietly(optionsContainer);

        if (allCreatedCloseables != null) {
            for (RocksObject rocksCloseable : allCreatedCloseables) {
                verify(rocksCloseable, times(1)).close();
            }
            allCreatedCloseables = null;
        }
    }

    public void setupRocksKeyedStateBackend() throws Exception {

        blocker = new OneShotLatch();
        waiter = new OneShotLatch();
        testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
        testStreamFactory.setBlockerLatch(blocker);
        testStreamFactory.setWaiterLatch(waiter);
        testStreamFactory.setAfterNumberInvocations(10);
        prepareRocksDB();

        RocksDBKeyedStateBackendBuilder keyedStateBackendBuilder =
                RocksDBTestUtils.builderForTestDB(
                                TempDirUtils.newFolder(
                                        tempFolder), // this is not used anyways because the DB is
                                // injected
                                IntSerializer.INSTANCE,
                                spy(db),
                                defaultCFHandle,
                                optionsContainer.getColumnOptions())
                        .setEnableIncrementalCheckpointing(enableIncrementalCheckpointing);

        if (enableIncrementalCheckpointing) {
            rocksDBStateUploader =
                    spy(
                            new RocksDBStateUploader(
                                    RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue()));
            keyedStateBackendBuilder.setRocksDBStateUploader(rocksDBStateUploader);
        }

        keyedStateBackend = keyedStateBackendBuilder.build();

        testState1 =
                keyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("TestState-1", Integer.class, 0));

        testState2 =
                keyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("TestState-2", String.class, ""));

        allCreatedCloseables = new ArrayList<>();

        doAnswer(
                        new Answer<Object>() {

                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                RocksIterator rocksIterator =
                                        spy((RocksIterator) invocationOnMock.callRealMethod());
                                allCreatedCloseables.add(rocksIterator);
                                return rocksIterator;
                            }
                        })
                .when(keyedStateBackend.db)
                .newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class));

        doAnswer(
                        new Answer<Object>() {

                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                Snapshot snapshot =
                                        spy((Snapshot) invocationOnMock.callRealMethod());
                                allCreatedCloseables.add(snapshot);
                                return snapshot;
                            }
                        })
                .when(keyedStateBackend.db)
                .getSnapshot();

        doAnswer(
                        new Answer<Object>() {

                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                ColumnFamilyHandle snapshot =
                                        spy((ColumnFamilyHandle) invocationOnMock.callRealMethod());
                                allCreatedCloseables.add(snapshot);
                                return snapshot;
                            }
                        })
                .when(keyedStateBackend.db)
                .createColumnFamily(any(ColumnFamilyDescriptor.class));

        for (int i = 0; i < 100; ++i) {
            keyedStateBackend.setCurrentKey(i);
            testState1.update(4200 + i);
            testState2.update("S-" + (4200 + i));
        }
    }

    @TestTemplate
    public void testCorrectMergeOperatorSet() throws Exception {
        prepareRocksDB();
        final ColumnFamilyOptions columnFamilyOptions = spy(new ColumnFamilyOptions());
        RocksDBKeyedStateBackend<Integer> test = null;

        try {
            test =
                    RocksDBTestUtils.builderForTestDB(
                                    TempDirUtils.newFolder(tempFolder),
                                    IntSerializer.INSTANCE,
                                    db,
                                    defaultCFHandle,
                                    columnFamilyOptions)
                            .setEnableIncrementalCheckpointing(enableIncrementalCheckpointing)
                            .build();

            ValueStateDescriptor<String> stubState1 =
                    new ValueStateDescriptor<>("StubState-1", StringSerializer.INSTANCE);
            test.createOrUpdateInternalState(StringSerializer.INSTANCE, stubState1);
            ValueStateDescriptor<String> stubState2 =
                    new ValueStateDescriptor<>("StubState-2", StringSerializer.INSTANCE);
            test.createOrUpdateInternalState(StringSerializer.INSTANCE, stubState2);

            // The default CF is pre-created so sum up to 2 times (once for each stub state)
            verify(columnFamilyOptions, Mockito.times(2))
                    .setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);
        } finally {
            if (test != null) {
                IOUtils.closeQuietly(test);
                test.dispose();
            }
            columnFamilyOptions.close();
        }
    }

    @TestTemplate
    public void testReleasingSnapshotAfterBackendClosed() throws Exception {
        setupRocksKeyedStateBackend();

        try {
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    keyedStateBackend.snapshot(
                            0L,
                            0L,
                            testStreamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            RocksDB spyDB = keyedStateBackend.db;

            // Ensure every RocksObjects not closed yet
            for (RocksObject rocksCloseable : allCreatedCloseables) {
                verify(rocksCloseable, times(0)).close();
            }

            snapshot.cancel(true);

            this.keyedStateBackend.dispose();

            verify(spyDB, times(1)).close();
            assertThat(keyedStateBackend.isDisposed()).isTrue();

            // Ensure every RocksObjects was closed exactly once
            for (RocksObject rocksCloseable : allCreatedCloseables) {
                verify(rocksCloseable, times(1)).close();
            }

        } finally {
            keyedStateBackend.dispose();
            keyedStateBackend = null;
        }
        verifyRocksDBStateUploaderClosed();
    }

    @TestTemplate
    public void testDismissingSnapshot() throws Exception {
        setupRocksKeyedStateBackend();
        try {
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    keyedStateBackend.snapshot(
                            0L,
                            0L,
                            testStreamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            snapshot.cancel(true);
            verifyRocksObjectsReleased();
        } finally {
            this.keyedStateBackend.dispose();
            this.keyedStateBackend = null;
        }
        verifyRocksDBStateUploaderClosed();
    }

    @TestTemplate
    public void testDismissingSnapshotNotRunnable() throws Exception {
        setupRocksKeyedStateBackend();
        try {
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    keyedStateBackend.snapshot(
                            0L,
                            0L,
                            testStreamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            snapshot.cancel(true);
            Thread asyncSnapshotThread = new Thread(snapshot);
            asyncSnapshotThread.start();
            assertThatThrownBy(snapshot::get);
            asyncSnapshotThread.join();
            verifyRocksObjectsReleased();
        } finally {
            this.keyedStateBackend.dispose();
            this.keyedStateBackend = null;
        }
        verifyRocksDBStateUploaderClosed();
    }

    @TestTemplate
    public void testCompletingSnapshot() throws Exception {
        setupRocksKeyedStateBackend();
        try {
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    keyedStateBackend.snapshot(
                            0L,
                            0L,
                            testStreamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            Thread asyncSnapshotThread = new Thread(snapshot);
            asyncSnapshotThread.start();
            waiter.await(); // wait for snapshot to run
            waiter.reset();
            runStateUpdates();
            blocker.trigger(); // allow checkpointing to start writing
            waiter.await(); // wait for snapshot stream writing to run

            SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
            KeyedStateHandle keyedStateHandle = snapshotResult.getJobManagerOwnedSnapshot();
            assertThat(keyedStateHandle).isNotNull();
            assertThat(keyedStateHandle.getStateSize()).isGreaterThan(0);
            assertThat(keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups()).isEqualTo(2);

            for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
                assertThat(stream.isClosed()).isTrue();
            }

            asyncSnapshotThread.join();
            verifyRocksObjectsReleased();
        } finally {
            this.keyedStateBackend.dispose();
            this.keyedStateBackend = null;
        }
        verifyRocksDBStateUploaderClosed();
    }

    @TestTemplate
    public void testCancelRunningSnapshot() throws Exception {
        setupRocksKeyedStateBackend();
        try {
            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    keyedStateBackend.snapshot(
                            0L,
                            0L,
                            testStreamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            Thread asyncSnapshotThread = new Thread(snapshot);
            asyncSnapshotThread.start();
            waiter.await(); // wait for snapshot to run
            waiter.reset();
            runStateUpdates();
            snapshot.cancel(true);
            blocker.trigger(); // allow checkpointing to start writing

            for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
                assertThat(stream.isClosed()).isTrue();
            }

            waiter.await(); // wait for snapshot stream writing to run
            assertThatThrownBy(snapshot::get);

            asyncSnapshotThread.join();
            verifyRocksObjectsReleased();
        } finally {
            this.keyedStateBackend.dispose();
            this.keyedStateBackend = null;
        }
        verifyRocksDBStateUploaderClosed();
    }

    @TestTemplate
    public void testDisposeDeletesAllDirectories() throws Exception {
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        Collection<File> allFilesInDbDir =
                FileUtils.listFilesAndDirs(
                        new File(dbPath), new AcceptAllFilter(), new AcceptAllFilter());
        try {
            ValueStateDescriptor<String> kvId =
                    new ValueStateDescriptor<>("id", String.class, null);

            kvId.initializeSerializerUnlessSet(new ExecutionConfig());

            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

            backend.setCurrentKey(1);
            state.update("Hello");

            // more than just the root directory
            assertThat(allFilesInDbDir.size()).isGreaterThan(1);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
        allFilesInDbDir =
                FileUtils.listFilesAndDirs(
                        new File(dbPath), new AcceptAllFilter(), new AcceptAllFilter());

        // just the root directory left
        assertThat(allFilesInDbDir).hasSize(1);
    }

    @TestTemplate
    public void testSharedIncrementalStateDeRegistration() throws Exception {
        if (enableIncrementalCheckpointing) {
            CheckpointableKeyedStateBackend<Integer> backend =
                    createKeyedBackend(IntSerializer.INSTANCE);
            try {
                ValueStateDescriptor<String> kvId =
                        new ValueStateDescriptor<>("id", String.class, null);

                kvId.initializeSerializerUnlessSet(new ExecutionConfig());

                ValueState<String> state =
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

                Queue<IncrementalRemoteKeyedStateHandle> previousStateHandles = new LinkedList<>();
                SharedStateRegistry sharedStateRegistry = spy(new SharedStateRegistryImpl());
                for (int checkpointId = 0; checkpointId < 3; ++checkpointId) {

                    reset(sharedStateRegistry);

                    backend.setCurrentKey(checkpointId);
                    state.update("Hello-" + checkpointId);

                    RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                            backend.snapshot(
                                    checkpointId,
                                    checkpointId,
                                    createStreamFactory(),
                                    CheckpointOptions.forCheckpointWithDefaultLocation());

                    snapshot.run();

                    SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();

                    IncrementalRemoteKeyedStateHandle stateHandle =
                            (IncrementalRemoteKeyedStateHandle)
                                    snapshotResult.getJobManagerOwnedSnapshot();

                    // create new HandleAndLocalPath object for keeping handle before replacement
                    List<HandleAndLocalPath> sharedState =
                            stateHandle.getSharedState().stream()
                                    .map(
                                            e ->
                                                    HandleAndLocalPath.of(
                                                            e.getHandle(), e.getLocalPath()))
                                    .collect(Collectors.toList());

                    stateHandle.registerSharedStates(sharedStateRegistry, checkpointId);

                    for (HandleAndLocalPath handleAndLocalPath : sharedState) {
                        verify(sharedStateRegistry)
                                .registerReference(
                                        SharedStateRegistryKey.forStreamStateHandle(
                                                handleAndLocalPath.getHandle()),
                                        handleAndLocalPath.getHandle(),
                                        checkpointId);
                    }

                    previousStateHandles.add(stateHandle);
                    ((CheckpointListener) backend).notifyCheckpointComplete(checkpointId);

                    // -----------------------------------------------------------------

                    if (previousStateHandles.size() > 1) {
                        previousStateHandles.remove().discardState();
                    }
                }

                while (!previousStateHandles.isEmpty()) {

                    reset(sharedStateRegistry);

                    previousStateHandles.remove().discardState();
                }
            } finally {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
        }
    }

    @TestTemplate
    public void testMapStateClear() throws Exception {
        setupRocksKeyedStateBackend();
        MapStateDescriptor<Integer, String> kvId =
                new MapStateDescriptor<>("id", Integer.class, String.class);
        MapState<Integer, String> state =
                keyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

        doAnswer(
                        invocationOnMock -> {
                            throw new RocksDBException("Artificial failure");
                        })
                .when(keyedStateBackend.db)
                .newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class));

        assertThatThrownBy(state::clear).isInstanceOf(FlinkRuntimeException.class);
    }

    private void runStateUpdates() throws Exception {
        for (int i = 50; i < 150; ++i) {
            if (i % 10 == 0) {
                Thread.sleep(1);
            }
            keyedStateBackend.setCurrentKey(i);
            testState1.update(4200 + i);
            testState2.update("S-" + (4200 + i));
        }
    }

    private void verifyRocksObjectsReleased() {
        // Ensure every RocksObject was closed exactly once
        for (RocksObject rocksCloseable : allCreatedCloseables) {
            verify(rocksCloseable, times(1)).close();
        }

        assertThat(keyedStateBackend.db).isNotNull();
        RocksDB spyDB = keyedStateBackend.db;

        keyedStateBackend.dispose();
        verify(spyDB, times(1)).close();
        assertThat(keyedStateBackend.isDisposed()).isTrue();
    }

    private void verifyRocksDBStateUploaderClosed() {
        if (enableIncrementalCheckpointing) {
            verify(rocksDBStateUploader, times(1)).close();
        }
    }

    private static class AcceptAllFilter implements IOFileFilter {
        @Override
        public boolean accept(File file) {
            return true;
        }

        @Override
        public boolean accept(File file, String s) {
            return true;
        }
    }
}
