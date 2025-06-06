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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.v2.StateBackendTestV2Base;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.configuration.StateRecoveryOptions.RESTORE_MODE;
import static org.apache.flink.state.forst.ForStConfigurableOptions.USE_DELETE_FILES_IN_RANGE_DURING_RESCALING;
import static org.apache.flink.state.forst.ForStConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
import static org.apache.flink.state.forst.ForStOptions.LOCAL_DIRECTORIES;
import static org.apache.flink.state.forst.ForStOptions.PRIMARY_DIRECTORY;
import static org.apache.flink.state.forst.ForStStateBackend.CHECKPOINT_DIR_AS_PRIMARY_SHORTCUT;
import static org.apache.flink.state.forst.ForStStateBackend.LOCAL_DIR_AS_PRIMARY_SHORTCUT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for the async keyed state backend part of {@link ForStStateBackend}. */
@ExtendWith(ParameterizedTestExtension.class)
class ForStStateBackendV2Test extends StateBackendTestV2Base<ForStStateBackend> {

    enum RestoreMode {
        FAILOVER,
        MANUALLY_RESTORE_CLAIM,
        MANUALLY_RESTORE_NO_CLAIM,
    }

    @TempDir private static java.nio.file.Path tempFolder;
    @TempDir private static java.nio.file.Path tempFolderForForStLocal;
    @TempDir private static java.nio.file.Path tempFolderForForstRemote;

    private static final SupplierWithException<CheckpointStorage, IOException>
            jobManagerCheckpointStorage = JobManagerCheckpointStorage::new;

    private static final SupplierWithException<CheckpointStorage, IOException>
            filesystemCheckpointStorage =
                    () -> {
                        String checkpointPath =
                                TempDirUtils.newFolder(tempFolder).toURI().toString();
                        return new FileSystemCheckpointStorage(new Path(checkpointPath), 0, -1);
                    };

    private static final SupplierWithException<CheckpointStorage, IOException>
            dbReusableCheckpointStorage =
                    () -> {
                        String checkpointPath = "file://" + tempFolderForForstRemote.toString();
                        return new FileSystemCheckpointStorage(new Path(checkpointPath), 0, -1);
                    };

    private static List<Object[]> addAllCasesForParameter(
            List<Object[]> baseCases, Object[] parameterCases) {
        List<Object[]> newCases = new ArrayList<>();
        for (Object[] baseCase : baseCases) {
            for (Object param : parameterCases) {
                Object[] newCase = Arrays.copyOf(baseCase, baseCase.length + 1);
                newCase[baseCase.length] = param;
                newCases.add(newCase);
            }
        }
        return newCases;
    }

    @Parameters(
            name =
                    " hasLocalDir: {0},"
                            + " hasRemoteDir: {1},"
                            + " useIngestDbRestoreMode: {2},"
                            + " useDeleteFileInRange: {3},"
                            + " CheckpointStorage: {4}, "
                            + " restoreMode: {5}")
    public static List<Object[]> modes() {
        // ---------- base cases ----------
        // combination of parameters: [hasLocalDir, hasRemoteDir, useIngestDbRestoreMode,
        // useDeleteFileInRange]
        List<Object[]> cases =
                Arrays.asList(
                        new Object[][] {
                            {true, false, true, false},
                            {false, true, false, true},
                            {false, true, true, true},
                            {true, true, true, true},
                            {true, true, false, false},
                        });

        // ---------- enumerate different cases for CheckpointStorage ----------
        cases =
                addAllCasesForParameter(
                        cases,
                        new Object[] {filesystemCheckpointStorage, dbReusableCheckpointStorage});
        // a special case for jobManagerCheckpointStorage
        cases.add(new Object[] {true, false, false, false, jobManagerCheckpointStorage});

        // ---------- enumerate different cases for restoreMode ----------
        cases = addAllCasesForParameter(cases, RestoreMode.values());

        return cases;
    }

    @Parameter public boolean hasLocalDir;

    @Parameter(1)
    public boolean hasRemoteDir;

    @Parameter(2)
    public boolean useIngestDbRestoreMode;

    @Parameter(3)
    public boolean useDeleteFileInRange;

    @Parameter(4)
    public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    @Parameter(5)
    public RestoreMode restoreMode;

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        return storageSupplier.get();
    }

    @Override
    protected ConfigurableStateBackend getStateBackend() throws Exception {
        ForStStateBackend backend = new ForStStateBackend();
        Configuration config = new Configuration();
        if (hasLocalDir) {
            config.set(LOCAL_DIRECTORIES, tempFolderForForStLocal.toString());
        }
        if (hasRemoteDir) {
            config.set(PRIMARY_DIRECTORY, tempFolderForForstRemote.toString());
        } else {
            config.set(PRIMARY_DIRECTORY, LOCAL_DIR_AS_PRIMARY_SHORTCUT);
        }
        config.set(USE_INGEST_DB_RESTORE_MODE, useIngestDbRestoreMode);
        config.set(USE_DELETE_FILES_IN_RANGE_DURING_RESCALING, useDeleteFileInRange);
        config.set(
                RESTORE_MODE,
                restoreMode == RestoreMode.MANUALLY_RESTORE_NO_CLAIM
                        ? RecoveryClaimMode.NO_CLAIM
                        : RecoveryClaimMode.CLAIM);
        return backend.configure(config, Thread.currentThread().getContextClassLoader());
    }

    @Override
    protected void restoreJob() throws Exception {
        if (restoreMode != RestoreMode.FAILOVER) {
            jobID = new JobID();
            env = buildMockEnv();
        }
    }

    @TestTemplate
    void testRemoteDirShareCheckpointDirWithJobId() throws Exception {
        testRemoteDirShareCheckpointDir(true);
    }

    @TestTemplate
    void testRemoteDirShareCheckpointDirWOJob() throws Exception {
        testRemoteDirShareCheckpointDir(false);
    }

    void testRemoteDirShareCheckpointDir(boolean createJob) throws Exception {
        JobID jobID = new JobID();
        String checkpointPath = TempDirUtils.newFolder(tempFolder).toURI().toString();
        FileSystemCheckpointStorage checkpointStorage =
                new FileSystemCheckpointStorage(new Path(checkpointPath), 0, -1);

        Configuration config = new Configuration();
        config.set(LOCAL_DIRECTORIES, tempFolderForForStLocal.toString());
        config.set(PRIMARY_DIRECTORY, CHECKPOINT_DIR_AS_PRIMARY_SHORTCUT);
        config.set(CheckpointingOptions.CREATE_CHECKPOINT_SUB_DIR, createJob);

        checkpointStorage =
                checkpointStorage.configure(config, Thread.currentThread().getContextClassLoader());
        MockEnvironment mockEnvironment =
                MockEnvironment.builder().setTaskStateManager(getTestTaskStateManager()).build();
        mockEnvironment.setCheckpointStorageAccess(
                checkpointStorage.createCheckpointStorage(jobID));

        ForStStateBackend backend = new ForStStateBackend();
        backend = backend.configure(config, Thread.currentThread().getContextClassLoader());
        KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 127);
        ForStKeyedStateBackend<Integer> keyedBackend =
                backend.createAsyncKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                mockEnvironment,
                                jobID,
                                "test_op",
                                IntSerializer.INSTANCE,
                                keyGroupRange.getNumberOfKeyGroups(),
                                keyGroupRange,
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                getMetricGroup(),
                                getCustomInitializationMetrics(),
                                Collections.emptyList(),
                                new CloseableRegistry(),
                                1.0d));

        assertThat(keyedBackend.getRemoteBasePath().getParent())
                .isEqualTo(
                        new Path(
                                checkpointStorage.getCheckpointPath(),
                                createJob ? jobID + "/shared" : "/shared"));
    }
}
