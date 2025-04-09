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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.forst.sync.ForStSyncKeyedStateBackend;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.state.forst.ForStConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
import static org.apache.flink.state.forst.ForStOptions.LOCAL_DIRECTORIES;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the partitioned state part of {@link ForStStateBackendTest}. */
@ExtendWith(ParameterizedTestExtension.class)
class ForStStateBackendTest extends StateBackendTestBase<ForStStateBackend> {
    @TempDir private static java.nio.file.Path tempFolder;

    @Parameters
    public static List<Object[]> modes() {
        return Arrays.asList(
                new Object[][] {
                    {
                        (SupplierWithException<CheckpointStorage, IOException>)
                                JobManagerCheckpointStorage::new
                    },
                    {
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

    @Parameter public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        return storageSupplier.get();
    }

    @Override
    protected ConfigurableStateBackend getStateBackend() throws Exception {
        ForStStateBackend backend = new ForStStateBackend();
        Configuration config = new Configuration();
        config.set(LOCAL_DIRECTORIES, tempFolder.toString());
        config.set(USE_INGEST_DB_RESTORE_MODE, true);
        return backend.configure(config, Thread.currentThread().getContextClassLoader());
    }

    @Override
    protected boolean isSerializerPresenceRequiredOnRestore() {
        return false;
    }

    @Override
    protected boolean supportsAsynchronousSnapshots() {
        return true;
    }

    /**
     * @return true if state backend is safe to reuse state.
     */
    @Override
    protected boolean isSafeToReuseKVState() {
        return true;
    }

    @TestTemplate
    void testConfiguration() throws Exception {
        ForStStateBackend backend = new ForStStateBackend();
        assertThat(backend.isIncrementalCheckpointsEnabled()).isFalse();
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        backend = backend.configure(config, Thread.currentThread().getContextClassLoader());
        assertThat(backend.isIncrementalCheckpointsEnabled()).isTrue();
    }

    @TestTemplate
    void testCreateKeyedStateBackend() throws Exception {
        Assumptions.assumeFalse(
                getCheckpointStorage() instanceof JobManagerCheckpointStorage,
                "Skip JM checkpoint storage");
        ForStStateBackend backend = new ForStStateBackend();
        ForStSyncKeyedStateBackend keyedStateBackend1 =
                (ForStSyncKeyedStateBackend) createKeyedBackend(IntSerializer.INSTANCE);
        assertThat(keyedStateBackend1.getOptionsContainer().getRemoteBasePath()).isNull();
        Configuration config = new Configuration();
        config.set(ForStOptions.SYNC_ENFORCE_LOCAL, false);
        backend = backend.configure(config, Thread.currentThread().getContextClassLoader());
        ForStSyncKeyedStateBackend keyedStateBackend2 =
                (ForStSyncKeyedStateBackend)
                        backend.createKeyedStateBackend(
                                new KeyedStateBackendParametersImpl<>(
                                        env,
                                        new JobID(),
                                        "test_op",
                                        IntSerializer.INSTANCE,
                                        10,
                                        KeyGroupRange.of(0, 9),
                                        env.getTaskKvStateRegistry(),
                                        TtlTimeProvider.DEFAULT,
                                        getMetricGroup(),
                                        getCustomInitializationMetrics(),
                                        Collections.emptyList(),
                                        new CloseableRegistry(),
                                        1.0d));
        assertThat(keyedStateBackend2.getOptionsContainer().getRemoteBasePath()).isNotNull();
    }
}
