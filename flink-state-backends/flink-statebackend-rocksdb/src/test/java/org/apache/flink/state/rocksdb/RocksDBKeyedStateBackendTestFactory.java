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

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TernaryBoolean;

import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.mock;

/** External resource for tests that require an instance of RocksDBKeyedStateBackend. */
public class RocksDBKeyedStateBackendTestFactory implements AutoCloseable {

    private MockEnvironment env;

    private RocksDBKeyedStateBackend<?> keyedStateBackend;

    public <K> RocksDBKeyedStateBackend<K> create(
            TemporaryFolder tmp, TypeSerializer<K> keySerializer, int maxKeyGroupNumber)
            throws Exception {
        EmbeddedRocksDBStateBackend backend = getRocksDBStateBackend(tmp);
        env = MockEnvironment.builder().build();
        JobID jobID = new JobID();
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, maxKeyGroupNumber - 1);
        TaskKvStateRegistry kvStateRegistry = mock(TaskKvStateRegistry.class);
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        keyedStateBackend =
                (RocksDBKeyedStateBackend<K>)
                        backend.createKeyedStateBackend(
                                new KeyedStateBackendParametersImpl<>(
                                        env,
                                        jobID,
                                        "Test",
                                        keySerializer,
                                        maxKeyGroupNumber,
                                        keyGroupRange,
                                        kvStateRegistry,
                                        TtlTimeProvider.DEFAULT,
                                        new UnregisteredMetricsGroup(),
                                        Collections.emptyList(),
                                        cancelStreamRegistry));

        return (RocksDBKeyedStateBackend<K>) keyedStateBackend;
    }

    @Override
    public void close() {
        if (keyedStateBackend != null) {
            keyedStateBackend.dispose();
        }

        IOUtils.closeQuietly(env);
    }

    private EmbeddedRocksDBStateBackend getRocksDBStateBackend(TemporaryFolder tmp)
            throws IOException {
        String dbPath = tmp.newFolder().getAbsolutePath();
        String checkpointPath = tmp.newFolder().toURI().toString();
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(TernaryBoolean.TRUE);
        backend.setDbStoragePath(dbPath);
        return backend;
    }
}
