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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/** Test utils for the RocksDB state backend. */
public final class RocksDBTestUtils {

    public static <K> RocksDBKeyedStateBackendBuilder<K> builderForTestDefaults(
            File instanceBasePath, TypeSerializer<K> keySerializer) {

        return builderForTestDefaults(
                instanceBasePath,
                keySerializer,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);
    }

    public static <K> RocksDBKeyedStateBackendBuilder<K> builderForTestDefaults(
            File instanceBasePath,
            TypeSerializer<K> keySerializer,
            EmbeddedRocksDBStateBackend.PriorityQueueStateType queueStateType) {

        final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();

        return new RocksDBKeyedStateBackendBuilder<>(
                "no-op",
                ClassLoader.getSystemClassLoader(),
                instanceBasePath,
                optionsContainer,
                stateName -> optionsContainer.getColumnOptions(),
                new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
                keySerializer,
                2,
                new KeyGroupRange(0, 1),
                new ExecutionConfig(),
                TestLocalRecoveryConfig.disabled(),
                queueStateType,
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                UncompressedStreamCompressionDecorator.INSTANCE,
                new CloseableRegistry());
    }

    public static <K> RocksDBKeyedStateBackendBuilder<K> builderForTestDB(
            File instanceBasePath,
            TypeSerializer<K> keySerializer,
            RocksDB db,
            ColumnFamilyHandle defaultCFHandle,
            ColumnFamilyOptions columnFamilyOptions) {

        final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();

        return new RocksDBKeyedStateBackendBuilder<>(
                "no-op",
                ClassLoader.getSystemClassLoader(),
                instanceBasePath,
                optionsContainer,
                stateName -> columnFamilyOptions,
                new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
                keySerializer,
                2,
                new KeyGroupRange(0, 1),
                new ExecutionConfig(),
                TestLocalRecoveryConfig.disabled(),
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP,
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                UncompressedStreamCompressionDecorator.INSTANCE,
                db,
                defaultCFHandle,
                new CloseableRegistry());
    }

    public static <K> RocksDBKeyedStateBackend<K> createKeyedStateBackend(
            EmbeddedRocksDBStateBackend rocksDbBackend,
            Environment env,
            TypeSerializer<K> keySerializer)
            throws IOException {

        return (RocksDBKeyedStateBackend<K>)
                rocksDbBackend.createKeyedStateBackend(
                        env,
                        env.getJobID(),
                        "test_op",
                        keySerializer,
                        1,
                        new KeyGroupRange(0, 0),
                        env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        new CloseableRegistry());
    }
}
