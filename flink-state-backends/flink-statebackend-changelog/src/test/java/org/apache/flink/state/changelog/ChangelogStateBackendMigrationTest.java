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

package org.apache.flink.state.changelog;

import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackendMigrationTestBase;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/** Tests for the partitioned state part of {@link ChangelogStateBackend}. */
@ExtendWith(ParameterizedTestExtension.class)
public class ChangelogStateBackendMigrationTest
        extends StateBackendMigrationTestBase<ChangelogStateBackend> {

    @Parameters
    public static List<Supplier<AbstractStateBackend>> modes() {
        return Arrays.asList(
                HashMapStateBackend::new,
                () -> new EmbeddedRocksDBStateBackend(false),
                () -> new EmbeddedRocksDBStateBackend(true));
    }

    @Parameter public Supplier<AbstractStateBackend> delegatedStateBackendSupplier;

    @Override
    protected ChangelogStateBackend getStateBackend() throws Exception {
        return new ChangelogStateBackend(delegatedStateBackendSupplier.get());
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() {
        return new JobManagerCheckpointStorage();
    }

    @Override
    protected boolean supportsKeySerializerCheck() {
        // TODO support checking key serializer
        return false;
    }
}
