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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.FileStateBackendTest;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/** Tests for {@link ChangelogStateBackend} delegating {@link FsStateBackend}. */
public class ChangelogDelegateFileStateBackendTest extends FileStateBackendTest {

    @Rule public final TemporaryFolder temp = new TemporaryFolder();

    @Override
    protected TestTaskStateManager getTestTaskStateManager() throws IOException {
        return ChangelogStateBackendTestUtils.createTaskStateManager(temp.newFolder());
    }

    @Override
    protected boolean snapshotUsesStreamFactory() {
        return false;
    }

    @Override
    protected boolean supportsMetaInfoVerification() {
        return false;
    }

    @Override
    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        return ChangelogStateBackendTestUtils.createKeyedBackend(
                new ChangelogStateBackend(super.getStateBackend()),
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                env);
    }

    @Override
    protected ConfigurableStateBackend getStateBackend() throws Exception {
        return new ChangelogStateBackend(super.getStateBackend());
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() {
        return new JobManagerCheckpointStorage();
    }
}
