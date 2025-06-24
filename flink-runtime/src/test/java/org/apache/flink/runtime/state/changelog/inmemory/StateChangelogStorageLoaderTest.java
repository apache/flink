/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog.inmemory;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageLoader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredTaskManagerJobMetricGroup;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StateChangelogStorageLoader}. */
class StateChangelogStorageLoaderTest {

    @Test
    void testLoadSpiImplementation() throws IOException {
        StateChangelogStorageLoader.initialize(getPluginManager(emptyIterator()));
        assertThat(
                        StateChangelogStorageLoader.load(
                                JobID.generate(),
                                new Configuration(),
                                createUnregisteredTaskManagerJobMetricGroup(),
                                TestLocalRecoveryConfig.disabled()))
                .isNotNull();
    }

    @Test
    void testLoadNotExist() throws IOException {
        StateChangelogStorageLoader.initialize(getPluginManager(emptyIterator()));
        assertThat(
                        StateChangelogStorageLoader.load(
                                JobID.generate(),
                                new Configuration()
                                        .set(
                                                StateChangelogOptions.STATE_CHANGE_LOG_STORAGE,
                                                "not_exist"),
                                createUnregisteredTaskManagerJobMetricGroup(),
                                TestLocalRecoveryConfig.disabled()))
                .isNull();
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testLoadPluginImplementation() throws IOException {
        StateChangelogStorageFactory factory = new TestStateChangelogStorageFactory();
        PluginManager pluginManager = getPluginManager(singletonList(factory).iterator());
        StateChangelogStorageLoader.initialize(pluginManager);
        StateChangelogStorage loaded =
                StateChangelogStorageLoader.load(
                        JobID.generate(),
                        new Configuration(),
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        assertThat(loaded).isInstanceOf(TestStateChangelogStorage.class);
    }

    private PluginManager getPluginManager(
            Iterator<? extends StateChangelogStorageFactory> iterator) {
        return new PluginManager() {

            @Override
            public <P> Iterator<P> load(Class<P> service) {
                checkArgument(service.equals(StateChangelogStorageFactory.class));
                //noinspection unchecked
                return (Iterator<P>) iterator;
            }
        };
    }

    private static class TestStateChangelogStorage
            implements StateChangelogStorage<ChangelogStateHandle> {
        @Override
        public StateChangelogWriter<ChangelogStateHandle> createWriter(
                String operatorID, KeyGroupRange keyGroupRange, MailboxExecutor mailboxExecutor) {
            return null;
        }

        @Override
        public StateChangelogHandleReader<ChangelogStateHandle> createReader() {
            return null;
        }
    }

    private static class TestStateChangelogStorageFactory implements StateChangelogStorageFactory {

        @Override
        public String getIdentifier() {
            // same identifier for overlapping test.
            return InMemoryStateChangelogStorageFactory.IDENTIFIER;
        }

        @Override
        public StateChangelogStorage<?> createStorage(
                JobID jobID,
                Configuration configuration,
                TaskManagerJobMetricGroup metricGroup,
                LocalRecoveryConfig localRecoveryConfig) {
            return new TestStateChangelogStorage();
        }

        @Override
        public StateChangelogStorageView<?> createStorageView(Configuration configuration)
                throws IOException {
            return new TestStateChangelogStorage();
        }
    }
}
