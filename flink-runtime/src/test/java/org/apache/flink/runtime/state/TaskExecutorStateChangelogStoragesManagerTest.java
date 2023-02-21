/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageLoader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredTaskManagerJobMetricGroup;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Tests for {@link TaskExecutorStateChangelogStoragesManager}. */
public class TaskExecutorStateChangelogStoragesManagerTest {

    @Test
    public void testDuplicatedAllocation() throws IOException {
        TaskExecutorStateChangelogStoragesManager manager =
                new TaskExecutorStateChangelogStoragesManager();
        Configuration configuration = new Configuration();
        JobID jobId1 = new JobID(1L, 1L);
        StateChangelogStorage<?> storage1 =
                manager.stateChangelogStorageForJob(
                        jobId1,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        StateChangelogStorage<?> storage2 =
                manager.stateChangelogStorageForJob(
                        jobId1,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertEquals(storage1, storage2);

        JobID jobId2 = new JobID(1L, 2L);
        StateChangelogStorage<?> storage3 =
                manager.stateChangelogStorageForJob(
                        jobId2,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertNotEquals(storage1, storage3);
        manager.shutdown();
    }

    @Test
    public void testReleaseForJob() throws IOException {
        StateChangelogStorageLoader.initialize(TestStateChangelogStorageFactory.pluginManager);
        TaskExecutorStateChangelogStoragesManager manager =
                new TaskExecutorStateChangelogStoragesManager();
        Configuration configuration = new Configuration();
        configuration.set(
                StateChangelogOptions.STATE_CHANGE_LOG_STORAGE,
                TestStateChangelogStorageFactory.identifier);
        JobID jobId1 = new JobID(1L, 1L);
        StateChangelogStorage<?> storage1 =
                manager.stateChangelogStorageForJob(
                        jobId1,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertTrue(storage1 instanceof TestStateChangelogStorage);
        Assert.assertFalse(((TestStateChangelogStorage) storage1).closed);
        manager.releaseResourcesForJob(jobId1);
        Assert.assertTrue(((TestStateChangelogStorage) storage1).closed);

        StateChangelogStorage<?> storage2 =
                manager.stateChangelogStorageForJob(
                        jobId1,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertNotEquals(storage1, storage2);

        manager.shutdown();
        StateChangelogStorageLoader.initialize(null);
    }

    @Test
    public void testConsistencyAmongTask() throws IOException {
        TaskExecutorStateChangelogStoragesManager manager =
                new TaskExecutorStateChangelogStoragesManager();
        Configuration configuration = new Configuration();
        configuration.set(StateChangelogOptions.STATE_CHANGE_LOG_STORAGE, "invalid");

        JobID jobId1 = new JobID(1L, 1L);
        StateChangelogStorage<?> storage1 =
                manager.stateChangelogStorageForJob(
                        jobId1,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertNull(storage1);

        // change configuration, assert the result not change.
        configuration.set(
                StateChangelogOptions.STATE_CHANGE_LOG_STORAGE,
                StateChangelogOptions.STATE_CHANGE_LOG_STORAGE.defaultValue());
        StateChangelogStorage<?> storage2 =
                manager.stateChangelogStorageForJob(
                        jobId1,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertNull(storage2);

        JobID jobId2 = new JobID(1L, 2L);
        StateChangelogStorage<?> storage3 =
                manager.stateChangelogStorageForJob(
                        jobId2,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertNotNull(storage3);

        configuration.set(StateChangelogOptions.STATE_CHANGE_LOG_STORAGE, "invalid");
        StateChangelogStorage<?> storage4 =
                manager.stateChangelogStorageForJob(
                        jobId2,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertNotNull(storage4);
        Assert.assertEquals(storage3, storage4);

        manager.shutdown();
    }

    @Test
    public void testShutdown() throws IOException {
        StateChangelogStorageLoader.initialize(TestStateChangelogStorageFactory.pluginManager);
        TaskExecutorStateChangelogStoragesManager manager =
                new TaskExecutorStateChangelogStoragesManager();
        Configuration configuration = new Configuration();
        configuration.set(
                StateChangelogOptions.STATE_CHANGE_LOG_STORAGE,
                TestStateChangelogStorageFactory.identifier);
        JobID jobId1 = new JobID(1L, 1L);
        StateChangelogStorage<?> storage1 =
                manager.stateChangelogStorageForJob(
                        jobId1,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertTrue(storage1 instanceof TestStateChangelogStorage);
        Assert.assertFalse(((TestStateChangelogStorage) storage1).closed);

        JobID jobId2 = new JobID(1L, 2L);
        StateChangelogStorage<?> storage2 =
                manager.stateChangelogStorageForJob(
                        jobId1,
                        configuration,
                        createUnregisteredTaskManagerJobMetricGroup(),
                        TestLocalRecoveryConfig.disabled());
        Assert.assertTrue(storage2 instanceof TestStateChangelogStorage);
        Assert.assertFalse(((TestStateChangelogStorage) storage2).closed);

        manager.shutdown();
        Assert.assertTrue(((TestStateChangelogStorage) storage1).closed);
        Assert.assertTrue(((TestStateChangelogStorage) storage2).closed);

        StateChangelogStorageLoader.initialize(null);
    }

    private static class TestStateChangelogStorage
            implements StateChangelogStorage<ChangelogStateHandle> {
        public boolean closed = false;

        @Override
        public StateChangelogWriter<ChangelogStateHandle> createWriter(
                String operatorID, KeyGroupRange keyGroupRange, MailboxExecutor mailboxExecutor) {
            return null;
        }

        @Override
        public StateChangelogHandleReader<ChangelogStateHandle> createReader() {
            return null;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class TestStateChangelogStorageFactory implements StateChangelogStorageFactory {

        public static String identifier = "test-factory";

        /** PluginManager that can load this factory. */
        public static PluginManager pluginManager =
                new PluginManager() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <P> Iterator<P> load(Class<P> service) {
                        checkArgument(service.equals(StateChangelogStorageFactory.class));
                        return (Iterator<P>)
                                singletonList(new TestStateChangelogStorageFactory()).iterator();
                    }
                };

        @Override
        public String getIdentifier() {
            // same identifier for overlapping test.
            return identifier;
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
