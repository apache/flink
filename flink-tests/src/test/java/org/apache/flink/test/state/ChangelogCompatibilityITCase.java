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

package org.apache.flink.test.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.forPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.flink.util.ExceptionUtils.findThrowableSerializedAware;
import static org.junit.Assert.fail;

/**
 * A test suite to check that restrictions on recovery with changelog enabled are enforced; and that
 * non-restricted scenarios are not blocked.
 */
@RunWith(Parameterized.class)
public class ChangelogCompatibilityITCase {

    private final TestCase testCase;

    @Parameterized.Parameters(name = "{0}")
    public static List<TestCase> parameters() {
        return Arrays.asList(
                // disable changelog - allow restore from CANONICAL_SAVEPOINT
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(false)
                        .from(RestoreSource.CANONICAL_SAVEPOINT)
                        .allowRestore(true),
                // disable changelog - allow restore from CHECKPOINT
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(false)
                        .from(RestoreSource.CHECKPOINT)
                        .allowRestore(true),
                // enable changelog - allow restore only from CANONICAL_SAVEPOINT
                TestCase.startWithChangelog(false)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.CANONICAL_SAVEPOINT)
                        .allowRestore(true),
                // enable recovery from  non-changelog checkpoints
                TestCase.startWithChangelog(false)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.CHECKPOINT)
                        .allowRestore(true),
                // normal cases: changelog enabled before and after recovery
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.CANONICAL_SAVEPOINT)
                        .allowRestore(true),
                // taking native savepoints is not supported with changelog
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.NATIVE_SAVEPOINT)
                        .allowSave(false)
                        .allowRestore(false),
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.CHECKPOINT)
                        .allowRestore(true));
    }

    @Test
    public void testRestore() throws Exception {
        runAndStoreIfAllowed().ifPresent(this::restoreAndValidate);
    }

    private Optional<String> runAndStoreIfAllowed() throws Exception {
        JobGraph initialGraph = addGraph(initEnvironment());
        try {
            String location = tryCheckpointAndStop(initialGraph);
            if (!testCase.allowStore) {
                fail(testCase.describeStore());
            }
            return Optional.of(location);
        } catch (Exception e) {
            if (testCase.allowStore) {
                throw e;
            } else {
                return Optional.empty();
            }
        }
    }

    private StreamExecutionEnvironment initEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableChangelogStateBackend(testCase.startWithChangelog);
        if (testCase.restoreSource == RestoreSource.CHECKPOINT) {
            env.enableCheckpointing(50);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        }
        return env;
    }

    private JobGraph addGraph(StreamExecutionEnvironment env) {
        env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE)
                .countWindowAll(37) // any stateful transformation suffices
                .reduce((ReduceFunction<Long>) Long::sum) // overflow is fine, result is discarded
                .sinkTo(new DiscardingSink<>());
        return env.getStreamGraph().getJobGraph();
    }

    private String tryCheckpointAndStop(JobGraph jobGraph) throws Exception {
        ClusterClient<?> client = miniClusterResource.getClusterClient();
        submit(jobGraph, client);
        if (testCase.restoreSource == RestoreSource.CHECKPOINT) {
            waitForCheckpoint(jobGraph.getJobID(), miniClusterResource.getMiniCluster(), 1);
            client.cancel(jobGraph.getJobID()).get();
            // obtain the latest checkpoint *after* cancellation - that one won't be subsumed
            return CommonTestUtils.getLatestCompletedCheckpointPath(
                            jobGraph.getJobID(), miniClusterResource.getMiniCluster())
                    .<NoSuchElementException>orElseThrow(
                            () -> {
                                throw new NoSuchElementException("No checkpoint was created yet");
                            });
        } else {
            return client.stopWithSavepoint(
                            jobGraph.getJobID(),
                            false,
                            pathToString(savepointDir),
                            testCase.restoreSource == RestoreSource.CANONICAL_SAVEPOINT
                                    ? SavepointFormatType.CANONICAL
                                    : SavepointFormatType.NATIVE)
                    .get();
        }
    }

    private void restoreAndValidate(String location) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableChangelogStateBackend(testCase.restoreWithChangelog);
        JobGraph jobGraph = addGraph(env);
        jobGraph.setSavepointRestoreSettings(forPath(location));

        if (tryRun(jobGraph) != testCase.allowRestore) {
            fail(testCase.describeRestore());
        }
    }

    private boolean tryRun(JobGraph jobGraph) {
        try {
            submit(jobGraph, miniClusterResource.getClusterClient());
            miniClusterResource.getClusterClient().cancel(jobGraph.getJobID()).get();
            return true;
        } catch (AssertionError | Exception e) { // AssertionError is thrown by CommonTestUtils
            if (isValidationError(e)) {
                return false;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean isValidationError(Throwable e) {
        return findThrowableSerializedAware(
                        e, IllegalStateException.class, this.getClass().getClassLoader())
                .filter(i -> i.getMessage().toLowerCase().contains("recovery not supported"))
                .isPresent();
    }

    private static final class TestCase {
        boolean startWithChangelog;
        boolean restoreWithChangelog;
        RestoreSource restoreSource;
        boolean allowStore = true;
        boolean allowRestore = false;

        public static TestCase startWithChangelog(boolean changelogEnabled) {
            TestCase testCase = new TestCase();
            testCase.startWithChangelog = changelogEnabled;
            return testCase;
        }

        public TestCase restoreWithChangelog(boolean restoreWithChangelog) {
            this.restoreWithChangelog = restoreWithChangelog;
            return this;
        }

        public TestCase from(RestoreSource restoreSource) {
            this.restoreSource = restoreSource;
            return this;
        }

        public TestCase allowRestore(boolean allowRestore) {
            this.allowRestore = allowRestore;
            return this;
        }

        public TestCase allowSave(boolean allowSave) {
            this.allowStore = allowSave;
            return this;
        }

        @Override
        public String toString() {
            return String.format(
                    "startWithChangelog=%s, restoreWithChangelog=%s, restoreFrom=%s, allowStore=%s, allowRestore=%s",
                    startWithChangelog,
                    restoreWithChangelog,
                    restoreSource,
                    allowStore,
                    allowRestore);
        }

        private String describeStore() {
            return String.format(
                    "taking %s with changelog.enabled=%b should be %s",
                    restoreSource, startWithChangelog, allowStore ? "allowed" : "disallowed");
        }

        private String describeRestore() {
            return String.format(
                    "restoring from %s taken with changelog.enabled=%b should be %s with changelog.enabled=%b",
                    restoreSource,
                    allowRestore ? "allowed" : "disallowed",
                    startWithChangelog,
                    restoreWithChangelog);
        }
    }

    private enum RestoreSource {
        CANONICAL_SAVEPOINT,
        NATIVE_SAVEPOINT,
        CHECKPOINT
    }

    private void submit(JobGraph jobGraph, ClusterClient<?> client) throws Exception {
        client.submitJob(jobGraph).get();
        waitForAllTaskRunning(miniClusterResource.getMiniCluster(), jobGraph.getJobID(), true);
    }

    private static String pathToString(File path) {
        return path.toURI().toString();
    }

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
    private File checkpointDir;
    private File savepointDir;
    private MiniClusterWithClientResource miniClusterResource;

    public ChangelogCompatibilityITCase(TestCase testCase) {
        this.testCase = testCase;
    }

    @Before
    public void before() throws Exception {
        checkpointDir = TEMPORARY_FOLDER.newFolder();
        savepointDir = TEMPORARY_FOLDER.newFolder();
        Configuration config = new Configuration();
        config.setString(CHECKPOINTS_DIRECTORY, pathToString(checkpointDir));
        config.setString(SAVEPOINT_DIRECTORY, pathToString(savepointDir));
        FsStateChangelogStorageFactory.configure(
                config, TEMPORARY_FOLDER.newFolder(), Duration.ofMinutes(1), 10);
        miniClusterResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(11)
                                .setNumberSlotsPerTaskManager(1)
                                .build());
        miniClusterResource.before();
    }

    @After
    public void after() {
        if (miniClusterResource != null) {
            miniClusterResource.after();
        }
    }
}
