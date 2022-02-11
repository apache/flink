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
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.forPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.flink.test.util.TestUtils.getMostRecentCompletedCheckpointMaybe;
import static org.apache.flink.util.ExceptionUtils.findThrowableSerializedAware;
import static org.junit.Assert.fail;

/**
 * A test suite to check that restrictions on recovery with changelog enabled are enforced; and that
 * non-restricted scenarios are not blocked. In particular, recovery from non-changelog checkpoints
 * should not be allowed (see <a
 * href="https://issues.apache.org/jira/browse/FLINK-26079">FLINK-26079</a>).
 */
@RunWith(Parameterized.class)
public class ChangelogCompatibilityITCase {

    private final TestCase testCase;

    @Parameterized.Parameters(name = "{0}")
    public static List<TestCase> parameters() {
        return Arrays.asList(
                // disable changelog - allow restore only from CANONICAL_SAVEPOINT
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(false)
                        .from(RestoreSource.CANONICAL_SAVEPOINT)
                        .allowRestore(true),
                // enable changelog - allow restore only from CANONICAL_SAVEPOINT
                TestCase.startWithChangelog(false)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.CANONICAL_SAVEPOINT)
                        .allowRestore(true),
                // explicitly disallow recovery from  non-changelog checkpoints
                // https://issues.apache.org/jira/browse/FLINK-26079
                TestCase.startWithChangelog(false)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.CHECKPOINT)
                        .allowRestore(false),
                // normal cases: changelog enabled before and after recovery
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.CANONICAL_SAVEPOINT)
                        .allowRestore(true),
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.NATIVE_SAVEPOINT)
                        .allowRestore(true),
                TestCase.startWithChangelog(true)
                        .restoreWithChangelog(true)
                        .from(RestoreSource.CHECKPOINT)
                        .allowRestore(true));
    }

    @Test
    public void testRestore() throws Exception {
        JobGraph initialGraph = addGraph(initEnvironment());
        String restoreSourceLocation = checkpointAndStop(initialGraph);

        restoreAndValidate(restoreSourceLocation);
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
                .addSink(new DiscardingSink<>());
        return env.getStreamGraph().getJobGraph();
    }

    private String checkpointAndStop(JobGraph jobGraph) throws Exception {
        ClusterClient<?> client = miniClusterResource.getClusterClient();
        submit(jobGraph, client);
        if (testCase.restoreSource == RestoreSource.CHECKPOINT) {
            String location = pathToString(waitForCheckpoint());
            client.cancel(jobGraph.getJobID()).get();
            return location;
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

    private void restoreAndValidate(String location) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableChangelogStateBackend(testCase.restoreWithChangelog);
        JobGraph jobGraph = addGraph(env);
        jobGraph.setSavepointRestoreSettings(forPath(location));

        boolean restored = tryRun(jobGraph);

        if (restored && !testCase.allowRestore) {
            fail(
                    String.format(
                            "restoring from %s taken with changelog.enabled=%b should NOT be allowed with changelog.enabled=%b",
                            testCase.restoreSource,
                            testCase.startWithChangelog,
                            testCase.restoreWithChangelog));
        } else if (!restored && testCase.allowRestore) {
            fail(
                    String.format(
                            "restoring from %s taken with changelog.enabled=%b should be allowed with changelog.enabled=%b",
                            testCase.restoreSource,
                            testCase.startWithChangelog,
                            testCase.restoreWithChangelog));
        }
    }

    private boolean tryRun(JobGraph jobGraph) throws Exception {
        try {
            submit(jobGraph, miniClusterResource.getClusterClient());
            miniClusterResource.getClusterClient().cancel(jobGraph.getJobID()).get();
            return true;
        } catch (AssertionError | Exception e) { // AssertionError is thrown by CommonTestUtils
            if (isValidationError(e)) {
                return false;
            } else {
                throw e;
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
        boolean allowRestore;

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

        @Override
        public String toString() {
            return String.format(
                    "startWithChangelog=%s, restoreWithChangelog=%s, restoreFrom=%s, allowRestore=%s",
                    startWithChangelog, restoreWithChangelog, restoreSource, allowRestore);
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

    private File waitForCheckpoint() throws IOException, InterruptedException {
        Optional<File> location = getMostRecentCompletedCheckpointMaybe(checkpointDir);
        while (!location.isPresent()) {
            Thread.sleep(50);
            location = getMostRecentCompletedCheckpointMaybe(checkpointDir);
        }
        return location.get();
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
        FsStateChangelogStorageFactory.configure(config, TEMPORARY_FOLDER.newFolder());
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
