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

package org.apache.flink.test.classloading;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.BASE_PATH;
import static org.apache.flink.changelog.fs.FsStateChangelogStorageFactory.IDENTIFIER;
import static org.apache.flink.configuration.StateChangelogOptions.STATE_CHANGE_LOG_STORAGE;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test job classloader. */
@ExtendWith(TestLoggerExtension.class)
class ClassLoaderITCase {

    private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderITCase.class);

    private static final String STREAMING_INPUT_SPLITS_PROG_JAR_FILE =
            "target/streaming-customsplit-test-jar.jar";

    private static final String STREAMING_PROG_JAR_FILE =
            "target/streamingclassloader-test-jar.jar";

    private static final String STREAMING_CHECKPOINTED_PROG_JAR_FILE =
            "target/streaming-checkpointed-classloader-test-jar.jar";

    private static final String USERCODETYPE_JAR_PATH = "target/usercodetype-test-jar.jar";

    private static final String CUSTOM_KV_STATE_JAR_PATH = "target/custom_kv_state-test-jar.jar";

    private static final String CHECKPOINTING_CUSTOM_KV_STATE_JAR_PATH =
            "target/checkpointing_custom_kv_state-test-jar.jar";

    private static final String CLASSLOADING_POLICY_JAR_PATH =
            "target/classloading_policy-test-jar.jar";

    @TempDir static java.nio.file.Path folder;

    private static MiniClusterResource miniClusterResource = null;

    private static final int parallelism = 4;

    @BeforeAll
    static void setUp() throws Exception {
        Configuration config = new Configuration();

        // we need to use the "filesystem" state backend to ensure FLINK-2543 is not happening
        // again.
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                new Path(TempDirUtils.newFolder(folder).toURI()).toString());

        // Savepoint path
        config.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY,
                new Path(TempDirUtils.newFolder(folder).toURI()).toString());
        // required as we otherwise run out of memory
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("80m"));

        // If changelog backend is enabled then this test might run too slow with in-memory
        // implementation - use fs-based instead.
        // The randomization currently happens on the job level (environment); while this factory
        // can only be set on the cluster level; so we do it unconditionally here.
        config.set(STATE_CHANGE_LOG_STORAGE, IDENTIFIER);
        config.set(BASE_PATH, new Path(TempDirUtils.newFolder(folder).toURI()).toString());

        // some tests check for serialization problems related to class-loading
        // this requires all RPCs to actually go through serialization
        config.set(RpcOptions.FORCE_RPC_INVOCATION_SERIALIZATION, true);

        miniClusterResource =
                new MiniClusterResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .setConfiguration(config)
                                .build());

        miniClusterResource.before();
    }

    @AfterAll
    static void tearDownClass() {
        if (miniClusterResource != null) {
            miniClusterResource.after();
        }
    }

    @AfterEach
    void tearDown() {
        TestStreamEnvironment.unsetAsContext();
    }

    @Test
    void testStreamingCustomSplitJobWithCustomClassLoader() throws ProgramInvocationException {
        PackagedProgram streamingInputSplitTestProg =
                PackagedProgram.newBuilder()
                        .setJarFile(new File(STREAMING_INPUT_SPLITS_PROG_JAR_FILE))
                        .build();

        TestStreamEnvironment.setAsContext(
                miniClusterResource.getMiniCluster(),
                parallelism,
                Collections.singleton(new Path(STREAMING_INPUT_SPLITS_PROG_JAR_FILE)),
                Collections.emptyList());

        streamingInputSplitTestProg.invokeInteractiveModeForExecution();
    }

    @Test
    void testStreamingClassloaderJobWithCustomClassLoader() throws ProgramInvocationException {
        // regular streaming job
        PackagedProgram streamingProg =
                PackagedProgram.newBuilder().setJarFile(new File(STREAMING_PROG_JAR_FILE)).build();

        TestStreamEnvironment.setAsContext(
                miniClusterResource.getMiniCluster(),
                parallelism,
                Collections.singleton(new Path(STREAMING_PROG_JAR_FILE)),
                Collections.emptyList());

        streamingProg.invokeInteractiveModeForExecution();
    }

    @Test
    void testCheckpointedStreamingClassloaderJobWithCustomClassLoader()
            throws ProgramInvocationException {
        // checkpointed streaming job with custom classes for the checkpoint (FLINK-2543)
        // the test also ensures that user specific exceptions are serializable between JobManager
        // <--> JobClient.
        PackagedProgram streamingCheckpointedProg =
                PackagedProgram.newBuilder()
                        .setJarFile(new File(STREAMING_CHECKPOINTED_PROG_JAR_FILE))
                        .build();

        TestStreamEnvironment.setAsContext(
                miniClusterResource.getMiniCluster(),
                parallelism,
                Collections.singleton(new Path(STREAMING_CHECKPOINTED_PROG_JAR_FILE)),
                Collections.emptyList());

        // sanity check that the exception from the user-jar is not on the classpath
        assertThatThrownBy(
                        () ->
                                Class.forName(
                                        "org.apache.flink.test.classloading.jar.CheckpointedStreamingProgram$SuccessException"))
                .isInstanceOf(ClassNotFoundException.class);

        // Program should terminate with a 'SuccessException'
        // the exception should be contained in a SerializedThrowable, which failed to deserialize
        // the original exception because it is only contained in the user-jar
        assertThatThrownBy(streamingCheckpointedProg::invokeInteractiveModeForExecution)
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SerializedThrowable.class,
                                "org.apache.flink.test.classloading.jar.CheckpointedStreamingProgram$SuccessException"));
    }

    @Test
    void testUserCodeTypeJobWithCustomClassLoader() throws ProgramInvocationException {
        PackagedProgram userCodeTypeProg =
                PackagedProgram.newBuilder().setJarFile(new File(USERCODETYPE_JAR_PATH)).build();

        TestStreamEnvironment.setAsContext(
                miniClusterResource.getMiniCluster(),
                parallelism,
                Collections.singleton(new Path(USERCODETYPE_JAR_PATH)),
                Collections.emptyList());

        userCodeTypeProg.invokeInteractiveModeForExecution();
    }

    @Test
    void testCheckpointingCustomKvStateJobWithCustomClassLoader()
            throws IOException, ProgramInvocationException {
        File checkpointDir = TempDirUtils.newFolder(folder);
        File outputDir = TempDirUtils.newFolder(folder);

        final PackagedProgram program =
                PackagedProgram.newBuilder()
                        .setJarFile(new File(CHECKPOINTING_CUSTOM_KV_STATE_JAR_PATH))
                        .setArguments(
                                new String[] {
                                    checkpointDir.toURI().toString(), outputDir.toURI().toString()
                                })
                        .build();

        TestStreamEnvironment.setAsContext(
                miniClusterResource.getMiniCluster(),
                parallelism,
                Collections.singleton(new Path(CHECKPOINTING_CUSTOM_KV_STATE_JAR_PATH)),
                Collections.emptyList());

        assertThatThrownBy(program::invokeInteractiveModeForExecution)
                .isInstanceOf(ProgramInvocationException.class)
                .hasRootCauseInstanceOf(SuccessException.class);
    }

    /** Tests disposal of a savepoint, which contains custom user code KvState. */
    @Test
    void testDisposeSavepointWithCustomKvState() throws Exception {
        ClusterClient<?> clusterClient =
                new MiniClusterClient(new Configuration(), miniClusterResource.getMiniCluster());

        Deadline deadline = new FiniteDuration(100, TimeUnit.SECONDS).fromNow();

        File checkpointDir = TempDirUtils.newFolder(folder);
        File outputDir = TempDirUtils.newFolder(folder);

        final PackagedProgram program =
                PackagedProgram.newBuilder()
                        .setJarFile(new File(CUSTOM_KV_STATE_JAR_PATH))
                        .setArguments(
                                new String[] {
                                    String.valueOf(parallelism),
                                    checkpointDir.toURI().toString(),
                                    "5000",
                                    outputDir.toURI().toString(),
                                    "false" // Disable unaligned checkpoints as this test is
                                    // triggering concurrent savepoints/checkpoints
                                })
                        .build();

        TestStreamEnvironment.setAsContext(
                miniClusterResource.getMiniCluster(),
                parallelism,
                Collections.singleton(new Path(CUSTOM_KV_STATE_JAR_PATH)),
                Collections.emptyList());

        // Execute detached
        Thread invokeThread =
                new Thread(
                        () -> {
                            try {
                                program.invokeInteractiveModeForExecution();
                            } catch (ProgramInvocationException ex) {
                                if (ex.getCause() == null
                                        || !(ex.getCause() instanceof JobCancellationException)) {
                                    ex.printStackTrace();
                                }
                            }
                        });

        LOG.info("Starting program invoke thread");
        invokeThread.start();

        LOG.info("Waiting for job status running.");

        // Wait for running job
        waitUntilCondition(
                () ->
                        clusterClient.listJobs().get().stream()
                                .anyMatch(job -> job.getJobState() == JobStatus.RUNNING),
                20);
        JobID jobId =
                clusterClient.listJobs().get().stream()
                        .findFirst()
                        .map(JobStatusMessage::getJobId)
                        .orElseThrow();

        // Trigger savepoint
        waitForAllTaskRunning(miniClusterResource.getMiniCluster(), jobId, false);
        String savepointPath =
                clusterClient
                        .triggerSavepoint(jobId, null, SavepointFormatType.CANONICAL)
                        .get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

        clusterClient.disposeSavepoint(savepointPath).get();

        clusterClient.cancel(jobId).get();

        // make sure, the execution is finished to not influence other test methods
        invokeThread.join(deadline.timeLeft().toMillis());
        assertThat(invokeThread.isAlive()).as("Program invoke thread still running").isFalse();
    }

    @Test
    void testProgramWithChildFirstClassLoader() throws IOException, ProgramInvocationException {
        // We have two files named test-resource in src/resource (parent classloader classpath) and
        // tmp folders (child classloader classpath) respectively.
        String childResourceDirName = "child0";
        String testResourceName = "test-resource";
        File childResourceDir = TempDirUtils.newFolder(folder, childResourceDirName);
        File childResource = new File(childResourceDir, testResourceName);
        assertThat(childResource.createNewFile()).isTrue();

        TestStreamEnvironment.setAsContext(
                miniClusterResource.getMiniCluster(),
                parallelism,
                Collections.singleton(new Path(CLASSLOADING_POLICY_JAR_PATH)),
                Collections.emptyList());

        // child-first classloading
        Configuration childFirstConf = new Configuration();
        childFirstConf.setString("classloader.resolve-order", "child-first");

        final PackagedProgram childFirstProgram =
                PackagedProgram.newBuilder()
                        .setJarFile(new File(CLASSLOADING_POLICY_JAR_PATH))
                        .setUserClassPaths(
                                Collections.singletonList(childResourceDir.toURI().toURL()))
                        .setConfiguration(childFirstConf)
                        .setArguments(testResourceName, childResourceDirName)
                        .build();

        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(childFirstProgram.getUserCodeClassLoader());
        try {
            childFirstProgram.invokeInteractiveModeForExecution();
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Test
    void testProgramWithParentFirstClassLoader() throws IOException, ProgramInvocationException {
        // We have two files named test-resource in src/resource (parent classloader classpath) and
        // tmp folders (child classloader classpath) respectively.
        String childResourceDirName = "child1";
        String testResourceName = "test-resource";
        File childResourceDir = TempDirUtils.newFolder(folder, childResourceDirName);
        File childResource = new File(childResourceDir, testResourceName);
        assertThat(childResource.createNewFile()).isTrue();

        TestStreamEnvironment.setAsContext(
                miniClusterResource.getMiniCluster(),
                parallelism,
                Collections.singleton(new Path(CLASSLOADING_POLICY_JAR_PATH)),
                Collections.emptyList());

        // parent-first classloading
        Configuration parentFirstConf = new Configuration();
        parentFirstConf.setString("classloader.resolve-order", "parent-first");

        final PackagedProgram parentFirstProgram =
                PackagedProgram.newBuilder()
                        .setJarFile(new File(CLASSLOADING_POLICY_JAR_PATH))
                        .setUserClassPaths(
                                Collections.singletonList(childResourceDir.toURI().toURL()))
                        .setConfiguration(parentFirstConf)
                        .setArguments(testResourceName, "test-classes")
                        .build();

        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(parentFirstProgram.getUserCodeClassLoader());
        try {
            parentFirstProgram.invokeInteractiveModeForExecution();
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }
}
