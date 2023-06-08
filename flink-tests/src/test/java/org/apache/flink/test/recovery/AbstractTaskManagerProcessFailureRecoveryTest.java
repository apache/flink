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

package org.apache.flink.test.recovery;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.util.BlobServerExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.test.recovery.utils.TaskExecutorProcessEntryPoint;
import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.test.util.TestProcessBuilder.TestProcess;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Abstract base for tests verifying the behavior of the recovery in the case when a TaskManager
 * fails (process is killed) in the middle of a job execution.
 *
 * <p>The test works with multiple task managers processes by spawning JVMs. Initially, it starts a
 * JobManager in process and two TaskManagers JVMs with 2 task slots each. It submits a program with
 * parallelism 4 and waits until all tasks are brought up. Coordination between the test and the
 * tasks happens via checking for the existence of temporary files. It then starts another
 * TaskManager, which is guaranteed to remain empty (all tasks are already deployed) and kills one
 * of the original task managers. The recovery should restart the tasks on the new TaskManager.
 */
abstract class AbstractTaskManagerProcessFailureRecoveryTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractTaskManagerProcessFailureRecoveryTest.class);
    protected static final String READY_MARKER_FILE_PREFIX = "ready_";
    protected static final String PROCEED_MARKER_FILE = "proceed";

    protected static final int PARALLELISM = 4;

    @TempDir public Path temporaryFolder;

    @RegisterExtension
    public final EachCallbackWrapper<BlobServerExtension> blobServerExtensionWrapper =
            new EachCallbackWrapper<>(new BlobServerExtension());

    @RegisterExtension
    public final EachCallbackWrapper<ZooKeeperExtension> zooKeeperExtensionWrapper =
            new EachCallbackWrapper<>(new ZooKeeperExtension());

    @TestTemplate
    void testTaskManagerProcessFailure() throws Exception {

        TestProcess taskManagerProcess1 = null;
        TestProcess taskManagerProcess2 = null;
        TestProcess taskManagerProcess3 = null;

        File coordinateTempDir;

        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.setString(RestOptions.BIND_PORT, "0");
        config.setLong(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, 200L);
        config.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 10000L);
        config.set(HeartbeatManagerOptions.HEARTBEAT_RPC_FAILURE_THRESHOLD, 1);
        config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("3200k"));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("3200k"));
        config.set(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS, 16);
        config.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse("128m"));
        config.set(TaskManagerOptions.CPU_CORES, 1.0);
        config.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");
        config.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofSeconds(30L));

        try (final StandaloneSessionClusterEntrypoint clusterEntrypoint =
                new StandaloneSessionClusterEntrypoint(config)) {

            // check that we run this test only if the java command
            // is available on this machine
            String javaCommand = getJavaCommandPath();
            if (javaCommand == null) {
                System.out.println(
                        "---- Skipping Process Failure test : Could not find java executable ----");
                return;
            }

            clusterEntrypoint.startCluster();

            // coordination between the processes goes through a directory
            coordinateTempDir = TempDirUtils.newFolder(temporaryFolder);

            TestProcessBuilder taskManagerProcessBuilder =
                    new TestProcessBuilder(TaskExecutorProcessEntryPoint.class.getName());

            taskManagerProcessBuilder.addConfigAsMainClassArgs(config);

            // start the first two TaskManager processes
            taskManagerProcess1 = taskManagerProcessBuilder.start();
            taskManagerProcess2 = taskManagerProcessBuilder.start();

            // the program will set a marker file in each of its parallel tasks once they are ready,
            // so that
            // this coordinating code is aware of this.
            // the program will very slowly consume elements until the marker file (later created by
            // the
            // test driver code) is present
            final File coordinateDirClosure = coordinateTempDir;
            final AtomicReference<Throwable> errorRef = new AtomicReference<>();

            // we trigger program execution in a separate thread
            Thread programTrigger =
                    new Thread("Program Trigger") {
                        @Override
                        public void run() {
                            try {
                                testTaskManagerFailure(config, coordinateDirClosure);
                            } catch (Throwable t) {
                                t.printStackTrace();
                                errorRef.set(t);
                            }
                        }
                    };

            // start the test program
            programTrigger.start();

            // wait until all marker files are in place, indicating that all tasks have started
            // max 20 seconds
            if (!waitForMarkerFiles(
                    coordinateTempDir, READY_MARKER_FILE_PREFIX, PARALLELISM, 120000)) {
                // check if the program failed for some reason
                if (errorRef.get() != null) {
                    Throwable error = errorRef.get();
                    error.printStackTrace();
                    fail(
                            "The program encountered a "
                                    + error.getClass().getSimpleName()
                                    + " : "
                                    + error.getMessage());
                } else {
                    // no error occurred, simply a timeout
                    fail("The tasks were not started within time (" + 120000 + "ms)");
                }
            }

            // start the third TaskManager
            taskManagerProcess3 = taskManagerProcessBuilder.start();

            // kill one of the previous TaskManagers, triggering a failure and recovery
            taskManagerProcess1.destroy();
            waitForShutdown("TaskManager 1", taskManagerProcess1);

            // we create the marker file which signals the program functions tasks that they can
            // complete
            touchFile(new File(coordinateTempDir, PROCEED_MARKER_FILE));

            // wait for at most 5 minutes for the program to complete
            programTrigger.join(300000);

            // check that the program really finished
            assertThat(programTrigger.isAlive())
                    .withFailMessage("The program did not finish in time")
                    .isFalse();

            // check whether the program encountered an error
            if (errorRef.get() != null) {
                Throwable error = errorRef.get();
                error.printStackTrace();
                fail(
                        "The program encountered a "
                                + error.getClass().getSimpleName()
                                + " : "
                                + error.getMessage());
            }

            // all seems well :-)
        } catch (Exception e) {
            e.printStackTrace();
            printProcessLog("TaskManager 1", taskManagerProcess1);
            printProcessLog("TaskManager 2", taskManagerProcess2);
            printProcessLog("TaskManager 3", taskManagerProcess3);
            fail(e.getMessage());
        } catch (Error e) {
            e.printStackTrace();
            printProcessLog("TaskManager 1", taskManagerProcess1);
            printProcessLog("TaskManager 2", taskManagerProcess2);
            printProcessLog("TaskManager 3", taskManagerProcess3);
            throw e;
        } finally {
            if (taskManagerProcess1 != null) {
                taskManagerProcess1.destroy();
            }
            if (taskManagerProcess2 != null) {
                taskManagerProcess2.destroy();
            }
            if (taskManagerProcess3 != null) {
                taskManagerProcess3.destroy();
            }

            waitForShutdown("TaskManager 1", taskManagerProcess1);
            waitForShutdown("TaskManager 2", taskManagerProcess2);
            waitForShutdown("TaskManager 3", taskManagerProcess3);
        }
    }

    private void waitForShutdown(final String processName, @Nullable final TestProcess process)
            throws InterruptedException {
        if (process == null) {
            return;
        }

        if (!process.getProcess().waitFor(30, TimeUnit.SECONDS)) {
            LOG.error("{} did not shutdown in time.", processName);
            printProcessLog(processName, process);
            process.getProcess().destroyForcibly();
        }
    }

    /**
     * The test program should be implemented here in a form of a separate thread. This provides a
     * solution for checking that it has been terminated.
     *
     * @param configuration the config to use
     * @param coordinateDir TaskManager failure will be triggered only after processes have
     *     successfully created file under this directory
     */
    public abstract void testTaskManagerFailure(Configuration configuration, File coordinateDir)
            throws Exception;

    static void printProcessLog(String processName, TestProcess process) {
        if (process == null) {
            System.out.println("-----------------------------------------");
            System.out.println(" PROCESS " + processName + " WAS NOT STARTED.");
            System.out.println("-----------------------------------------");
        } else {
            System.out.println("-----------------------------------------");
            System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + processName);
            System.out.println("-----------------------------------------");
            System.out.println(process.getErrorOutput().toString());
            System.out.println("-----------------------------------------");
            System.out.println("		END SPAWNED PROCESS LOG");
            System.out.println("-----------------------------------------");
        }
    }

    protected static void touchFile(File file) throws IOException {
        if (!file.exists()) {
            new FileOutputStream(file).close();
        }
        if (!file.setLastModified(System.currentTimeMillis())) {
            throw new IOException("Could not touch the file.");
        }
    }

    protected static boolean waitForMarkerFiles(
            File basedir, String prefix, int num, long timeout) {
        long now = System.currentTimeMillis();
        final long deadline = now + timeout;

        while (now < deadline) {
            boolean allFound = true;

            for (int i = 0; i < num; i++) {
                File nextToCheck = new File(basedir, prefix + i);
                if (!nextToCheck.exists()) {
                    allFound = false;
                    break;
                }
            }

            if (allFound) {
                return true;
            } else {
                // not all found, wait for a bit
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                now = System.currentTimeMillis();
            }
        }

        return false;
    }

    // --------------------------------------------------------------------------------------------

}
