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

package org.apache.flink.runtime.testutils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.testutils.CommonTestUtils.PipeForwarder;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Utility class wrapping {@link ProcessBuilder} and pre-configuring it with common options.
 *
 * <p>This class unifies the functionality previously split between TestJvmProcess and the original
 * TestProcessBuilder from flink-test-utils. It provides a builder pattern for flexible
 * configuration and includes lifecycle management, process monitoring, and file-based
 * synchronization utilities.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * TestProcess process = new TestProcessBuilder(MyMainClass.class.getName())
 *     .setJvmMemory(MemorySize.parse("256mb"))
 *     .addMainClassArg("--config")
 *     .addMainClassArg("value")
 *     .start();
 * }</pre>
 */
public class TestProcessBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TestProcessBuilder.class);

    private final String javaCommand = checkNotNull(getJavaCommandPath());

    private final ArrayList<String> jvmArgs = new ArrayList<>();
    private final ArrayList<String> mainClassArgs = new ArrayList<>();

    private final String mainClass;

    private MemorySize jvmMemory = MemorySize.parse("80mb");

    private boolean withCleanEnvironment = false;

    private String processName;

    public TestProcessBuilder(String mainClass) throws IOException {
        this(mainClass, null);
    }

    /**
     * Creates a new TestProcessBuilder.
     *
     * @param mainClass The fully qualified name of the main class to run
     * @param processName Optional name for the process (for logging/debugging)
     */
    public TestProcessBuilder(String mainClass, String processName) throws IOException {
        File tempLogFile =
                File.createTempFile(getClass().getSimpleName() + "-", "-log4j.properties");
        tempLogFile.deleteOnExit();
        CommonTestUtils.printLog4jDebugConfig(tempLogFile);

        jvmArgs.add("-Dlog.level=DEBUG");
        jvmArgs.add("-Dlog4j.configurationFile=file:" + tempLogFile.getAbsolutePath());
        jvmArgs.add("-classpath");
        jvmArgs.add(getCurrentClasspath());
        jvmArgs.add("-XX:+IgnoreUnrecognizedVMOptions");

        final String moduleConfig = System.getProperty("surefire.module.config");
        if (moduleConfig != null) {
            for (String moduleArg : moduleConfig.split(" ")) {
                addJvmArg(moduleArg);
            }
        }

        this.mainClass = mainClass;
        this.processName = processName != null ? processName : mainClass;
    }

    public TestProcess start() throws IOException {
        final ArrayList<String> commands = new ArrayList<>();

        commands.add(javaCommand);
        commands.add(String.format("-Xms%dm", jvmMemory.getMebiBytes()));
        commands.add(String.format("-Xmx%dm", jvmMemory.getMebiBytes()));
        commands.addAll(jvmArgs);
        commands.add(mainClass);
        commands.addAll(mainClassArgs);

        StringWriter processOutput = new StringWriter();
        StringWriter errorOutput = new StringWriter();
        LOG.info("Starting process '{}' with commands {}", processName, commands);
        final ProcessBuilder processBuilder = new ProcessBuilder(commands);
        if (withCleanEnvironment) {
            processBuilder.environment().clear();
        }
        Process process = processBuilder.start();
        new PipeForwarder(process.getInputStream(), processOutput);
        new PipeForwarder(process.getErrorStream(), errorOutput);

        return new TestProcess(process, processOutput, errorOutput, processName);
    }

    public TestProcessBuilder setJvmMemory(MemorySize jvmMemory) {
        this.jvmMemory = jvmMemory;
        return this;
    }

    public TestProcessBuilder addJvmArg(String arg) {
        jvmArgs.add(arg);
        return this;
    }

    public TestProcessBuilder addMainClassArg(String arg) {
        mainClassArgs.add(arg);
        return this;
    }

    public TestProcessBuilder addConfigAsMainClassArgs(Configuration config) {
        for (Entry<String, String> keyValue : config.toMap().entrySet()) {
            addMainClassArg("--" + keyValue.getKey());
            addMainClassArg(keyValue.getValue());
        }
        return this;
    }

    public TestProcessBuilder withCleanEnvironment() {
        withCleanEnvironment = true;
        return this;
    }

    public TestProcessBuilder setProcessName(String processName) {
        this.processName = processName;
        return this;
    }

    /**
     * Helper factory method to simplify migration from TestJvmProcess subclasses.
     *
     * <p>This method mimics the pattern used by TestJvmProcess where you provide an entry point
     * class and main method arguments.
     *
     * @param entryPointClassName The fully qualified name of the entry point class
     * @param processName Optional name for the process
     * @param mainMethodArgs Arguments to pass to the main method
     * @return A TestProcess instance
     * @throws Exception if the process cannot be started
     */
    public static TestProcess createAndStart(
            String entryPointClassName, String processName, String... mainMethodArgs)
            throws Exception {
        TestProcessBuilder builder = new TestProcessBuilder(entryPointClassName, processName);
        for (String arg : mainMethodArgs) {
            builder.addMainClassArg(arg);
        }
        return builder.start();
    }

    // ---------------------------------------------------------------------------------------------
    // File-based synchronization utilities (migrated from TestJvmProcess)
    // ---------------------------------------------------------------------------------------------

    /**
     * Touches a file by updating its last modified timestamp.
     *
     * @param file The file to touch
     * @throws IOException if the file cannot be touched
     */
    public static void touchFile(File file) throws IOException {
        if (!file.exists()) {
            new FileOutputStream(file).close();
        }
        if (!file.setLastModified(System.currentTimeMillis())) {
            throw new IOException("Could not touch the file.");
        }
    }

    /**
     * Waits for a marker file to exist.
     *
     * @param file The marker file to wait for
     * @param timeoutMillis The maximum time to wait in milliseconds
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public static void waitForMarkerFile(File file, long timeoutMillis)
            throws InterruptedException {
        final long deadline = System.nanoTime() + timeoutMillis * 1_000_000;

        boolean exists;
        while (!(exists = file.exists()) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }

        assertThat(exists)
                .withFailMessage("The marker file was not found within %s msecs", timeoutMillis)
                .isTrue();
    }

    /**
     * Kills a process using SIGTERM signal.
     *
     * @param pid The process ID to kill
     * @throws Exception if the kill command fails
     */
    public static void killProcessWithSigTerm(long pid) throws Exception {
        final Process kill = Runtime.getRuntime().exec("kill " + pid);
        kill.waitFor();
        assertThat(kill.exitValue())
                .withFailMessage("failed to send SIG_TERM to process %s", pid)
                .isZero();
    }

    /**
     * Waits for multiple marker files to exist.
     *
     * @param basedir The base directory containing the marker files
     * @param prefix The prefix of the marker files
     * @param num The number of marker files to wait for
     * @param timeout The maximum time to wait in milliseconds
     */
    public static void waitForMarkerFiles(File basedir, String prefix, int num, long timeout) {
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
                return;
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                now = System.currentTimeMillis();
            }
        }

        fail("The tasks were not started within time (" + timeout + "msecs)");
    }

    /**
     * Wrapper for a {@link Process} with its output streams and lifecycle management.
     *
     * <p>This class provides enhanced lifecycle management including automatic shutdown hooks,
     * process monitoring, and detailed output capture.
     */
    public static class TestProcess {
        private static final Logger LOG = LoggerFactory.getLogger(TestProcess.class);

        private final Process process;
        private final StringWriter processOutput;
        private final StringWriter errorOutput;
        private final String processName;
        private final Thread shutdownHook;

        private volatile boolean destroyed;

        public TestProcess(
                Process process,
                StringWriter processOutput,
                StringWriter errorOutput,
                String processName) {
            this.process = process;
            this.processOutput = processOutput;
            this.errorOutput = errorOutput;
            this.processName = processName;

            this.shutdownHook =
                    new Thread(
                            () -> {
                                try {
                                    destroyForcibly();
                                } catch (Throwable t) {
                                    LOG.error(
                                            "Error during process cleanup shutdown hook for '{}'.",
                                            processName,
                                            t);
                                }
                            });

            try {
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down. No need to do this.
            } catch (Throwable t) {
                LOG.error("Cannot register process cleanup shutdown hook.", t);
            }
        }

        public Process getProcess() {
            return process;
        }

        public StringWriter getProcessOutput() {
            return processOutput;
        }

        public StringWriter getErrorOutput() {
            return errorOutput;
        }

        public String getProcessName() {
            return processName;
        }

        /**
         * Destroys the process gracefully.
         *
         * @deprecated Use {@link #destroyForcibly()} for explicit force destroy or {@link
         *     Process#destroy()} for graceful destroy.
         */
        @Deprecated
        public void destroy() {
            process.destroy();
        }

        /**
         * Destroys the process forcibly and waits for it to terminate.
         *
         * @throws InterruptedException if interrupted while waiting for process termination
         */
        public void destroyForcibly() throws InterruptedException {
            if (destroyed) {
                return;
            }

            LOG.info("Destroying '{}' process.", processName);

            try {
                process.destroyForcibly();
                process.waitFor();
            } finally {
                destroyed = true;
                ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
            }
        }

        /**
         * Prints the captured process output to System.out.
         *
         * <p>This is useful for debugging test failures.
         */
        public void printProcessLog() {
            System.out.println("-----------------------------------------");
            System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + processName);
            System.out.println("-----------------------------------------");

            String out = processOutput.toString();
            if (out == null || out.length() == 0) {
                System.out.println("(STDOUT EMPTY)");
            } else {
                System.out.println("STDOUT:");
                System.out.println(out);
            }

            System.out.println();

            String err = errorOutput.toString();
            if (err == null || err.length() == 0) {
                System.out.println("(STDERR EMPTY)");
            } else {
                System.out.println("STDERR:");
                System.out.println(err);
            }

            System.out.println("-----------------------------------------");
            System.out.println("  END SPAWNED PROCESS LOG " + processName);
            System.out.println("-----------------------------------------");
        }

        /**
         * Gets the process ID, if possible.
         *
         * <p>This method works on UNIX-based operating systems and modern Java versions. On others,
         * it returns {@code -1}.
         *
         * @return The process ID, or -1 if the ID cannot be determined.
         */
        public long getProcessId() {
            try {
                Class<? extends Process> clazz = process.getClass();
                if (clazz.getName().equals("java.lang.UNIXProcess")) {
                    Field pidField = clazz.getDeclaredField("pid");
                    pidField.setAccessible(true);
                    return pidField.getLong(process);
                } else if (clazz.getName().equals("java.lang.ProcessImpl")) {
                    Method pid = clazz.getDeclaredMethod("pid");
                    pid.setAccessible(true);
                    return (long) pid.invoke(process);
                } else {
                    return -1;
                }
            } catch (Throwable ignored) {
                return -1;
            }
        }

        /**
         * Checks if the process is still alive.
         *
         * @return true if the process is still running, false otherwise
         */
        public boolean isAlive() {
            if (destroyed) {
                return false;
            } else {
                try {
                    process.exitValue();
                    return false;
                } catch (IllegalThreadStateException ignored) {
                    return true;
                }
            }
        }

        /**
         * Waits for the process to terminate.
         *
         * @throws InterruptedException if interrupted while waiting
         */
        public void waitFor() throws InterruptedException {
            process.waitFor();
        }

        /**
         * Waits for the process to terminate with a timeout.
         *
         * @param timeout the maximum time to wait
         * @param unit the time unit of the timeout argument
         * @return true if the process terminated, false if the timeout elapsed
         * @throws InterruptedException if interrupted while waiting
         */
        public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
            return process.waitFor(timeout, unit);
        }

        /**
         * Gets the exit code of the process.
         *
         * @return the exit code
         * @throws IllegalThreadStateException if the process has not yet terminated
         */
        public int exitCode() {
            return process.exitValue();
        }
    }
}
