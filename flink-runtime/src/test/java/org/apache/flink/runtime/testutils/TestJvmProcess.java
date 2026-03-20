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

import org.apache.flink.util.ShutdownHookUtil;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testutils.CommonTestUtils.createTemporaryLog4JProperties;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link Process} running a separate JVM.
 *
 * @deprecated This class has been unified with {@link TestProcessBuilder}. Please migrate to using
 *     TestProcessBuilder which provides the same functionality with a more flexible builder
 *     pattern.
 *     <p>Migration guide:
 *     <ul>
 *       <li>Instead of extending TestJvmProcess, use TestProcessBuilder directly
 *       <li>Replace {@code new MyProcess().startProcess()} with {@code new
 *           TestProcessBuilder(MyEntryPoint.class.getName()).addMainClassArg(...).start()}
 *       <li>Replace {@code setJVMMemory(int)} with {@code
 *           setJvmMemory(MemorySize.ofMebiBytes(...))}
 *       <li>Replace {@code getProcessOutput()} with {@code getProcessOutput().toString()}
 *       <li>File synchronization utilities (touchFile, waitForMarkerFile, etc.) are now static
 *           methods in TestProcessBuilder
 *     </ul>
 *     <p>Example migration:
 *     <pre>{@code
 * // Old approach:
 * class MyTestProcess extends TestJvmProcess {
 *     public String getName() { return "MyProcess"; }
 *     public String getEntryPointClassName() { return MyMain.class.getName(); }
 *     public String[] getMainMethodArgs() { return new String[]{"arg1", "arg2"}; }
 * }
 * MyTestProcess process = new MyTestProcess();
 * process.setJVMMemory(256);
 * process.startProcess();
 *
 * // New approach:
 * TestProcess process = new TestProcessBuilder(MyMain.class.getName())
 *     .setProcessName("MyProcess")
 *     .setJvmMemory(MemorySize.parse("256mb"))
 *     .addMainClassArg("arg1")
 *     .addMainClassArg("arg2")
 *     .start();
 * }</pre>
 */
@Deprecated
public abstract class TestJvmProcess {

    private static final Logger LOG = LoggerFactory.getLogger(TestJvmProcess.class);

    /** Lock to guard {@link #startProcess()} and {@link #destroy()} calls. */
    private final Object createDestroyLock = new Object();

    /** The java command path */
    private final String javaCommandPath;

    /** The log4j configuration path. */
    private final String log4jConfigFilePath;

    /** Shutdown hook for resource cleanup */
    private final Thread shutdownHook;

    /** JVM process memory (set for both '-Xms' and '-Xmx'). */
    private int jvmMemoryInMb = 80;

    /** The JVM process */
    private volatile Process process;

    /** Writer for the process output */
    private volatile StringWriter processOutput;

    /** flag to mark the process as already destroyed */
    private volatile boolean destroyed;

    public TestJvmProcess() throws Exception {
        this(getJavaCommandPath(), createTemporaryLog4JProperties().getPath());
    }

    public TestJvmProcess(String javaCommandPath, String log4jConfigFilePath) {
        this.javaCommandPath = checkNotNull(javaCommandPath, "Java command path");
        this.log4jConfigFilePath = checkNotNull(log4jConfigFilePath, "log4j config file path");

        this.shutdownHook =
                new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    destroy();
                                } catch (Throwable t) {
                                    LOG.error("Error during process cleanup shutdown hook.", t);
                                }
                            }
                        });
    }

    /** Returns the name of the process. */
    public abstract String getName();

    /**
     * Returns the arguments to the JVM.
     *
     * <p>These can be parsed by the main method of the entry point class.
     */
    public abstract String[] getMainMethodArgs();

    /**
     * Returns the name of the class to run.
     *
     * <p>Arguments to the main method can be specified via {@link #getMainMethodArgs()}.
     */
    public abstract String getEntryPointClassName();

    // ---------------------------------------------------------------------------------------------

    /**
     * Sets the memory for the process (<code>-Xms</code> and <code>-Xmx</code> flags) (>= 80).
     *
     * @param jvmMemoryInMb Amount of memory in Megabytes for the JVM (>= 80).
     */
    public void setJVMMemory(int jvmMemoryInMb) {
        checkArgument(jvmMemoryInMb >= 80, "Process JVM Requires at least 80 MBs of memory.");
        checkState(process == null, "Cannot set memory after process was started");

        this.jvmMemoryInMb = jvmMemoryInMb;
    }

    /**
     * Creates and starts the {@link Process}.
     *
     * <p><strong>Important:</strong> Don't forget to call {@link #destroy()} to prevent resource
     * leaks. The created process will be child process and is not guaranteed to terminate when the
     * parent process terminates.
     */
    public void startProcess() throws IOException {
        String[] cmd =
                new String[] {
                    javaCommandPath,
                    "-Dlog.level=DEBUG",
                    "-Dlog4j.configurationFile=file:" + log4jConfigFilePath,
                    "-Xms" + jvmMemoryInMb + "m",
                    "-Xmx" + jvmMemoryInMb + "m",
                    "-classpath",
                    getCurrentClasspath(),
                    "-XX:+IgnoreUnrecognizedVMOptions"
                };

        final String moduleConfig = System.getProperty("surefire.module.config");
        if (moduleConfig != null) {
            cmd = ArrayUtils.addAll(cmd, moduleConfig.trim().split("\\s+"));
        }

        cmd = ArrayUtils.add(cmd, getEntryPointClassName());

        String[] mainMethodArgs = getMainMethodArgs();

        if (mainMethodArgs != null && mainMethodArgs.length > 0) {
            cmd = ArrayUtils.addAll(cmd, mainMethodArgs);
        }

        synchronized (createDestroyLock) {
            checkState(process == null, "process already started");

            LOG.debug("Running command '{}'.", Arrays.toString(cmd));
            this.process = new ProcessBuilder(cmd).start();

            // Forward output
            this.processOutput = new StringWriter();
            new CommonTestUtils.PipeForwarder(process.getErrorStream(), processOutput);

            try {
                // Add JVM shutdown hook to call shutdown of service
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down. No need to do this.
            } catch (Throwable t) {
                LOG.error("Cannot register process cleanup shutdown hook.", t);
            }
        }
    }

    public void printProcessLog() {
        checkState(processOutput != null, "not started");

        System.out.println("-----------------------------------------");
        System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + getName());
        System.out.println("-----------------------------------------");

        String out = processOutput.toString();
        if (out == null || out.length() == 0) {
            System.out.println("(EMPTY)");
        } else {
            System.out.println(out);
        }

        System.out.println("-----------------------------------------");
        System.out.println("		END SPAWNED PROCESS LOG " + getName());
        System.out.println("-----------------------------------------");
    }

    public void destroy() throws InterruptedException {
        synchronized (createDestroyLock) {
            checkState(process != null, "process not started");

            if (destroyed) {
                // already done
                return;
            }

            LOG.info("Destroying " + getName() + " process.");

            try {
                process.destroyForcibly();
                process.waitFor();
            } finally {
                destroyed = true;
                ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
            }
        }
    }

    public String getProcessOutput() {
        if (processOutput != null) {
            return processOutput.toString();
        } else {
            return null;
        }
    }

    /**
     * Gets the process ID, if possible. This method currently only work on UNIX-based operating
     * systems. On others, it returns {@code -1}.
     *
     * @return The process ID, or -1, if the ID cannot be determined.
     */
    public long getProcessId() {
        checkState(process != null, "process not started");

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

    public boolean isAlive() {
        if (destroyed) {
            return false;
        } else {
            try {
                // the method throws an exception as long as the
                // process is alive
                process.exitValue();
                return false;
            } catch (IllegalThreadStateException ignored) {
                // thi
                return true;
            }
        }
    }

    public void waitFor() throws InterruptedException {
        Process process = this.process;
        if (process != null) {
            process.waitFor();
        } else {
            throw new IllegalStateException("process not started");
        }
    }

    public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
        final Process process = this.process;
        if (process != null) {
            return process.waitFor(timeout, unit);
        } else {
            throw new IllegalStateException("process not started");
        }
    }

    public int exitCode() {
        Process process = this.process;
        if (process != null) {
            return process.exitValue();
        } else {
            throw new IllegalStateException("process not started");
        }
    }

    // ---------------------------------------------------------------------------------------------
    // File based synchronization utilities
    // ---------------------------------------------------------------------------------------------

    /**
     * @deprecated Use {@link TestProcessBuilder#touchFile(File)} instead.
     */
    @Deprecated
    public static void touchFile(File file) throws IOException {
        TestProcessBuilder.touchFile(file);
    }

    /**
     * @deprecated Use {@link TestProcessBuilder#waitForMarkerFile(File, long)} instead.
     */
    @Deprecated
    public static void waitForMarkerFile(File file, long timeoutMillis)
            throws InterruptedException {
        TestProcessBuilder.waitForMarkerFile(file, timeoutMillis);
    }

    /**
     * @deprecated Use {@link TestProcessBuilder#killProcessWithSigTerm(long)} instead.
     */
    @Deprecated
    public static void killProcessWithSigTerm(long pid) throws Exception {
        TestProcessBuilder.killProcessWithSigTerm(pid);
    }

    /**
     * @deprecated Use {@link TestProcessBuilder#waitForMarkerFiles(File, String, int, long)}
     *     instead.
     */
    @Deprecated
    public static void waitForMarkerFiles(File basedir, String prefix, int num, long timeout) {
        TestProcessBuilder.waitForMarkerFiles(basedir, prefix, num, timeout);
    }
}
