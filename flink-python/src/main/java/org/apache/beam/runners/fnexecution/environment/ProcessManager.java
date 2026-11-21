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
package org.apache.beam.runners.fnexecution.environment;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

/** A simple process manager which forks processes and kills them if necessary. */
@ThreadSafe
public class ProcessManager {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessManager.class);

    /** A symbolic file to indicate that we want to inherit I/O of parent process. */
    public static final File INHERIT_IO_FILE = new File("_inherit_io_unused_filename_");

    /** For debugging purposes, we inherit I/O of processes. */
    private static final boolean INHERIT_IO = LOG.isDebugEnabled();

    /** A list of all managers to ensure all processes shutdown on JVM exit . */
    private static final List<ProcessManager> ALL_PROCESS_MANAGERS = new ArrayList<>();

    @VisibleForTesting static Thread shutdownHook = null;

    private final Map<String, Process> processes;

    public static ProcessManager create() {
        return new ProcessManager();
    }

    private ProcessManager() {
        this.processes = Collections.synchronizedMap(new HashMap<>());
    }

    public static class RunningProcess {
        private Process process;

        RunningProcess(Process process) {
            this.process = process;
        }

        /** Checks if the underlying process is still running. */
        public void isAliveOrThrow() throws IllegalStateException {
            if (!process.isAlive()) {
                throw new IllegalStateException(
                        "Process died with exit code " + process.exitValue());
            }
        }

        @VisibleForTesting
        Process getUnderlyingProcess() {
            return process;
        }
    }

    /**
     * Forks a process with the given command and arguments.
     *
     * @param id A unique id for the process
     * @param command the name of the executable to run
     * @param args arguments to provide to the executable
     * @return A RunningProcess which can be checked for liveness
     */
    RunningProcess startProcess(String id, String command, List<String> args) throws IOException {
        return startProcess(id, command, args, Collections.emptyMap());
    }

    /**
     * Forks a process with the given command, arguments, and additional environment variables.
     *
     * @param id A unique id for the process
     * @param command The name of the executable to run
     * @param args Arguments to provide to the executable
     * @param env Additional environment variables for the process to be forked
     * @return A RunningProcess which can be checked for liveness
     */
    public RunningProcess startProcess(
            String id, String command, List<String> args, Map<String, String> env)
            throws IOException {
        final File outputFile;
        if (INHERIT_IO) {
            LOG.debug(
                    "==> DEBUG enabled: Inheriting stdout/stderr of process (adjustable in ProcessManager)");
            outputFile = INHERIT_IO_FILE;
        } else {
            // Pipe stdout and stderr to /dev/null to avoid blocking the process due to filled PIPE
            // buffer
            if (System.getProperty("os.name", "").startsWith("Windows")) {
                outputFile = new File("nul");
            } else {
                outputFile = new File("/dev/null");
            }
        }
        return startProcess(id, command, args, env, outputFile);
    }

    public RunningProcess startProcess(
            String id, String command, List<String> args, Map<String, String> env, File outputFile)
            throws IOException {
        checkNotNull(id, "Process id must not be null");
        checkNotNull(command, "Command must not be null");
        checkNotNull(args, "Process args must not be null");
        checkNotNull(env, "Environment map must not be null");
        checkNotNull(outputFile, "Output redirect file must not be null");

        ProcessBuilder pb =
                new ProcessBuilder(
                        ImmutableList.<String>builder().add(command).addAll(args).build());
        pb.environment().putAll(env);

        if (INHERIT_IO_FILE.equals(outputFile)) {
            pb.inheritIO();
        } else {
            pb.redirectErrorStream(true);
            pb.redirectOutput(outputFile);
        }

        LOG.debug("Attempting to start process with command: {}", pb.command());
        Process newProcess = pb.start();
        newProcess
                .onExit()
                .thenAccept(
                        exited ->
                                LOG.info(
                                        "Process for worker {} exited: pid={}, exitCode={}",
                                        id,
                                        exited.pid(),
                                        exited.exitValue()));
        Process oldProcess = processes.put(id, newProcess);
        synchronized (ALL_PROCESS_MANAGERS) {
            if (!ALL_PROCESS_MANAGERS.contains(this)) {
                ALL_PROCESS_MANAGERS.add(this);
            }
            if (shutdownHook == null) {
                shutdownHook = ShutdownHook.create();
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            }
        }
        if (oldProcess != null) {
            stopProcess(id, oldProcess);
            stopProcess(id, newProcess);
            throw new IllegalStateException("There was already a process running with id " + id);
        }

        return new RunningProcess(newProcess);
    }

    /** Stops a previously started process identified by its unique id. */
    public void stopProcess(String id) {
        checkNotNull(id, "Process id must not be null");
        try {
            Process process =
                    checkNotNull(processes.remove(id), "Process for id does not exist: %s", id);
            stopProcess(id, process);
        } finally {
            synchronized (ALL_PROCESS_MANAGERS) {
                if (processes.isEmpty()) {
                    ALL_PROCESS_MANAGERS.remove(this);
                }
                if (ALL_PROCESS_MANAGERS.isEmpty() && shutdownHook != null) {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                    shutdownHook = null;
                }
            }
        }
    }

    private void stopProcess(String id, Process process) {
        if (process.isAlive()) {
            LOG.debug("Attempting to stop process with id {}", id);
            // first try to kill gracefully
            process.destroy();
            long maxTimeToWait = 500;
            try {
                if (waitForProcessToDie(process, maxTimeToWait)) {
                    LOG.debug("Process for worker {} shut down gracefully.", id);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (process.isAlive()) {
                    LOG.info("Process for worker {} still running. Killing.", id);
                    process.destroyForcibly();
                }
            }
        }
        // Reap the child process to prevent zombie accumulation. destroy()/destroyForcibly() send
        // signals but do not call waitpid(), so the terminated process remains in the kernel
        // process
        // table as a zombie until waitFor() collects its exit status.
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Returns true if the process exists within maxWaitTimeMillis. */
    private static boolean waitForProcessToDie(Process process, long maxWaitTimeMillis)
            throws InterruptedException {
        final long startTime = System.currentTimeMillis();
        while (process.isAlive() && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
            Thread.sleep(50);
        }
        return !process.isAlive();
    }

    private static class ShutdownHook extends Thread {

        private static ShutdownHook create() {
            return new ShutdownHook();
        }

        private ShutdownHook() {}

        @Override
        public void run() {
            synchronized (ALL_PROCESS_MANAGERS) {
                ALL_PROCESS_MANAGERS.forEach(ProcessManager::stopAllProcesses);
                // If any processes are still alive, wait for 200 ms.
                try {
                    if (ALL_PROCESS_MANAGERS.stream()
                            .anyMatch(
                                    pm ->
                                            pm.processes.values().stream()
                                                    .anyMatch(Process::isAlive))) {
                        // Graceful shutdown period after asking processes to quit
                        Thread.sleep(200);
                    }
                } catch (InterruptedException ignored) {
                    // Ignore interruptions here to proceed with killing processes
                } finally {
                    ALL_PROCESS_MANAGERS.forEach(ProcessManager::killAllProcesses);
                }
            }
        }
    }

    /** Stop all remaining processes gracefully, i.e. upon JVM shutdown */
    private void stopAllProcesses() {
        processes.forEach((id, process) -> process.destroy());
    }

    /** Kill all remaining processes forcibly and reap them, i.e. upon JVM shutdown. */
    private void killAllProcesses() {
        processes.forEach(
                (id, process) -> {
                    process.destroyForcibly();
                    try {
                        process.waitFor();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
    }
}
