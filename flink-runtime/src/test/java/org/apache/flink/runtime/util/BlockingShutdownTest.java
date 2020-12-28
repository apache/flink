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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.util.OperatingSystem;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Test that verifies the behavior of blocking shutdown hooks and of the {@link
 * JvmShutdownSafeguard} that guards against it.
 */
public class BlockingShutdownTest {

    @Test
    public void testProcessShutdownBlocking() throws Exception {
        // this test works only on linux
        assumeTrue(OperatingSystem.isLinux());

        final File markerFile =
                new File(
                        EnvironmentInformation.getTemporaryFileDirectory(),
                        UUID.randomUUID() + ".marker");

        final BlockingShutdownProcess blockingProcess =
                new BlockingShutdownProcess(markerFile.getAbsolutePath(), 0, false);

        try {
            blockingProcess.startProcess();
            long pid = blockingProcess.getProcessId();
            assertTrue("Cannot determine process ID", pid != -1);

            // wait for the marker file to appear, which means the process is up properly
            TestJvmProcess.waitForMarkerFile(markerFile, 30000);

            // send it a regular kill command (SIG_TERM)
            Process kill = Runtime.getRuntime().exec("kill " + pid);
            kill.waitFor();
            assertEquals("failed to send SIG_TERM to process", 0, kill.exitValue());

            // minimal delay until the Java process object notices that the process is gone
            // this will not let the test fail predictably if the process is actually in fact going
            // away,
            // but it would create frequent failures. Not ideal, but the best we can do without
            // severely prolonging the test
            Thread.sleep(50);

            // the process should not go away by itself
            assertTrue(
                    "Test broken, process shutdown blocking does not work",
                    blockingProcess.isAlive());
        } finally {
            blockingProcess.destroy();

            //noinspection ResultOfMethodCallIgnored
            markerFile.delete();
        }
    }

    @Test
    public void testProcessExitsDespiteBlockingShutdownHook() throws Exception {
        // this test works only on linux
        assumeTrue(OperatingSystem.isLinux());

        final File markerFile =
                new File(
                        EnvironmentInformation.getTemporaryFileDirectory(),
                        UUID.randomUUID() + ".marker");

        final BlockingShutdownProcess blockingProcess =
                new BlockingShutdownProcess(markerFile.getAbsolutePath(), 100, true);

        try {
            blockingProcess.startProcess();
            long pid = blockingProcess.getProcessId();
            assertTrue("Cannot determine process ID", pid != -1);

            // wait for the marker file to appear, which means the process is up properly
            TestJvmProcess.waitForMarkerFile(markerFile, 30000);

            // send it a regular kill command (SIG_TERM)
            Process kill = Runtime.getRuntime().exec("kill " + pid);
            kill.waitFor();
            assertEquals("failed to send SIG_TERM to process", 0, kill.exitValue());

            // the process should eventually go away
            final long deadline = System.nanoTime() + 30_000_000_000L; // 30 secs in nanos
            while (blockingProcess.isAlive() && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }

            assertFalse(
                    "shutdown blocking process does not properly terminate itself",
                    blockingProcess.isAlive());
        } finally {
            blockingProcess.destroy();

            //noinspection ResultOfMethodCallIgnored
            markerFile.delete();
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    // a method that blocks indefinitely
    static void parkForever() {
        // park this forever
        final Object lock = new Object();
        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (lock) {
                    lock.wait();
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Blocking Process Implementation
    // ------------------------------------------------------------------------

    private static final class BlockingShutdownProcess extends TestJvmProcess {

        private final String tempFilePath;
        private final long selfKillDelay;
        private final boolean installSignalHandler;

        public BlockingShutdownProcess(
                String tempFilePath, long selfKillDelay, boolean installSignalHandler)
                throws Exception {

            this.tempFilePath = tempFilePath;
            this.selfKillDelay = selfKillDelay;
            this.installSignalHandler = installSignalHandler;
        }

        @Override
        public String getName() {
            return "BlockingShutdownProcess";
        }

        @Override
        public String[] getJvmArgs() {
            return new String[] {
                tempFilePath, String.valueOf(installSignalHandler), String.valueOf(selfKillDelay)
            };
        }

        @Override
        public String getEntryPointClassName() {
            return BlockingShutdownProcessEntryPoint.class.getName();
        }
    }

    // ------------------------------------------------------------------------

    public static final class BlockingShutdownProcessEntryPoint {

        private static final Logger LOG =
                LoggerFactory.getLogger(BlockingShutdownProcessEntryPoint.class);

        public static void main(String[] args) throws Exception {
            File touchFile = new File(args[0]);
            boolean installHandler = Boolean.parseBoolean(args[1]);
            long killDelay = Long.parseLong(args[2]);

            // install the blocking shutdown hook
            Thread shutdownHook = new Thread(new BlockingRunnable(), "Blocking ShutdownHook");
            try {
                // Add JVM shutdown hook to call shutdown of service
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down. No need to do this.
            } catch (Throwable t) {
                System.err.println("Cannot register process cleanup shutdown hook.");
                t.printStackTrace();
            }

            // install the jvm terminator, if we want it
            if (installHandler) {
                JvmShutdownSafeguard.installAsShutdownHook(LOG, killDelay);
            }

            System.err.println("signaling process started");
            TestJvmProcess.touchFile(touchFile);

            System.err.println("parking the main thread");
            parkForever();
        }
    }

    // ------------------------------------------------------------------------

    static final class BlockingRunnable implements Runnable {

        @Override
        public void run() {
            System.err.println("starting shutdown hook");
            parkForever();
        }
    }
}
