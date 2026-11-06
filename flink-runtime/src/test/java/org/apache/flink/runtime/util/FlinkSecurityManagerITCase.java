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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.util.OperatingSystem;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Integration tests for the {@link FlinkSecurityManager}. */
class FlinkSecurityManagerITCase {

    private static final int FORCED_EXIT_CODE = 222;

    @BeforeEach
    void ensureSupportedOS() {
        // based on the assumption in JvmExitOnFatalErrorTest, and manual testing on Mac, we do not
        // support all platforms (in particular not Windows)
        assumeThat(OperatingSystem.isLinux() || OperatingSystem.isMac()).isTrue();
    }

    @Tag("org.apache.flink.testutils.junit.FailsOnJava25")
    @Test
    void testForcedJVMExit() throws Exception {
        runAndAssertExitCode(
                new ForcedJVMExitProcess(ForcedExitEntryPoint.class), FORCED_EXIT_CODE);
    }

    @Tag("org.apache.flink.testutils.junit.FailsOnJava25")
    @Test
    void testIgnoredJVMExit() throws Exception {
        runAndAssertExitCode(new ForcedJVMExitProcess(IgnoredExitEntryPoint.class), 0);
    }

    @Test
    void testForcedJVMExitWithoutSecurityManager() throws Exception {
        runAndAssertExitCode(
                new ForcedJVMExitProcess(ForcedExitWithoutSecurityManagerEntryPoint.class),
                FORCED_EXIT_CODE);
    }

    @Test
    void testUnavailableSecurityManagerIsReported() throws Exception {
        // 'disallow' is only understood by JDK 12+; JDK 24+ accepts it although the Security
        // Manager is disabled there anyway
        assumeThat(Runtime.version().feature()).isGreaterThanOrEqualTo(12);
        runAndAssertExitCode(
                new ForcedJVMExitProcess(
                        UnavailableSecurityManagerEntryPoint.class,
                        "-Djava.security.manager=disallow"),
                FORCED_EXIT_CODE);
    }

    private static void runAndAssertExitCode(ForcedJVMExitProcess testProcess, int expectedExitCode)
            throws Exception {
        testProcess.startProcess();
        try {
            testProcess.waitFor();
            assertThat(testProcess.exitCode())
                    .withFailMessage(
                            "Unexpected exit code %d. Process output:%n%s",
                            testProcess.exitCode(), testProcess.getProcessOutput())
                    .isEqualTo(expectedExitCode);
        } finally {
            testProcess.destroy();
        }
    }

    /** Configures the {@link FlinkSecurityManager} to intercept calls to System.exit(). */
    private static void interceptUserSystemExit() {
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        FlinkSecurityManager.setFromConfiguration(configuration);
    }

    private static final class ForcedJVMExitProcess extends TestJvmProcess {

        private final Class<?> entryPoint;
        private final String[] jvmArgs;

        private ForcedJVMExitProcess(Class<?> entryPoint, String... jvmArgs) throws Exception {
            this.entryPoint = entryPoint;
            this.jvmArgs = jvmArgs;
        }

        @Override
        public String getName() {
            return getEntryPointClassName();
        }

        @Override
        public String[] getMainMethodArgs() {
            return new String[0];
        }

        @Override
        public String getEntryPointClassName() {
            return entryPoint.getName();
        }

        @Override
        protected String[] getJvmArgs() {
            return jvmArgs;
        }
    }

    /** An entry point that attempts to force exit the process. */
    public static final class ForcedExitEntryPoint {

        public static void main(String[] args) {
            interceptUserSystemExit();

            FlinkSecurityManager.forceProcessExit(FORCED_EXIT_CODE);

            System.exit(0);
        }
    }

    /** An entry point that attempts to exit the process but expects that call to be ignored. */
    public static final class IgnoredExitEntryPoint {

        public static void main(String[] args) {
            interceptUserSystemExit();

            FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
            // expect this call to be ignored
            try {
                System.exit(123);
            } catch (Throwable t) {
                System.err.println(
                        "Caught exception during system exit with message: " + t.getMessage());
            }
        }
    }

    /**
     * An entry point that force exits the process without any security manager having been
     * installed, as is always the case on JDK 24+.
     */
    public static final class ForcedExitWithoutSecurityManagerEntryPoint {

        public static void main(String[] args) {
            FlinkSecurityManager.forceProcessExit(FORCED_EXIT_CODE);

            System.exit(0);
        }
    }

    /**
     * An entry point that expects enabling the security manager to fail with a descriptive error
     * because the JVM does not allow installing one.
     */
    public static final class UnavailableSecurityManagerEntryPoint {

        public static void main(String[] args) {
            try {
                interceptUserSystemExit();
            } catch (IllegalConfigurationException e) {
                System.err.println("Caught exception: " + e.getMessage());
                if (e.getMessage().contains("JEP 486")
                        && e.getCause() instanceof UnsupportedOperationException) {
                    FlinkSecurityManager.forceProcessExit(FORCED_EXIT_CODE);
                }
            }

            System.exit(1);
        }
    }
}
