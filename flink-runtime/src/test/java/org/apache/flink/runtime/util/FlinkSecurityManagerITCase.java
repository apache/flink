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
import org.apache.flink.runtime.security.FlinkSecurityManager;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/** Integration tests for the {@link FlinkSecurityManager}. */
public class FlinkSecurityManagerITCase extends TestLogger {

    @Before
    public void ensureSupportedOS() {
        // based on the assumption in JvmExitOnFatalErrorTest, and manual testing on Mac, we do not
        // support all platforms (in particular not Windows)
        assumeTrue(OperatingSystem.isLinux() || OperatingSystem.isMac());
    }

    @Test
    public void testForcedJVMExit() throws Exception {
        final ForcedJVMExitProcess testProcess =
                new ForcedJVMExitProcess(ForcedExitEntryPoint.class);

        testProcess.startProcess();
        try {
            testProcess.waitFor();
            assertThat(testProcess.exitCode(), is(222));
        } finally {
            testProcess.destroy();
        }
    }

    @Test
    public void testIgnoredJVMExit() throws Exception {
        final ForcedJVMExitProcess testProcess =
                new ForcedJVMExitProcess(IgnoredExitEntryPoint.class);

        testProcess.startProcess();
        try {
            testProcess.waitFor();
            assertThat(testProcess.exitCode(), is(0));
        } finally {
            testProcess.destroy();
        }
    }

    private static final class ForcedJVMExitProcess extends TestJvmProcess {

        private final Class<?> entryPointName;

        private ForcedJVMExitProcess(Class<?> entryPointName) throws Exception {
            this.entryPointName = entryPointName;
        }

        @Override
        public String getName() {
            return getEntryPointClassName();
        }

        @Override
        public String[] getJvmArgs() {
            return new String[0];
        }

        @Override
        public String getEntryPointClassName() {
            return entryPointName.getName();
        }
    }

    public static final class ForcedExitEntryPoint {

        public static void main(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            // configure FlinkSecurityManager to intercept calls to System.exit().
            configuration.set(
                    ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT,
                    ClusterOptions.UserSystemExitMode.THROW);
            FlinkSecurityManager.setFromConfiguration(configuration);

            FlinkSecurityManager.forceProcessExit(222);

            System.exit(0);
        }
    }

    public static final class IgnoredExitEntryPoint {

        public static void main(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            // configure FlinkSecurityManager to intercept calls to System.exit().
            configuration.set(
                    ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT,
                    ClusterOptions.UserSystemExitMode.THROW);
            FlinkSecurityManager.setFromConfiguration(configuration);

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
}
