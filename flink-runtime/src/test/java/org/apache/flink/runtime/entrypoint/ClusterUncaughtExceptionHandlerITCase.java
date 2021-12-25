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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.runtime.util.ClusterUncaughtExceptionHandler;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/** Integration test to check exit behaviour for the {@link ClusterUncaughtExceptionHandler}. */
public class ClusterUncaughtExceptionHandlerITCase extends TestLogger {

    @Before
    public void ensureSupportedOS() {
        // based on the assumption in JvmExitOnFatalErrorTest, and manual testing on Mac, we do not
        // support all platforms (in particular not Windows)
        assumeTrue(OperatingSystem.isLinux() || OperatingSystem.isMac());
    }

    @Test
    public void testExitDueToUncaughtException() throws Exception {
        final ForcedJVMExitProcess testProcess =
                new ForcedJVMExitProcess(ClusterTestingEntrypoint.class);

        boolean success = false;

        testProcess.startProcess();
        try {
            testProcess.waitFor();
            int signedIntegerExitCode =
                    FatalExitExceptionHandler
                            .EXIT_CODE; // for FAIL mode, exit is done using this handler.
            int unsignedIntegerExitCode = ((byte) signedIntegerExitCode) & 0xFF;
            assertThat(testProcess.exitCode(), is(unsignedIntegerExitCode));
            success = true;
        } finally {
            if (!success) {
                testProcess.printProcessLog();
            }

            testProcess.destroy();
        }
    }

    private static class ClusterTestingEntrypoint extends ClusterEntrypoint {

        protected ClusterTestingEntrypoint(Configuration configuration) {
            super(configuration);
        }

        @Override
        protected DispatcherResourceManagerComponentFactory
                createDispatcherResourceManagerComponentFactory(Configuration configuration)
                        throws IOException {
            Thread t =
                    new Thread(
                            () -> {
                                throw new RuntimeException("Test exception");
                            });
            t.start();

            return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                    StandaloneResourceManagerFactory.getInstance());
        }

        @Override
        protected ExecutionGraphInfoStore createSerializableExecutionGraphStore(
                Configuration configuration, ScheduledExecutor scheduledExecutor)
                throws IOException {
            return null;
        }

        public static void main(String[] args) {
            try {
                final Configuration config = new Configuration();
                config.set(
                        ClusterOptions.UNCAUGHT_EXCEPTION_HANDLING,
                        ClusterOptions.UncaughtExceptionHandleMode.FAIL);
                ClusterTestingEntrypoint testingEntrypoint = new ClusterTestingEntrypoint(config);
                testingEntrypoint.startCluster();
            } catch (Throwable t) {
                System.exit(1);
            }
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
}
