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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A testing {@link ClusterEntrypoint} instance running in a separate JVM. */
public class TestingClusterEntrypointProcess extends TestJvmProcess {

    private final File markerFile;

    public TestingClusterEntrypointProcess(File markerFile) throws Exception {
        this.markerFile = checkNotNull(markerFile, "marker file");
    }

    @Override
    public String getName() {
        return getClass().getCanonicalName();
    }

    @Override
    public String[] getJvmArgs() {
        return new String[] {markerFile.getAbsolutePath()};
    }

    @Override
    public String getEntryPointClassName() {
        return TestingClusterEntrypointProcessEntryPoint.class.getName();
    }

    @Override
    public String toString() {
        return getClass().getCanonicalName();
    }

    /** Entrypoint for the testing cluster entrypoint process. */
    public static class TestingClusterEntrypointProcessEntryPoint {

        private static final Logger LOG =
                LoggerFactory.getLogger(TestingClusterEntrypointProcessEntryPoint.class);

        public static void main(String[] args) {
            try {
                final File markerFile = new File(args[0]);

                final Configuration config = new Configuration();
                config.setInteger(JobManagerOptions.PORT, 0);
                config.setString(RestOptions.BIND_PORT, "0");

                final TestingClusterEntrypoint clusterEntrypoint =
                        new TestingClusterEntrypoint(config, markerFile);

                SignalHandler.register(LOG);
                clusterEntrypoint.startCluster();
                TestJvmProcess.touchFile(markerFile);
                final int returnCode =
                        clusterEntrypoint.getTerminationFuture().get().processExitCode();
                System.exit(returnCode);
            } catch (Throwable t) {
                LOG.error("Failed to start TestingClusterEntrypoint process", t);
                System.exit(1);
            }
        }
    }

    private static class TestingClusterEntrypoint extends ClusterEntrypoint {

        private final File markerFile;

        protected TestingClusterEntrypoint(Configuration configuration, File markerFile) {
            super(configuration);
            this.markerFile = markerFile;
        }

        @Override
        protected DispatcherResourceManagerComponentFactory
                createDispatcherResourceManagerComponentFactory(Configuration configuration)
                        throws IOException {
            return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                    StandaloneResourceManagerFactory.getInstance());
        }

        @Override
        protected ExecutionGraphInfoStore createSerializableExecutionGraphStore(
                Configuration configuration, ScheduledExecutor scheduledExecutor)
                throws IOException {
            return new MemoryExecutionGraphInfoStore();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return super.closeAsync()
                    .thenRun(
                            () -> {
                                LOG.info("Deleting markerFile {}", markerFile);
                                IOUtils.deleteFileQuietly(markerFile.toPath());
                            });
        }
    }
}
