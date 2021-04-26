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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

/** Tests for the {@link ClusterEntrypoint}. */
public class ClusterEntrypointTest {

    @Test(expected = IllegalConfigurationException.class)
    public void testStandaloneSessionClusterEntrypointDeniedInReactiveMode() {
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
        new MockEntryPoint(configuration);
        fail("Entrypoint initialization is supposed to fail");
    }

    private static class MockEntryPoint extends ClusterEntrypoint {

        protected MockEntryPoint(Configuration configuration) {
            super(configuration);
        }

        @Override
        protected DispatcherResourceManagerComponentFactory
                createDispatcherResourceManagerComponentFactory(Configuration configuration)
                        throws IOException {
            throw new UnsupportedOperationException("Not needed for this test");
        }

        @Override
        protected ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
                Configuration configuration, ScheduledExecutor scheduledExecutor)
                throws IOException {
            throw new UnsupportedOperationException("Not needed for this test");
        }

        @Override
        protected boolean supportsReactiveMode() {
            return false;
        }
    }
}
