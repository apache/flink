/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.api;

import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Integration tests for {@link StreamExecutionEnvironment}. */
public class StreamExecutionEnvironmentITCase {

    // We use our own miniClusterResource because we wan't to connect to it using a remote executor.
    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void executeThrowsProgramInvocationException() throws Exception {
        UnmodifiableConfiguration clientConfiguration =
                miniClusterResource.getClientConfiguration();
        Configuration config = new Configuration(clientConfiguration);
        config.set(DeploymentOptions.TARGET, RemoteExecutor.NAME);
        config.setBoolean(DeploymentOptions.ATTACHED, true);

        // Create the execution environment explicitly from a Configuration so we know that we
        // don't get some other subclass. If we just did
        // StreamExecutionEnvironment.getExecutionEnvironment() we would get a
        // TestStreamEnvironment that the MiniClusterResource created. We want to test the behaviour
        // of the base environment, though.
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(config);

        env.fromElements("hello")
                .map(
                        in -> {
                            throw new RuntimeException("Failing");
                        })
                .print();

        thrown.expect(ProgramInvocationException.class);
        env.execute();
    }
}
