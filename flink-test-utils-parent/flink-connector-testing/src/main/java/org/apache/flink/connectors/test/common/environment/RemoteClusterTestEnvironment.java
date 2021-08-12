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

package org.apache.flink.connectors.test.common.environment;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Test environment for running test on a remote Flink cluster. */
@Experimental
public class RemoteClusterTestEnvironment implements TestEnvironment {

    private final String host;
    private final int port;
    private final String[] jarPath;
    private final Configuration config;

    /**
     * Construct a test environment for a remote Flink cluster.
     *
     * @param host Hostname of the remote JobManager
     * @param port REST port of the remote JobManager
     * @param jarPath Path of JARs to be shipped to Flink cluster
     */
    public RemoteClusterTestEnvironment(String host, int port, String... jarPath) {
        this(host, port, new Configuration(), jarPath);
    }

    /**
     * Construct a test environment for a remote Flink cluster with configurations.
     *
     * @param host Hostname of the remote JobManager
     * @param port REST port of the remote JobManager
     * @param config Configurations of the test environment
     * @param jarPath Path of JARs to be shipped to Flink cluster
     */
    public RemoteClusterTestEnvironment(
            String host, int port, Configuration config, String... jarPath) {
        this.host = host;
        this.port = port;
        this.config = config;
        this.jarPath = jarPath;
    }

    @Override
    public StreamExecutionEnvironment createExecutionEnvironment() {
        return StreamExecutionEnvironment.createRemoteEnvironment(host, port, jarPath);
    }

    @Override
    public void startUp() {}

    @Override
    public void tearDown() {}
}
