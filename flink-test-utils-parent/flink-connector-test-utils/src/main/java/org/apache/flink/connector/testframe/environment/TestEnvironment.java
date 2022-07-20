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

package org.apache.flink.connector.testframe.environment;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connector.testframe.TestResource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Test environment for running Flink jobs.
 *
 * <p>The environment is bound with a Flink cluster, such as MiniCluster, Flink on container, or
 * even a running session cluster.
 */
@Experimental
public interface TestEnvironment extends TestResource {

    /**
     * Get an instance of {@link StreamExecutionEnvironment} for building and executing Flink jobs
     * based on the provided configuration.
     *
     * <p>Note that this environment should be bound with the Flink cluster, because this will be
     * the entrypoint to submit Flink jobs (via {@link StreamExecutionEnvironment#execute()}) in
     * test cases.
     *
     * @param envOptions options for the environment to satisfy
     */
    StreamExecutionEnvironment createExecutionEnvironment(TestEnvironmentSettings envOptions);

    /** Get endpoint of the test environment for connecting via REST API. */
    Endpoint getRestEndpoint();

    /**
     * Get a path in string for storing checkpoint and savepoint in the test environment.
     *
     * <p>Note that testing framework may have no access to this storage (e.g. Flink cluster is on
     * some cloud service and testing framework is executed locally). In test cases for testing
     * failover scenario, this path will be passed to cluster client directly for triggering
     * checkpoint / savepoint in this path and recovering from checkpoint / savepoint stored under
     * this path.
     */
    String getCheckpointUri();

    /** Endpoint with address and port of the test environment. */
    class Endpoint {
        private final String address;
        private final int port;

        public Endpoint(String address, int port) {
            this.address = address;
            this.port = port;
        }

        public String getAddress() {
            return address;
        }

        public int getPort() {
            return port;
        }
    }
}
