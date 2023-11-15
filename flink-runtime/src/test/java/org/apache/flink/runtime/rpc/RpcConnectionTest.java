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

package org.apache.flink.runtime.rpc;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test validates that the RPC service gives a good message when it cannot connect to an
 * RpcEndpoint.
 */
class RpcConnectionTest {

    @Test
    void testConnectFailure() throws Exception {
        // we start the RPC service with a very long timeout to ensure that the test
        // can only pass if the connection problem is not recognized merely via a timeout
        Configuration configuration = new Configuration();
        configuration.set(AkkaOptions.ASK_TIMEOUT_DURATION, Duration.ofSeconds(10000000));

        try (RpcSystem rpcSystem = RpcSystem.load()) {
            final RpcService rpcService =
                    rpcSystem
                            .remoteServiceBuilder(configuration, null, "8000-9000")
                            .withBindAddress("localhost")
                            .withBindPort(0)
                            .createAndStart();

            final String invalidAddress =
                    rpcSystem.getRpcUrl(
                            rpcService.getAddress() + ".invalid",
                            rpcService.getPort(),
                            "foo",
                            AddressResolution.NO_ADDRESS_RESOLUTION,
                            new Configuration());

            assertThatThrownBy(
                            () ->
                                    rpcService
                                            .connect(invalidAddress, TaskExecutorGateway.class)
                                            .get(10000000, TimeUnit.SECONDS))
                    .cause()
                    .isInstanceOf(RpcConnectionException.class)
                    .hasMessageContaining(invalidAddress);
            rpcService.closeAsync().get();
        }
    }
}
