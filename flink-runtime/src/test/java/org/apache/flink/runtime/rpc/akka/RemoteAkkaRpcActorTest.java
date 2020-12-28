/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for remote AkkaRpcActors. */
public class RemoteAkkaRpcActorTest extends TestLogger {

    private static AkkaRpcService rpcService;
    private static AkkaRpcService otherRpcService;

    @BeforeClass
    public static void setupClass() throws Exception {
        final Configuration configuration = new Configuration();
        rpcService =
                AkkaRpcServiceUtils.createRemoteRpcService(
                        configuration, "localhost", "0", null, Optional.empty());

        otherRpcService =
                AkkaRpcServiceUtils.createRemoteRpcService(
                        configuration, "localhost", "0", null, Optional.empty());
    }

    @AfterClass
    public static void teardownClass()
            throws InterruptedException, ExecutionException, TimeoutException {
        RpcUtils.terminateRpcServices(Time.seconds(10), rpcService, otherRpcService);
    }

    @Test
    public void canRespondWithNullValueRemotely() throws Exception {
        try (final AkkaRpcActorTest.NullRespondingEndpoint nullRespondingEndpoint =
                new AkkaRpcActorTest.NullRespondingEndpoint(rpcService)) {
            nullRespondingEndpoint.start();

            final AkkaRpcActorTest.NullRespondingGateway rpcGateway =
                    otherRpcService
                            .connect(
                                    nullRespondingEndpoint.getAddress(),
                                    AkkaRpcActorTest.NullRespondingGateway.class)
                            .join();

            final CompletableFuture<Integer> nullValuedResponseFuture = rpcGateway.foobar();

            assertThat(nullValuedResponseFuture.join(), is(nullValue()));
        }
    }

    @Test
    public void canRespondWithSynchronousNullValueRemotely() throws Exception {
        try (final AkkaRpcActorTest.NullRespondingEndpoint nullRespondingEndpoint =
                new AkkaRpcActorTest.NullRespondingEndpoint(rpcService)) {
            nullRespondingEndpoint.start();

            final AkkaRpcActorTest.NullRespondingGateway rpcGateway =
                    otherRpcService
                            .connect(
                                    nullRespondingEndpoint.getAddress(),
                                    AkkaRpcActorTest.NullRespondingGateway.class)
                            .join();

            final Integer value = rpcGateway.synchronousFoobar();

            assertThat(value, is(nullValue()));
        }
    }
}
