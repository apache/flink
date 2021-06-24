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
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.akka.AkkaFutureUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.Option;
import scala.Tuple2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test validates that the RPC service gives a good message when it cannot connect to an
 * RpcEndpoint.
 */
public class RpcConnectionTest extends TestLogger {

    @Test
    public void testConnectFailure() throws Exception {
        ActorSystem actorSystem = null;
        RpcService rpcService = null;
        try {
            actorSystem =
                    AkkaUtils.createActorSystem(
                            new Configuration(),
                            Option.<Tuple2<String, Object>>apply(new Tuple2<>("localhost", 0)));

            // we start the RPC service with a very long timeout to ensure that the test
            // can only pass if the connection problem is not recognized merely via a timeout
            Configuration configuration = new Configuration();
            configuration.set(AkkaOptions.ASK_TIMEOUT_DURATION, Duration.ofSeconds(10000000));
            rpcService =
                    new AkkaRpcService(
                            actorSystem,
                            AkkaRpcServiceConfiguration.fromConfiguration(configuration));

            CompletableFuture<TaskExecutorGateway> future =
                    rpcService.connect("foo.bar.com.test.invalid", TaskExecutorGateway.class);

            future.get(10000000, TimeUnit.SECONDS);
            fail("should never complete normally");
        } catch (TimeoutException e) {
            fail("should not fail with a generic timeout exception");
        } catch (ExecutionException e) {
            // that is what we want
            assertTrue(e.getCause() instanceof RpcConnectionException);
            assertTrue(
                    "wrong error message",
                    e.getCause().getMessage().contains("foo.bar.com.test.invalid"));
        } catch (Throwable t) {
            fail("wrong exception: " + t);
        } finally {
            final CompletableFuture<Void> rpcTerminationFuture;

            if (rpcService != null) {
                rpcTerminationFuture = rpcService.stopService();
            } else {
                rpcTerminationFuture = CompletableFuture.completedFuture(null);
            }

            final CompletableFuture<Terminated> actorSystemTerminationFuture;

            if (actorSystem != null) {
                actorSystemTerminationFuture = AkkaFutureUtils.toJava(actorSystem.terminate());
            } else {
                actorSystemTerminationFuture = CompletableFuture.completedFuture(null);
            }

            FutureUtils.waitForAll(
                            Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
                    .get();
        }
    }
}
