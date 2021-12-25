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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.CheckedSupplier;

import akka.actor.ActorSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for the {@link AkkaBootstrapTools}. */
public class AkkaBootstrapToolsTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(AkkaBootstrapToolsTest.class);

    /**
     * Tests that we can concurrently create two {@link ActorSystem} without port conflicts. This
     * effectively tests that we don't open a socket to check for a ports availability. See
     * FLINK-10580 for more details.
     */
    @Test
    public void testConcurrentActorSystemCreation() throws Exception {
        final int concurrentCreations = 10;
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrentCreations);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(concurrentCreations);

        try {
            final List<CompletableFuture<Void>> actorSystemFutures =
                    IntStream.range(0, concurrentCreations)
                            .mapToObj(
                                    ignored ->
                                            CompletableFuture.supplyAsync(
                                                    CheckedSupplier.unchecked(
                                                            () -> {
                                                                cyclicBarrier.await();

                                                                return AkkaBootstrapTools
                                                                        .startRemoteActorSystem(
                                                                                new Configuration(),
                                                                                "localhost",
                                                                                "0",
                                                                                LOG);
                                                            }),
                                                    executorService))
                            .map(
                                    // terminate ActorSystems
                                    actorSystemFuture ->
                                            actorSystemFuture.thenCompose(
                                                    AkkaUtils::terminateActorSystem))
                            .collect(Collectors.toList());

            FutureUtils.completeAll(actorSystemFutures).get();
        } finally {
            ExecutorUtils.gracefulShutdown(10000L, TimeUnit.MILLISECONDS, executorService);
        }
    }

    /**
     * Tests that the {@link ActorSystem} fails with an expressive exception if it cannot be
     * instantiated due to an occupied port.
     */
    @Test
    public void testActorSystemInstantiationFailureWhenPortOccupied() throws Exception {
        final ServerSocket portOccupier = new ServerSocket(0, 10, InetAddress.getByName("0.0.0.0"));

        try {
            final int port = portOccupier.getLocalPort();
            AkkaBootstrapTools.startRemoteActorSystem(
                    new Configuration(), "0.0.0.0", String.valueOf(port), LOG);
            fail("Expected to fail with a BindException");
        } catch (Exception e) {
            assertThat(ExceptionUtils.findThrowable(e, BindException.class).isPresent(), is(true));
        } finally {
            portOccupier.close();
        }
    }
}
