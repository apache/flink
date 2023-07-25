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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.pekko.ScalaFutureUtils;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Terminated;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link PekkoRpcService}. */
class PekkoRpcServiceTest {

    // ------------------------------------------------------------------------
    //  shared test members
    // ------------------------------------------------------------------------

    private static ActorSystem actorSystem;

    private static PekkoRpcService pekkoRpcService;

    @BeforeAll
    static void setup() {
        actorSystem = PekkoUtils.createDefaultActorSystem();
        pekkoRpcService =
                new PekkoRpcService(
                        actorSystem, PekkoRpcServiceConfiguration.defaultConfiguration());
    }

    @AfterAll
    static void shutdown() throws InterruptedException, ExecutionException, TimeoutException {
        final CompletableFuture<Void> rpcTerminationFuture = pekkoRpcService.closeAsync();
        final CompletableFuture<Terminated> actorSystemTerminationFuture =
                ScalaFutureUtils.toJava(actorSystem.terminate());

        FutureUtils.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
                .get();

        actorSystem = null;
        pekkoRpcService = null;
    }

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------
    @Test
    void testScheduleRunnable() throws Exception {
        final OneShotLatch latch = new OneShotLatch();
        final long delay = 100L;
        final long start = System.nanoTime();

        ScheduledFuture<?> scheduledFuture =
                pekkoRpcService
                        .getScheduledExecutor()
                        .schedule(latch::trigger, delay, TimeUnit.MILLISECONDS);

        scheduledFuture.get();

        assertThat(latch.isTriggered()).isTrue();
        final long stop = System.nanoTime();

        assertThat(((stop - start) / 1000000))
                .as("call was not properly delayed")
                .isGreaterThanOrEqualTo(delay);
    }

    /** Tests that the {@link PekkoRpcService} can execute runnables. */
    @Test
    void testExecuteRunnable() throws Exception {
        final OneShotLatch latch = new OneShotLatch();

        pekkoRpcService.getScheduledExecutor().execute(latch::trigger);

        latch.await(30L, TimeUnit.SECONDS);
    }

    @Test
    void testGetAddress() {
        assertThat(pekkoRpcService.getAddress())
                .isEqualTo(PekkoUtils.getAddress(actorSystem).host().get());
    }

    @Test
    void testGetPort() {
        assertThat(pekkoRpcService.getPort())
                .isEqualTo(PekkoUtils.getAddress(actorSystem).port().get());
    }

    /**
     * Tests a simple scheduled runnable being executed by the RPC services scheduled executor
     * service.
     */
    @Test
    void testScheduledExecutorServiceSimpleSchedule() throws Exception {
        ScheduledExecutor scheduledExecutor = pekkoRpcService.getScheduledExecutor();

        final OneShotLatch latch = new OneShotLatch();

        ScheduledFuture<?> future =
                scheduledExecutor.schedule(latch::trigger, 10L, TimeUnit.MILLISECONDS);

        future.get();

        // once the future is completed, then the latch should have been triggered
        assertThat(latch.isTriggered()).isTrue();
    }

    /**
     * Tests that the RPC service's scheduled executor service can execute runnables at a fixed
     * rate.
     */
    @Test
    void testScheduledExecutorServicePeriodicSchedule() throws Exception {
        ScheduledExecutor scheduledExecutor = pekkoRpcService.getScheduledExecutor();

        final int tries = 4;
        final long delay = 10L;
        final CountDownLatch countDownLatch = new CountDownLatch(tries);

        long currentTime = System.nanoTime();

        ScheduledFuture<?> future =
                scheduledExecutor.scheduleAtFixedRate(
                        countDownLatch::countDown, delay, delay, TimeUnit.MILLISECONDS);

        assertThat((Future) future).isNotDone();

        countDownLatch.await();

        // the future should not complete since we have a periodic task
        assertThat((Future) future).isNotDone();

        long finalTime = System.nanoTime() - currentTime;

        // the processing should have taken at least delay times the number of count downs.
        assertThat(finalTime).isGreaterThanOrEqualTo(tries * delay);

        future.cancel(true);
    }

    /**
     * Tests that the RPC service's scheduled executor service can execute runnable with a fixed
     * delay.
     */
    @Test
    void testScheduledExecutorServiceWithFixedDelaySchedule() throws Exception {
        ScheduledExecutor scheduledExecutor = pekkoRpcService.getScheduledExecutor();

        final int tries = 4;
        final long delay = 10L;
        final CountDownLatch countDownLatch = new CountDownLatch(tries);

        long currentTime = System.nanoTime();

        ScheduledFuture<?> future =
                scheduledExecutor.scheduleWithFixedDelay(
                        countDownLatch::countDown, delay, delay, TimeUnit.MILLISECONDS);

        assertThat((Future) future).isNotDone();

        countDownLatch.await();

        // the future should not complete since we have a periodic task
        assertThat((Future) future).isNotDone();

        long finalTime = System.nanoTime() - currentTime;

        // the processing should have taken at least delay times the number of count downs.
        assertThat(finalTime).isGreaterThanOrEqualTo(tries * delay);

        future.cancel(true);
    }

    /**
     * Tests that canceling the returned future will stop the execution of the scheduled runnable.
     */
    @Test
    void testScheduledExecutorServiceCancelWithFixedDelay() throws InterruptedException {
        ScheduledExecutor scheduledExecutor = pekkoRpcService.getScheduledExecutor();

        long delay = 10L;

        final OneShotLatch futureTask = new OneShotLatch();
        final OneShotLatch latch = new OneShotLatch();
        final OneShotLatch shouldNotBeTriggeredLatch = new OneShotLatch();

        ScheduledFuture<?> future =
                scheduledExecutor.scheduleWithFixedDelay(
                        () -> {
                            try {
                                if (futureTask.isTriggered()) {
                                    shouldNotBeTriggeredLatch.trigger();
                                } else {
                                    // first run
                                    futureTask.trigger();
                                    latch.await();
                                }
                            } catch (InterruptedException ignored) {
                                // ignore
                            }
                        },
                        delay,
                        delay,
                        TimeUnit.MILLISECONDS);

        // wait until we're in the runnable
        futureTask.await();

        // cancel the scheduled future
        future.cancel(false);

        latch.trigger();

        assertThatThrownBy(() -> shouldNotBeTriggeredLatch.await(5 * delay, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    /**
     * Tests that the {@link PekkoRpcService} terminates all its RpcEndpoints when shutting down.
     */
    @Test
    void testRpcServiceShutDownWithRpcEndpoints() throws Exception {
        final PekkoRpcService pekkoRpcService = startRpcService();

        try {
            final int numberActors = 5;

            final RpcServiceShutdownTestHelper rpcServiceShutdownTestHelper =
                    startStopNCountingAsynchronousOnStopEndpoints(pekkoRpcService, numberActors);

            for (CompletableFuture<Void> onStopFuture :
                    rpcServiceShutdownTestHelper.getStopFutures()) {
                onStopFuture.complete(null);
            }

            rpcServiceShutdownTestHelper.waitForRpcServiceTermination();
            assertThat(pekkoRpcService.getActorSystem().whenTerminated().isCompleted()).isTrue();
        } finally {
            RpcUtils.terminateRpcService(pekkoRpcService);
        }
    }

    /**
     * Tests that {@link PekkoRpcService} terminates all its RpcEndpoints and also stops the
     * underlying {@link ActorSystem} if one of the RpcEndpoints fails while stopping.
     */
    @Test
    void testRpcServiceShutDownWithFailingRpcEndpoints() throws Exception {
        final PekkoRpcService pekkoRpcService = startRpcService();

        final int numberActors = 5;

        final RpcServiceShutdownTestHelper rpcServiceShutdownTestHelper =
                startStopNCountingAsynchronousOnStopEndpoints(pekkoRpcService, numberActors);

        final Iterator<CompletableFuture<Void>> iterator =
                rpcServiceShutdownTestHelper.getStopFutures().iterator();

        for (int i = 0; i < numberActors - 1; i++) {
            iterator.next().complete(null);
        }

        iterator.next().completeExceptionally(new OnStopException("onStop exception occurred."));

        assertThatThrownBy(rpcServiceShutdownTestHelper::waitForRpcServiceTermination)
                .satisfies(FlinkAssertions.anyCauseMatches(OnStopException.class));

        assertThat(pekkoRpcService.getActorSystem().whenTerminated().isCompleted()).isTrue();
    }

    @Test
    void failsRpcResultImmediatelyIfEndpointIsStopped() throws Exception {
        try (final PekkoRpcActorTest.SerializedValueRespondingEndpoint endpoint =
                new PekkoRpcActorTest.SerializedValueRespondingEndpoint(pekkoRpcService)) {
            endpoint.start();

            final PekkoRpcActorTest.SerializedValueRespondingGateway gateway =
                    pekkoRpcService
                            .connect(
                                    endpoint.getAddress(),
                                    PekkoRpcActorTest.SerializedValueRespondingGateway.class)
                            .join();

            endpoint.close();

            assertThatThrownBy(() -> gateway.getSerializedValue().join())
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(RecipientUnreachableException.class));
        }
    }

    private static RpcServiceShutdownTestHelper startStopNCountingAsynchronousOnStopEndpoints(
            PekkoRpcService pekkoRpcService, int numberActors) throws InterruptedException {
        final Collection<CompletableFuture<Void>> onStopFutures = new ArrayList<>(numberActors);

        final CountDownLatch countDownLatch = new CountDownLatch(numberActors);

        for (int i = 0; i < numberActors; i++) {
            CompletableFuture<Void> onStopFuture = new CompletableFuture<>();
            final CountingAsynchronousOnStopEndpoint endpoint =
                    new CountingAsynchronousOnStopEndpoint(
                            pekkoRpcService, onStopFuture, countDownLatch);
            endpoint.start();
            onStopFutures.add(onStopFuture);
        }

        CompletableFuture<Void> terminationFuture = pekkoRpcService.closeAsync();

        countDownLatch.await();

        assertThat(terminationFuture).isNotDone();
        assertThat(pekkoRpcService.getActorSystem().whenTerminated().isCompleted()).isFalse();

        return new RpcServiceShutdownTestHelper(
                Collections.unmodifiableCollection(onStopFutures), terminationFuture);
    }

    private static class RpcServiceShutdownTestHelper {

        private final Collection<CompletableFuture<Void>> stopFutures;
        private final CompletableFuture<Void> terminationFuture;

        public RpcServiceShutdownTestHelper(
                Collection<CompletableFuture<Void>> stopFutures,
                CompletableFuture<Void> terminationFuture) {
            this.stopFutures = stopFutures;
            this.terminationFuture = terminationFuture;
        }

        public Collection<CompletableFuture<Void>> getStopFutures() {
            return stopFutures;
        }

        public void waitForRpcServiceTermination() throws ExecutionException, InterruptedException {
            terminationFuture.get();
        }
    }

    @Nonnull
    private PekkoRpcService startRpcService() {
        final ActorSystem actorSystem = PekkoUtils.createDefaultActorSystem();
        return new PekkoRpcService(
                actorSystem, PekkoRpcServiceConfiguration.defaultConfiguration());
    }

    private static class CountingAsynchronousOnStopEndpoint
            extends PekkoRpcActorTest.AsynchronousOnStopEndpoint {

        private final CountDownLatch countDownLatch;

        protected CountingAsynchronousOnStopEndpoint(
                RpcService rpcService,
                CompletableFuture<Void> onStopFuture,
                CountDownLatch countDownLatch) {
            super(rpcService, onStopFuture);
            this.countDownLatch = countDownLatch;
        }

        @Override
        public CompletableFuture<Void> onStop() {
            countDownLatch.countDown();
            return super.onStop();
        }
    }

    private static class OnStopException extends FlinkException {
        private static final long serialVersionUID = 7136609202083168954L;

        public OnStopException(String message) {
            super(message);
        }
    }
}
