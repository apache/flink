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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the RpcEndpoint, its self gateways and MainThreadExecutor scheduling command. */
public class RpcEndpointTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(10L);
    private static ActorSystem actorSystem = null;
    private static RpcService rpcService = null;

    @BeforeClass
    public static void setup() {
        actorSystem = AkkaUtils.createDefaultActorSystem();
        rpcService =
                new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());
    }

    @AfterClass
    public static void teardown() throws Exception {

        final CompletableFuture<Void> rpcTerminationFuture = rpcService.stopService();
        final CompletableFuture<Terminated> actorSystemTerminationFuture =
                FutureUtils.toJava(actorSystem.terminate());

        FutureUtils.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
                .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
    }

    /**
     * Tests that we can obtain the self gateway from a RpcEndpoint and can interact with it via the
     * self gateway.
     */
    @Test
    public void testSelfGateway() throws Exception {
        int expectedValue = 1337;
        BaseEndpoint baseEndpoint = new BaseEndpoint(rpcService, expectedValue);

        try {
            baseEndpoint.start();

            BaseGateway baseGateway = baseEndpoint.getSelfGateway(BaseGateway.class);

            CompletableFuture<Integer> foobar = baseGateway.foobar();

            assertEquals(Integer.valueOf(expectedValue), foobar.get());
        } finally {
            RpcUtils.terminateRpcEndpoint(baseEndpoint, TIMEOUT);
        }
    }

    /**
     * Tests that we cannot accidentally obtain a wrong self gateway type which is not implemented
     * by the RpcEndpoint.
     */
    @Test(expected = RuntimeException.class)
    public void testWrongSelfGateway() throws Exception {
        int expectedValue = 1337;
        BaseEndpoint baseEndpoint = new BaseEndpoint(rpcService, expectedValue);

        try {
            baseEndpoint.start();

            DifferentGateway differentGateway = baseEndpoint.getSelfGateway(DifferentGateway.class);

            fail(
                    "Expected to fail with a RuntimeException since we requested the wrong gateway type.");
        } finally {
            RpcUtils.terminateRpcEndpoint(baseEndpoint, TIMEOUT);
        }
    }

    /**
     * Tests that we can extend existing RpcEndpoints and can communicate with them via the self
     * gateways.
     */
    @Test
    public void testEndpointInheritance() throws Exception {
        int foobar = 1;
        int barfoo = 2;
        String foo = "foobar";

        ExtendedEndpoint endpoint = new ExtendedEndpoint(rpcService, foobar, barfoo, foo);

        try {
            endpoint.start();

            BaseGateway baseGateway = endpoint.getSelfGateway(BaseGateway.class);
            ExtendedGateway extendedGateway = endpoint.getSelfGateway(ExtendedGateway.class);
            DifferentGateway differentGateway = endpoint.getSelfGateway(DifferentGateway.class);

            assertEquals(Integer.valueOf(foobar), baseGateway.foobar().get());
            assertEquals(Integer.valueOf(foobar), extendedGateway.foobar().get());

            assertEquals(Integer.valueOf(barfoo), extendedGateway.barfoo().get());
            assertEquals(foo, differentGateway.foo().get());
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }

    /** Tests that the RPC is running after it has been started. */
    @Test
    public void testRunningState()
            throws InterruptedException, ExecutionException, TimeoutException {
        RunningStateTestingEndpoint endpoint =
                new RunningStateTestingEndpoint(
                        rpcService, CompletableFuture.completedFuture(null));
        RunningStateTestingEndpointGateway gateway =
                endpoint.getSelfGateway(RunningStateTestingEndpointGateway.class);

        try {
            endpoint.start();
            assertThat(gateway.queryIsRunningFlag().get(), is(true));
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }

    /** Tests that the RPC is not running if it is being stopped. */
    @Test
    public void testNotRunningState()
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        RunningStateTestingEndpoint endpoint =
                new RunningStateTestingEndpoint(rpcService, stopFuture);
        RunningStateTestingEndpointGateway gateway =
                endpoint.getSelfGateway(RunningStateTestingEndpointGateway.class);

        endpoint.start();
        CompletableFuture<Void> terminationFuture = endpoint.closeAndWaitUntilOnStopCalled();

        assertThat(gateway.queryIsRunningFlag().get(), is(false));

        stopFuture.complete(null);
        terminationFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
    }

    public interface BaseGateway extends RpcGateway {
        CompletableFuture<Integer> foobar();
    }

    public interface ExtendedGateway extends BaseGateway {
        CompletableFuture<Integer> barfoo();
    }

    public interface DifferentGateway extends RpcGateway {
        CompletableFuture<String> foo();
    }

    public static class BaseEndpoint extends RpcEndpoint implements BaseGateway {

        private final int foobarValue;

        protected BaseEndpoint(RpcService rpcService) {
            super(rpcService);
            this.foobarValue = Integer.MAX_VALUE;
        }

        protected BaseEndpoint(RpcService rpcService, int foobarValue) {
            super(rpcService);

            this.foobarValue = foobarValue;
        }

        @Override
        public CompletableFuture<Integer> foobar() {
            return CompletableFuture.completedFuture(foobarValue);
        }
    }

    public static class ExtendedEndpoint extends BaseEndpoint
            implements ExtendedGateway, DifferentGateway {

        private final int barfooValue;

        private final String fooString;

        protected ExtendedEndpoint(
                RpcService rpcService, int foobarValue, int barfooValue, String fooString) {
            super(rpcService, foobarValue);

            this.barfooValue = barfooValue;
            this.fooString = fooString;
        }

        @Override
        public CompletableFuture<Integer> barfoo() {
            return CompletableFuture.completedFuture(barfooValue);
        }

        @Override
        public CompletableFuture<String> foo() {
            return CompletableFuture.completedFuture(fooString);
        }
    }

    public interface RunningStateTestingEndpointGateway extends RpcGateway {
        CompletableFuture<Boolean> queryIsRunningFlag();
    }

    private static final class RunningStateTestingEndpoint extends RpcEndpoint
            implements RunningStateTestingEndpointGateway {
        private final CountDownLatch onStopCalled;
        private final CompletableFuture<Void> stopFuture;

        RunningStateTestingEndpoint(RpcService rpcService, CompletableFuture<Void> stopFuture) {
            super(rpcService);
            this.stopFuture = stopFuture;
            this.onStopCalled = new CountDownLatch(1);
        }

        @Override
        public CompletableFuture<Void> onStop() {
            onStopCalled.countDown();
            return stopFuture;
        }

        CompletableFuture<Void> closeAndWaitUntilOnStopCalled() throws InterruptedException {
            CompletableFuture<Void> terminationFuture = closeAsync();
            onStopCalled.await();
            return terminationFuture;
        }

        public CompletableFuture<Boolean> queryIsRunningFlag() {
            return CompletableFuture.completedFuture(isRunning());
        }
    }

    /** Tests running the runnable in the main thread of the underlying RPC endpoint. */
    @Test
    public void testRunAsync() throws InterruptedException, ExecutionException, TimeoutException {
        final RpcEndpoint endpoint = new BaseEndpoint(rpcService);
        final CompletableFuture<Void> asyncExecutionFuture = new CompletableFuture<>();
        try {
            endpoint.start();
            endpoint.getMainThreadExecutor()
                    .runAsync(
                            () -> {
                                // no need to catch the validation failure
                                // if the validation fail, the future will never complete
                                endpoint.validateRunsInMainThread();
                                asyncExecutionFuture.complete(null);
                            });
            asyncExecutionFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit());
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }

    /**
     * Tests scheduling the runnable in the main thread of the underlying RPC endpoint, with a delay
     * of the given number of milliseconds.
     */
    @Test
    public void testScheduleRunAsync()
            throws InterruptedException, ExecutionException, TimeoutException {
        final RpcEndpoint endpoint = new BaseEndpoint(rpcService);
        final long expectedDelayMs = 100L;
        final CompletableFuture<Long> actualDelayMsFuture = new CompletableFuture<>();
        try {
            endpoint.start();
            final long startTime = System.currentTimeMillis();
            endpoint.getMainThreadExecutor()
                    .scheduleRunAsync(
                            () -> {
                                endpoint.validateRunsInMainThread();
                                actualDelayMsFuture.complete(
                                        System.currentTimeMillis() - startTime);
                            },
                            expectedDelayMs);
            final long actualDelayMs =
                    actualDelayMsFuture.get(expectedDelayMs * 2, TimeUnit.MILLISECONDS);
            assertTrue(actualDelayMs > expectedDelayMs * 0.5);
            assertTrue(actualDelayMs < expectedDelayMs * 1.5);
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }

    /** Tests executing the runnable in the main thread of the underlying RPC endpoint. */
    @Test
    public void testExecute() throws InterruptedException, ExecutionException, TimeoutException {
        final RpcEndpoint endpoint = new BaseEndpoint(rpcService);
        final CompletableFuture<Void> asyncExecutionFuture = new CompletableFuture<>();
        try {
            endpoint.start();
            endpoint.getMainThreadExecutor()
                    .execute(
                            () -> {
                                endpoint.validateRunsInMainThread();
                                asyncExecutionFuture.complete(null);
                            });
            asyncExecutionFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit());
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }

    /** Tests scheduling runnable with delay specified in number and TimeUnit. */
    @Test
    public void testScheduleRunnable()
            throws InterruptedException, ExecutionException, TimeoutException {
        final Time expectedDelay1 = Time.seconds(1);
        final Time expectedDelay2 = Time.milliseconds(500);
        final CompletableFuture<Long> actualDelayMsFuture1 = new CompletableFuture<>();
        final CompletableFuture<Long> actualDelayMsFuture2 = new CompletableFuture<>();
        final RpcEndpoint endpoint = new BaseEndpoint(rpcService);
        try {
            endpoint.start();
            final long startTime = System.currentTimeMillis();
            endpoint.getMainThreadExecutor()
                    .schedule(
                            () -> {
                                endpoint.validateRunsInMainThread();
                                actualDelayMsFuture1.complete(
                                        System.currentTimeMillis() - startTime);
                            },
                            expectedDelay1.getSize(),
                            expectedDelay1.getUnit());
            endpoint.getMainThreadExecutor()
                    .schedule(
                            () -> {
                                endpoint.validateRunsInMainThread();
                                actualDelayMsFuture2.complete(
                                        System.currentTimeMillis() - startTime);
                            },
                            expectedDelay2.getSize(),
                            expectedDelay2.getUnit());
            final long actualDelayMs1 =
                    actualDelayMsFuture1.get(
                            expectedDelay1.getSize() * 2, expectedDelay1.getUnit());
            final long actualDelayMs2 =
                    actualDelayMsFuture2.get(
                            expectedDelay2.getSize() * 2, expectedDelay2.getUnit());
            assertTrue(actualDelayMs1 > expectedDelay1.toMilliseconds() * 0.8);
            assertTrue(actualDelayMs1 < expectedDelay1.toMilliseconds() * 1.2);
            assertTrue(actualDelayMs2 > expectedDelay2.toMilliseconds() * 0.8);
            assertTrue(actualDelayMs2 < expectedDelay2.toMilliseconds() * 1.2);
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }

    /** Tests scheduling callable with delay specified in number and TimeUnit. */
    @Test
    public void testScheduleCallable()
            throws InterruptedException, ExecutionException, TimeoutException {
        final Time expectedDelay1 = Time.seconds(1);
        final Time expectedDelay2 = Time.milliseconds(500);
        final CompletableFuture<Long> actualDelayMsFuture1 = new CompletableFuture<>();
        final CompletableFuture<Long> actualDelayMsFuture2 = new CompletableFuture<>();
        final RpcEndpoint endpoint = new BaseEndpoint(rpcService);
        final int expectedInt = 12345;
        final String expectedString = "Flink";
        try {
            endpoint.start();
            final long startTime = System.currentTimeMillis();
            final ScheduledFuture<Integer> intScheduleFuture =
                    endpoint.getMainThreadExecutor()
                            .schedule(
                                    () -> {
                                        endpoint.validateRunsInMainThread();
                                        actualDelayMsFuture1.complete(
                                                System.currentTimeMillis() - startTime);
                                        return expectedInt;
                                    },
                                    expectedDelay1.getSize(),
                                    expectedDelay1.getUnit());
            final ScheduledFuture<String> stringScheduledFuture =
                    endpoint.getMainThreadExecutor()
                            .schedule(
                                    () -> {
                                        endpoint.validateRunsInMainThread();
                                        actualDelayMsFuture2.complete(
                                                System.currentTimeMillis() - startTime);
                                        return expectedString;
                                    },
                                    expectedDelay2.getSize(),
                                    expectedDelay2.getUnit());

            final long actualDelayMs1 =
                    actualDelayMsFuture1.get(
                            expectedDelay1.getSize() * 2, expectedDelay1.getUnit());
            final long actualDelayMs2 =
                    actualDelayMsFuture2.get(
                            expectedDelay2.getSize() * 2, expectedDelay2.getUnit());
            final int actualInteger =
                    intScheduleFuture.get(expectedDelay1.getSize() * 2, expectedDelay1.getUnit());
            final String actualString =
                    stringScheduledFuture.get(
                            expectedDelay2.getSize() * 2, expectedDelay2.getUnit());
            assertTrue(actualDelayMs1 > expectedDelay1.toMilliseconds() * 0.8);
            assertTrue(actualDelayMs1 < expectedDelay1.toMilliseconds() * 1.2);
            assertTrue(actualDelayMs2 > expectedDelay2.toMilliseconds() * 0.8);
            assertTrue(actualDelayMs2 < expectedDelay2.toMilliseconds() * 1.2);
            assertEquals(expectedInt, actualInteger);
            assertEquals(expectedString, actualString);
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }

    /**
     * Tests executing the callable in the main thread of the underlying RPC service, returning a
     * future for the result of the callable. If the callable is not completed within the given
     * timeout, then the future will be failed with a TimeoutException. This schedule method is
     * called directly from RpcEndpoint, MainThreadExecutor do not support this method.
     */
    @Test
    public void testCallAsync() throws InterruptedException, ExecutionException, TimeoutException {
        final RpcEndpoint endpoint = new BaseEndpoint(rpcService);
        final Integer expectedInteger = 12345;
        try {
            endpoint.start();
            final CompletableFuture<Integer> integerFuture =
                    endpoint.callAsync(
                            () -> {
                                endpoint.validateRunsInMainThread();
                                return expectedInteger;
                            },
                            TIMEOUT);
            assertEquals(expectedInteger, integerFuture.get(TIMEOUT.getSize(), TIMEOUT.getUnit()));
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }

    /**
     * Make the callable sleep some time more than specified timeout, so TimeoutException is
     * expected.
     */
    @Test
    public void testCallAsyncTimeout()
            throws InterruptedException, ExecutionException, TimeoutException {
        final RpcEndpoint endpoint = new BaseEndpoint(rpcService);
        final Time timeout = Time.milliseconds(100);
        try {
            endpoint.start();
            final CompletableFuture<Throwable> throwableFuture =
                    endpoint.callAsync(
                                    () -> {
                                        endpoint.validateRunsInMainThread();
                                        TimeUnit.MILLISECONDS.sleep(timeout.toMilliseconds() * 2);
                                        return 12345;
                                    },
                                    timeout)
                            .handle((ignore, throwable) -> throwable);
            final Throwable throwable =
                    throwableFuture.get(timeout.getSize() * 2, timeout.getUnit());
            assertTrue(throwable instanceof TimeoutException);
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint, TIMEOUT);
        }
    }
}
