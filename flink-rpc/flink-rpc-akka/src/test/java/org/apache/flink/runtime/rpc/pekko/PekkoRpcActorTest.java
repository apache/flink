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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.pekko.ScalaFutureUtils;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.EndpointNotStartedException;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link PekkoRpcActor}. */
class PekkoRpcActorTest {

    private static final Logger LOG = LoggerFactory.getLogger(PekkoRpcActorTest.class);

    // ------------------------------------------------------------------------
    //  shared test members
    // ------------------------------------------------------------------------

    private static Duration timeout = Duration.ofSeconds(10L);

    private static PekkoRpcService pekkoRpcService;

    @BeforeAll
    static void setup() {
        pekkoRpcService =
                new PekkoRpcService(
                        PekkoUtils.createLocalActorSystem(new Configuration()),
                        PekkoRpcServiceConfiguration.defaultConfiguration());
    }

    @AfterAll
    static void shutdown() throws InterruptedException, ExecutionException, TimeoutException {
        RpcUtils.terminateRpcService(pekkoRpcService);
    }

    /**
     * Tests that the rpc endpoint and the associated rpc gateway have the same addresses.
     *
     * @throws Exception
     */
    @Test
    void testAddressResolution() throws Exception {
        DummyRpcEndpoint rpcEndpoint = new DummyRpcEndpoint(pekkoRpcService);

        CompletableFuture<DummyRpcGateway> futureRpcGateway =
                pekkoRpcService.connect(rpcEndpoint.getAddress(), DummyRpcGateway.class);

        DummyRpcGateway rpcGateway = futureRpcGateway.get();

        assertThat(rpcGateway.getAddress()).isEqualTo(rpcEndpoint.getAddress());
    }

    /**
     * Tests that a {@link RpcConnectionException} is thrown if the rpc endpoint cannot be connected
     * to.
     */
    @Test
    void testFailingAddressResolution() throws Exception {
        CompletableFuture<DummyRpcGateway> futureRpcGateway =
                pekkoRpcService.connect("foobar", DummyRpcGateway.class);

        assertThatThrownBy(() -> futureRpcGateway.get())
                .hasCauseInstanceOf(RpcConnectionException.class);
    }

    /**
     * Tests that the {@link PekkoRpcActor} discards messages until the corresponding {@link
     * RpcEndpoint} has been started.
     */
    @Test
    void testMessageDiscarding() throws Exception {
        int expectedValue = 1337;

        DummyRpcEndpoint rpcEndpoint = new DummyRpcEndpoint(pekkoRpcService);

        DummyRpcGateway rpcGateway = rpcEndpoint.getSelfGateway(DummyRpcGateway.class);

        // this message should be discarded and complete with an exception
        assertThatThrownBy(() -> rpcGateway.foobar().get())
                .hasCauseInstanceOf(EndpointNotStartedException.class);

        // set a new value which we expect to be returned
        rpcEndpoint.setFoobar(expectedValue);

        // start the endpoint so that it can process messages
        rpcEndpoint.start();

        try {
            // send the rpc again
            CompletableFuture<Integer> result = rpcGateway.foobar();

            // now we should receive a result :-)
            Integer actualValue = result.get();

            assertThat(actualValue).isEqualTo(expectedValue);
        } finally {
            RpcUtils.terminateRpcEndpoint(rpcEndpoint);
        }
    }

    /**
     * Tests that we can wait for a RpcEndpoint to terminate.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    void testRpcEndpointTerminationFuture() throws Exception {
        final DummyRpcEndpoint rpcEndpoint = new DummyRpcEndpoint(pekkoRpcService);
        rpcEndpoint.start();

        CompletableFuture<Void> terminationFuture = rpcEndpoint.getTerminationFuture();

        assertThat(terminationFuture).isNotDone();

        CompletableFuture.runAsync(rpcEndpoint::closeAsync, pekkoRpcService.getScheduledExecutor());

        // wait until the rpc endpoint has terminated
        terminationFuture.get();
    }

    @Test
    void testExceptionPropagation() throws Exception {
        ExceptionalEndpoint rpcEndpoint = new ExceptionalEndpoint(pekkoRpcService);
        rpcEndpoint.start();

        ExceptionalGateway rpcGateway = rpcEndpoint.getSelfGateway(ExceptionalGateway.class);
        CompletableFuture<Integer> result = rpcGateway.doStuff();

        assertThatThrownBy(() -> result.get())
                .extracting(e -> e.getCause())
                .satisfies(
                        e ->
                                assertThat(e)
                                        .isInstanceOf(RuntimeException.class)
                                        .hasMessage("my super specific test exception"));
    }

    @Test
    void testExceptionPropagationFuturePiping() throws Exception {
        ExceptionalFutureEndpoint rpcEndpoint = new ExceptionalFutureEndpoint(pekkoRpcService);
        rpcEndpoint.start();

        ExceptionalGateway rpcGateway = rpcEndpoint.getSelfGateway(ExceptionalGateway.class);
        CompletableFuture<Integer> result = rpcGateway.doStuff();

        assertThatThrownBy(() -> result.get())
                .extracting(e -> e.getCause())
                .satisfies(
                        e -> assertThat(e).isInstanceOf(Exception.class).hasMessage("some test"));
    }

    /**
     * Tests that the {@link PekkoInvocationHandler} properly fails the returned future if the
     * response cannot be deserialized.
     */
    @Test
    void testResultFutureFailsOnDeserializationError() throws Exception {
        // setup 2 actor systems and rpc services that support remote connections (for which RPCs go
        // through serialization)
        final PekkoRpcService serverPekkoRpcService =
                new PekkoRpcService(
                        PekkoUtils.createActorSystem(
                                "serverActorSystem",
                                PekkoUtils.getConfig(
                                        new Configuration(), new HostAndPort("localhost", 0))),
                        PekkoRpcServiceConfiguration.defaultConfiguration());

        final PekkoRpcService clientPekkoRpcService =
                new PekkoRpcService(
                        PekkoUtils.createActorSystem(
                                "clientActorSystem",
                                PekkoUtils.getConfig(
                                        new Configuration(), new HostAndPort("localhost", 0))),
                        PekkoRpcServiceConfiguration.defaultConfiguration());

        try {
            final DeserializatonFailingEndpoint rpcEndpoint =
                    new DeserializatonFailingEndpoint(serverPekkoRpcService);
            rpcEndpoint.start();

            final DeserializatonFailingGateway rpcGateway =
                    rpcEndpoint.getSelfGateway(DeserializatonFailingGateway.class);

            final DeserializatonFailingGateway connect =
                    clientPekkoRpcService
                            .connect(rpcGateway.getAddress(), DeserializatonFailingGateway.class)
                            .get();

            assertThatFuture(connect.doStuff())
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(RpcException.class);
        } finally {
            RpcUtils.terminateRpcService(clientPekkoRpcService);
            RpcUtils.terminateRpcService(serverPekkoRpcService);
        }
    }

    /** Tests that exception thrown in the onStop method are returned by the termination future. */
    @Test
    void testOnStopExceptionPropagation() throws Exception {
        FailingOnStopEndpoint rpcEndpoint =
                new FailingOnStopEndpoint(pekkoRpcService, "FailingOnStopEndpoint");
        rpcEndpoint.start();

        CompletableFuture<Void> terminationFuture = rpcEndpoint.closeAsync();

        assertThatThrownBy(terminationFuture::get)
                .hasCauseInstanceOf(FailingOnStopEndpoint.OnStopException.class);
    }

    /** Checks that the onStop callback is executed within the main thread. */
    @Test
    void testOnStopExecutedByMainThread() throws Exception {
        SimpleRpcEndpoint simpleRpcEndpoint =
                new SimpleRpcEndpoint(pekkoRpcService, "SimpleRpcEndpoint");
        simpleRpcEndpoint.start();

        CompletableFuture<Void> terminationFuture = simpleRpcEndpoint.closeAsync();

        // check that we executed the onStop method in the main thread, otherwise an exception
        // would be thrown here.
        terminationFuture.get();
    }

    /** Tests that actors are properly terminated when the {@link PekkoRpcService} is shut down. */
    @Test
    void testActorTerminationWhenServiceShutdown() throws Exception {
        final ActorSystem rpcActorSystem = PekkoUtils.createDefaultActorSystem();
        final RpcService rpcService =
                new PekkoRpcService(
                        rpcActorSystem, PekkoRpcServiceConfiguration.defaultConfiguration());

        try {
            SimpleRpcEndpoint rpcEndpoint =
                    new SimpleRpcEndpoint(rpcService, SimpleRpcEndpoint.class.getSimpleName());

            rpcEndpoint.start();

            CompletableFuture<Void> terminationFuture = rpcEndpoint.getTerminationFuture();

            rpcService.closeAsync();

            terminationFuture.get();
        } finally {
            rpcActorSystem.terminate();
            ScalaFutureUtils.toJava(rpcActorSystem.whenTerminated()).get();
        }
    }

    /**
     * Tests that the {@link PekkoRpcActor} only completes after the asynchronous post stop action
     * has completed.
     */
    @Test
    void testActorTerminationWithAsynchronousOnStopAction() throws Exception {
        final CompletableFuture<Void> onStopFuture = new CompletableFuture<>();
        final AsynchronousOnStopEndpoint endpoint =
                new AsynchronousOnStopEndpoint(pekkoRpcService, onStopFuture);

        try {
            endpoint.start();

            final CompletableFuture<Void> terminationFuture = endpoint.closeAsync();

            assertThat(terminationFuture).isNotDone();

            onStopFuture.complete(null);

            // the onStopFuture completion should allow the endpoint to terminate
            terminationFuture.get();
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint);
        }
    }

    /**
     * Tests that we can still run commands via the main thread executor when the onStop method is
     * called.
     */
    @Test
    void testMainThreadExecutionOnStop() throws Exception {
        final MainThreadExecutorOnStopEndpoint endpoint =
                new MainThreadExecutorOnStopEndpoint(pekkoRpcService);

        try {
            endpoint.start();

            CompletableFuture<Void> terminationFuture = endpoint.closeAsync();

            terminationFuture.get();
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint);
        }
    }

    /** Tests that when the onStop future completes that no other messages will be processed. */
    @Test
    void testOnStopFutureCompletionDirectlyTerminatesRpcActor() throws Exception {
        final CompletableFuture<Void> onStopFuture = new CompletableFuture<>();
        final TerminatingAfterOnStopFutureCompletionEndpoint endpoint =
                new TerminatingAfterOnStopFutureCompletionEndpoint(pekkoRpcService, onStopFuture);

        try {
            endpoint.start();

            final AsyncOperationGateway asyncOperationGateway =
                    endpoint.getSelfGateway(AsyncOperationGateway.class);

            final CompletableFuture<Void> terminationFuture = endpoint.closeAsync();

            assertThat(terminationFuture).isNotDone();

            final CompletableFuture<Integer> firstAsyncOperationFuture =
                    asyncOperationGateway.asyncOperation(Time.fromDuration(timeout));
            final CompletableFuture<Integer> secondAsyncOperationFuture =
                    asyncOperationGateway.asyncOperation(Time.fromDuration(timeout));

            endpoint.awaitEnterAsyncOperation();

            // complete stop operation which should prevent the second async operation from being
            // executed
            onStopFuture.complete(null);

            // we can only complete the termination after the first async operation has been
            // completed
            assertThat(terminationFuture).isNotDone();

            endpoint.triggerUnblockAsyncOperation();

            assertThat(firstAsyncOperationFuture.get()).isEqualTo(42);

            terminationFuture.get();

            assertThat(endpoint.getNumberAsyncOperationCalls()).isEqualTo(1);
            assertThatFuture(secondAsyncOperationFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(RecipientUnreachableException.class);
        } finally {
            RpcUtils.terminateRpcEndpoint(endpoint);
        }
    }

    /**
     * Tests that the {@link RpcEndpoint#onStart()} method is called when the {@link RpcEndpoint} is
     * started.
     */
    @Test
    void testOnStartIsCalledWhenRpcEndpointStarts() throws Exception {
        final OnStartEndpoint onStartEndpoint = new OnStartEndpoint(pekkoRpcService, null);

        try {
            onStartEndpoint.start();
            onStartEndpoint.awaitUntilOnStartCalled();
        } finally {
            RpcUtils.terminateRpcEndpoint(onStartEndpoint);
        }
    }

    /** Tests that if onStart fails, then the endpoint terminates. */
    @Test
    void testOnStartFails() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        final OnStartEndpoint onStartEndpoint = new OnStartEndpoint(pekkoRpcService, testException);

        onStartEndpoint.start();
        onStartEndpoint.awaitUntilOnStartCalled();

        assertThatThrownBy(() -> onStartEndpoint.getTerminationFuture().get())
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                testException.getClass(), testException.getMessage()));
    }

    /**
     * Tests that multiple termination calls won't trigger the onStop action multiple times. Note
     * that this test is a probabilistic test which only fails sometimes without the fix. See
     * FLINK-16703.
     */
    @Test
    void callsOnStopOnlyOnce() throws Exception {
        final CompletableFuture<Void> onStopFuture = new CompletableFuture<>();
        final OnStopCountingRpcEndpoint endpoint =
                new OnStopCountingRpcEndpoint(pekkoRpcService, onStopFuture);

        try {
            endpoint.start();

            final PekkoBasedEndpoint selfGateway =
                    endpoint.getSelfGateway(PekkoBasedEndpoint.class);

            // try to terminate the actor twice
            selfGateway.getActorRef().tell(ControlMessages.TERMINATE, ActorRef.noSender());
            selfGateway.getActorRef().tell(ControlMessages.TERMINATE, ActorRef.noSender());

            endpoint.waitUntilOnStopHasBeenCalled();

            onStopFuture.complete(null);

            endpoint.getTerminationFuture().get();

            assertThat(endpoint.getNumOnStopCalls()).isEqualTo(1);
        } finally {
            onStopFuture.complete(null);
            RpcUtils.terminateRpcEndpoint(endpoint);
        }
    }

    @Test
    void canReuseEndpointNameAfterTermination() throws Exception {
        final String endpointName = "not_unique";
        try (SimpleRpcEndpoint simpleRpcEndpoint1 =
                new SimpleRpcEndpoint(pekkoRpcService, endpointName)) {

            simpleRpcEndpoint1.start();

            simpleRpcEndpoint1.closeAsync().join();

            try (SimpleRpcEndpoint simpleRpcEndpoint2 =
                    new SimpleRpcEndpoint(pekkoRpcService, endpointName)) {
                simpleRpcEndpoint2.start();

                assertThat(simpleRpcEndpoint2.getAddress())
                        .isEqualTo(simpleRpcEndpoint1.getAddress());
            }
        }
    }

    @Test
    void terminationFutureDoesNotBlockRpcEndpointCreation() throws Exception {
        try (final SimpleRpcEndpoint simpleRpcEndpoint =
                new SimpleRpcEndpoint(pekkoRpcService, "foobar")) {
            final CompletableFuture<Void> terminationFuture =
                    simpleRpcEndpoint.getTerminationFuture();

            // Creating a new RpcEndpoint within the termination future ensures that
            // completing the termination future won't block the RpcService
            final CompletableFuture<SimpleRpcEndpoint> foobar2 =
                    terminationFuture.thenApply(
                            ignored -> new SimpleRpcEndpoint(pekkoRpcService, "foobar2"));

            simpleRpcEndpoint.closeAsync();

            final SimpleRpcEndpoint simpleRpcEndpoint2 = foobar2.join();
            simpleRpcEndpoint2.close();
        }
    }

    @Test
    void resolvesRunningRpcActor() throws Exception {
        final String endpointName = "foobar";

        try (RpcEndpoint simpleRpcEndpoint1 = createRpcEndpointWithRandomNameSuffix(endpointName);
                RpcEndpoint simpleRpcEndpoint2 =
                        createRpcEndpointWithRandomNameSuffix(endpointName)) {

            simpleRpcEndpoint1.closeAsync().join();

            final String wildcardName = RpcServiceUtils.createWildcardName(endpointName);
            final String wildcardAddress = PekkoRpcServiceUtils.getLocalRpcUrl(wildcardName);
            final RpcGateway rpcGateway =
                    pekkoRpcService.connect(wildcardAddress, RpcGateway.class).join();

            assertThat(rpcGateway.getAddress()).isEqualTo(simpleRpcEndpoint2.getAddress());
        }
    }

    private RpcEndpoint createRpcEndpointWithRandomNameSuffix(String prefix) {
        return new SimpleRpcEndpoint(pekkoRpcService, RpcServiceUtils.createRandomName(prefix));
    }

    @Test
    void canRespondWithNullValueLocally() throws Exception {
        try (final NullRespondingEndpoint nullRespondingEndpoint =
                new NullRespondingEndpoint(pekkoRpcService)) {
            nullRespondingEndpoint.start();

            final NullRespondingGateway selfGateway =
                    nullRespondingEndpoint.getSelfGateway(NullRespondingGateway.class);

            final CompletableFuture<Integer> nullValuedResponseFuture = selfGateway.foobar();

            assertThat(nullValuedResponseFuture.join()).isNull();
        }
    }

    @Test
    void canRespondWithSynchronousNullValueLocally() throws Exception {
        try (final NullRespondingEndpoint nullRespondingEndpoint =
                new NullRespondingEndpoint(pekkoRpcService)) {
            nullRespondingEndpoint.start();

            final NullRespondingGateway selfGateway =
                    nullRespondingEndpoint.getSelfGateway(NullRespondingGateway.class);

            final Integer value = selfGateway.synchronousFoobar();

            assertThat(value).isNull();
        }
    }

    @Test
    void canRespondWithSerializedValueLocally() throws Exception {
        try (final SerializedValueRespondingEndpoint endpoint =
                new SerializedValueRespondingEndpoint(pekkoRpcService)) {
            endpoint.start();

            final SerializedValueRespondingGateway selfGateway =
                    endpoint.getSelfGateway(SerializedValueRespondingGateway.class);

            assertThat(selfGateway.getSerializedValueSynchronously())
                    .isEqualTo(SerializedValueRespondingEndpoint.SERIALIZED_VALUE);

            final CompletableFuture<SerializedValue<String>> responseFuture =
                    selfGateway.getSerializedValue();

            assertThat(responseFuture.get())
                    .isEqualTo(SerializedValueRespondingEndpoint.SERIALIZED_VALUE);
        }
    }

    /**
     * Verifies that actions scheduled via the main thread executor are eventually run while
     * adhering to the provided delays.
     *
     * <p>This test does not assert any upper bounds for how late something is run, because that
     * would make the test unstable in some environments, and there is no guarantee that such an
     * upper bound exists in the first place.
     *
     * <p>There are various failure points for this test, including the scheduling from the {@link
     * RpcEndpoint} to the {@link PekkoInvocationHandler}, the conversion of these calls by the
     * handler into Call-/RunAsync messages, the handling of said messages by the {@link
     * PekkoRpcActor} and in the case of RunAsync the actual scheduling by the underlying actor
     * system. This isn't an ideal test setup, but these components are difficult to test in
     * isolation.
     */
    @Test
    void testScheduling() throws ExecutionException, InterruptedException {
        final SchedulingRpcEndpoint endpoint = new SchedulingRpcEndpoint(pekkoRpcService);

        endpoint.start();

        final SchedulingRpcEndpointGateway gateway =
                endpoint.getSelfGateway(SchedulingRpcEndpointGateway.class);

        final CompletableFuture<Void> scheduleRunnableFuture = new CompletableFuture<>();
        final CompletableFuture<Void> scheduleCallableFuture = new CompletableFuture<>();
        final CompletableFuture<Void> executeFuture = new CompletableFuture<>();

        final long scheduleTime = System.nanoTime();
        gateway.schedule(scheduleRunnableFuture, scheduleCallableFuture, executeFuture);

        assertThat(scheduleRunnableFuture.thenApply(ignored -> System.nanoTime()).get())
                .isGreaterThanOrEqualTo(
                        scheduleTime
                                + Duration.ofMillis(SchedulingRpcEndpoint.DELAY_MILLIS).toNanos());
        assertThat(scheduleCallableFuture.thenApply(ignored -> System.nanoTime()).get())
                .isGreaterThanOrEqualTo(
                        scheduleTime
                                + Duration.ofMillis(SchedulingRpcEndpoint.DELAY_MILLIS).toNanos());
        // execute() calls don't have a delay attached, so we just check that it was run at all
        executeFuture.get();
    }

    // ------------------------------------------------------------------------
    //  Test Actors and Interfaces
    // ------------------------------------------------------------------------

    interface DummyRpcGateway extends RpcGateway {
        CompletableFuture<Integer> foobar();
    }

    static class DummyRpcEndpoint extends RpcEndpoint implements DummyRpcGateway {

        private volatile int foobar = 42;

        protected DummyRpcEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public CompletableFuture<Integer> foobar() {
            return CompletableFuture.completedFuture(foobar);
        }

        public void setFoobar(int value) {
            foobar = value;
        }
    }

    // ------------------------------------------------------------------------

    interface NullRespondingGateway extends DummyRpcGateway {
        Integer synchronousFoobar();
    }

    static class NullRespondingEndpoint extends RpcEndpoint implements NullRespondingGateway {

        protected NullRespondingEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public CompletableFuture<Integer> foobar() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Integer synchronousFoobar() {
            return null;
        }
    }

    // ------------------------------------------------------------------------

    interface SerializedValueRespondingGateway extends RpcGateway {
        CompletableFuture<SerializedValue<String>> getSerializedValue();

        SerializedValue<String> getSerializedValueSynchronously();
    }

    static class SerializedValueRespondingEndpoint extends RpcEndpoint
            implements SerializedValueRespondingGateway {
        static final SerializedValue<String> SERIALIZED_VALUE;

        static {
            try {
                SERIALIZED_VALUE = new SerializedValue<>("string-value");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public SerializedValueRespondingEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public CompletableFuture<SerializedValue<String>> getSerializedValue() {
            return CompletableFuture.completedFuture(SERIALIZED_VALUE);
        }

        @Override
        public SerializedValue<String> getSerializedValueSynchronously() {
            return SERIALIZED_VALUE;
        }
    }

    // ------------------------------------------------------------------------

    private interface ExceptionalGateway extends RpcGateway {
        CompletableFuture<Integer> doStuff();
    }

    private static class ExceptionalEndpoint extends RpcEndpoint implements ExceptionalGateway {

        protected ExceptionalEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public CompletableFuture<Integer> doStuff() {
            throw new RuntimeException("my super specific test exception");
        }
    }

    private static class ExceptionalFutureEndpoint extends RpcEndpoint
            implements ExceptionalGateway {

        protected ExceptionalFutureEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public CompletableFuture<Integer> doStuff() {
            final CompletableFuture<Integer> future = new CompletableFuture<>();

            // complete the future slightly in the, well, future...
            new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                    }
                    future.completeExceptionally(new Exception("some test"));
                }
            }.start();

            return future;
        }
    }

    // ------------------------------------------------------------------------

    private interface DeserializatonFailingGateway extends RpcGateway {
        CompletableFuture<DeserializationFailingObject> doStuff();
    }

    private static class DeserializatonFailingEndpoint extends RpcEndpoint
            implements DeserializatonFailingGateway {

        protected DeserializatonFailingEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public CompletableFuture<DeserializationFailingObject> doStuff() {
            return CompletableFuture.completedFuture(new DeserializationFailingObject());
        }
    }

    private static class DeserializationFailingObject implements Serializable {
        private void readObject(ObjectInputStream aInputStream)
                throws ClassNotFoundException, IOException {
            throw new ClassNotFoundException("test exception");
        }
    }

    // ------------------------------------------------------------------------

    private static class SimpleRpcEndpoint extends RpcEndpoint implements RpcGateway {

        protected SimpleRpcEndpoint(RpcService rpcService, String endpointId) {
            super(rpcService, endpointId);
        }
    }

    // ------------------------------------------------------------------------

    private static class FailingOnStopEndpoint extends RpcEndpoint implements RpcGateway {

        protected FailingOnStopEndpoint(RpcService rpcService, String endpointId) {
            super(rpcService, endpointId);
        }

        @Override
        public CompletableFuture<Void> onStop() {
            return FutureUtils.completedExceptionally(new OnStopException("Test exception."));
        }

        private static class OnStopException extends FlinkException {

            private static final long serialVersionUID = 6701096588415871592L;

            public OnStopException(String message) {
                super(message);
            }
        }
    }

    // ------------------------------------------------------------------------

    static class AsynchronousOnStopEndpoint extends RpcEndpoint {

        private final CompletableFuture<Void> onStopFuture;

        protected AsynchronousOnStopEndpoint(
                RpcService rpcService, CompletableFuture<Void> onStopFuture) {
            super(rpcService);

            this.onStopFuture = Preconditions.checkNotNull(onStopFuture);
        }

        @Override
        public CompletableFuture<Void> onStop() {
            return onStopFuture;
        }
    }

    // ------------------------------------------------------------------------

    private static class MainThreadExecutorOnStopEndpoint extends RpcEndpoint {

        protected MainThreadExecutorOnStopEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public CompletableFuture<Void> onStop() {
            return CompletableFuture.runAsync(() -> {}, getMainThreadExecutor());
        }
    }

    // ------------------------------------------------------------------------

    interface AsyncOperationGateway extends RpcGateway {
        CompletableFuture<Integer> asyncOperation(@RpcTimeout Time timeout);
    }

    private static class TerminatingAfterOnStopFutureCompletionEndpoint extends RpcEndpoint
            implements AsyncOperationGateway {

        private final CompletableFuture<Void> onStopFuture;

        private final OneShotLatch blockAsyncOperation = new OneShotLatch();

        private final OneShotLatch enterAsyncOperation = new OneShotLatch();

        private final AtomicInteger asyncOperationCounter = new AtomicInteger(0);

        protected TerminatingAfterOnStopFutureCompletionEndpoint(
                RpcService rpcService, CompletableFuture<Void> onStopFuture) {
            super(rpcService);
            this.onStopFuture = onStopFuture;
        }

        @Override
        public CompletableFuture<Integer> asyncOperation(Time timeout) {
            asyncOperationCounter.incrementAndGet();
            enterAsyncOperation.trigger();

            try {
                blockAsyncOperation.await();
            } catch (InterruptedException e) {
                throw new FlinkRuntimeException(e);
            }

            return CompletableFuture.completedFuture(42);
        }

        @Override
        public CompletableFuture<Void> onStop() {
            return onStopFuture;
        }

        void awaitEnterAsyncOperation() throws InterruptedException {
            enterAsyncOperation.await();
        }

        void triggerUnblockAsyncOperation() {
            blockAsyncOperation.trigger();
        }

        int getNumberAsyncOperationCalls() {
            return asyncOperationCounter.get();
        }
    }

    // ------------------------------------------------------------------------

    private static final class OnStartEndpoint extends RpcEndpoint {

        private final CountDownLatch countDownLatch;

        @Nullable private final Exception exception;

        OnStartEndpoint(RpcService rpcService, @Nullable Exception exception) {
            super(rpcService);
            this.countDownLatch = new CountDownLatch(1);
            this.exception = exception;
            // remove this endpoint from the rpc service once it terminates (normally or
            // exceptionally)
            getTerminationFuture().whenComplete((aVoid, throwable) -> closeAsync());
        }

        @Override
        public void onStart() throws Exception {
            countDownLatch.countDown();

            ExceptionUtils.tryRethrowException(exception);
        }

        public void awaitUntilOnStartCalled() throws InterruptedException {
            countDownLatch.await();
        }
    }

    // ------------------------------------------------------------------------

    private static final class OnStopCountingRpcEndpoint extends RpcEndpoint {

        private final AtomicInteger numOnStopCalls = new AtomicInteger(0);

        private final OneShotLatch onStopHasBeenCalled = new OneShotLatch();

        private final CompletableFuture<Void> onStopFuture;

        private OnStopCountingRpcEndpoint(
                RpcService rpcService, CompletableFuture<Void> onStopFuture) {
            super(rpcService);
            this.onStopFuture = onStopFuture;
        }

        @Override
        protected CompletableFuture<Void> onStop() {
            onStopHasBeenCalled.trigger();
            numOnStopCalls.incrementAndGet();
            return onStopFuture;
        }

        private int getNumOnStopCalls() {
            return numOnStopCalls.get();
        }

        private void waitUntilOnStopHasBeenCalled() throws InterruptedException {
            onStopHasBeenCalled.await();
        }
    }

    // ------------------------------------------------------------------------

    interface SchedulingRpcEndpointGateway extends RpcGateway {
        @Local
        void schedule(
                final CompletableFuture<Void> scheduleRunnableFuture,
                final CompletableFuture<Void> scheduleCallableFuture,
                final CompletableFuture<Void> executeFuture);
    }

    private static final class SchedulingRpcEndpoint extends RpcEndpoint
            implements SchedulingRpcEndpointGateway {

        static final int DELAY_MILLIS = 20;

        public SchedulingRpcEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public void schedule(
                final CompletableFuture<Void> scheduleRunnableFuture,
                final CompletableFuture<Void> scheduleCallableFuture,
                final CompletableFuture<Void> executeFuture) {
            getMainThreadExecutor()
                    .schedule(
                            () -> scheduleRunnableFuture.complete(null),
                            DELAY_MILLIS,
                            TimeUnit.MILLISECONDS);
            getMainThreadExecutor()
                    .schedule(
                            () -> {
                                scheduleCallableFuture.complete(null);
                                return null;
                            },
                            DELAY_MILLIS,
                            TimeUnit.MILLISECONDS);
            getMainThreadExecutor().execute(() -> executeFuture.complete(null));
        }
    }
}
