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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.pekko.ScalaFutureUtils;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Terminated;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the context class loader handling in various parts of the Pekko rpc system.
 *
 * <p>The tests check cases where we call from the Pekko rpc system into Flink, in which case the
 * context class loader must be set to the Flink class loader. This ensures that the Pekko class
 * loader does not get accidentally leaked, e.g., via thread locals or thread pools on the Flink
 * side.
 */
class ContextClassLoadingSettingTest {

    // Many of the contained tests assert that a future is completed with a specific context class
    // loader by applying a synchronous operation.
    // If the initial future is completed by the time we apply the synchronous operation the test
    // thread will execute the operation instead. The tests are thus susceptible to timing issues.
    // We hence take a probabilistic approach: Assume that this timing is rare, guard these calls in
    // the test with a temporary class loader context, and assert that the actually used
    // context class loader is _either_ the one we truly expect or the temporary one.
    private static final ClassLoader testClassLoader =
            new URLClassLoader(new URL[0], ContextClassLoadingSettingTest.class.getClassLoader());

    private ClassLoader pretendFlinkClassLoader;
    private ActorSystem actorSystem;
    private PekkoRpcService pekkoRpcService;

    @BeforeEach
    void setup() {
        pretendFlinkClassLoader =
                new URLClassLoader(
                        new URL[0], ContextClassLoadingSettingTest.class.getClassLoader());
        actorSystem = PekkoUtils.createDefaultActorSystem();
        pekkoRpcService =
                new PekkoRpcService(
                        actorSystem,
                        PekkoRpcServiceConfiguration.defaultConfiguration(),
                        pretendFlinkClassLoader);

        PickyObject.classLoaderAssertion = this::assertIsFlinkClassLoader;
    }

    @AfterEach
    void shutdown() throws InterruptedException, ExecutionException {
        final CompletableFuture<Void> rpcTerminationFuture = pekkoRpcService.closeAsync();
        final CompletableFuture<Terminated> actorSystemTerminationFuture =
                ScalaFutureUtils.toJava(actorSystem.terminate());

        FutureUtils.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
                .get();

        actorSystem = null;
        pekkoRpcService = null;
    }

    @Test
    void testRpcService_ExecuteRunnableSetsFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<ClassLoader> contextClassLoader = new CompletableFuture<>();
        pekkoRpcService
                .getScheduledExecutor()
                .execute(
                        () ->
                                contextClassLoader.complete(
                                        Thread.currentThread().getContextClassLoader()));
        assertThat(contextClassLoader.get()).isSameAs(pretendFlinkClassLoader);
    }

    @Test
    void testRpcService_ScheduleCallableSetsFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {
        final ClassLoader contextClassLoader =
                pekkoRpcService
                        .getScheduledExecutor()
                        .schedule(
                                () -> Thread.currentThread().getContextClassLoader(),
                                0,
                                TimeUnit.MILLISECONDS)
                        .get();
        assertThat(contextClassLoader).isSameAs(pretendFlinkClassLoader);
    }

    @Test
    void testRpcService_ScheduleRunnableSetsFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<ClassLoader> contextClassLoader = new CompletableFuture<>();
        pekkoRpcService
                .getScheduledExecutor()
                .schedule(
                        () ->
                                contextClassLoader.complete(
                                        Thread.currentThread().getContextClassLoader()),
                        5,
                        TimeUnit.MILLISECONDS);
        assertThat(contextClassLoader.get()).isSameAs(pretendFlinkClassLoader);
    }

    @Test
    void testRpcService_ScheduleRunnableWithFixedRateSetsFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {
        final int numberOfScheduledRuns = 2;
        final List<ClassLoader> contextClassLoaders = new ArrayList<>(numberOfScheduledRuns);
        final CompletableFuture<Void> terminalFuture = new CompletableFuture<>();

        pekkoRpcService
                .getScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> {
                            if (contextClassLoaders.size() < numberOfScheduledRuns) {
                                contextClassLoaders.add(
                                        Thread.currentThread().getContextClassLoader());
                            } else {
                                terminalFuture.complete(null);
                                throw new RuntimeException("cancel task");
                            }
                        },
                        0,
                        1,
                        TimeUnit.MILLISECONDS);
        terminalFuture.get();
        assertEquals(numberOfScheduledRuns, contextClassLoaders.size());
        assertThat(contextClassLoaders)
                .allSatisfy(
                        classLoader -> assertThat(classLoader).isSameAs(pretendFlinkClassLoader));
    }

    @Test
    void testRpcService_ScheduleRunnableWithFixedDelaySetsFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {
        final int numberOfScheduledRuns = 2;

        final List<ClassLoader> contextClassLoaders = new ArrayList<>(numberOfScheduledRuns);
        final CompletableFuture<Void> terminalFuture = new CompletableFuture<>();
        pekkoRpcService
                .getScheduledExecutor()
                .scheduleWithFixedDelay(
                        () -> {
                            if (contextClassLoaders.size() < numberOfScheduledRuns) {
                                contextClassLoaders.add(
                                        Thread.currentThread().getContextClassLoader());
                            } else {
                                terminalFuture.complete(null);
                                throw new RuntimeException("cancel task");
                            }
                        },
                        0,
                        1,
                        TimeUnit.MILLISECONDS);
        terminalFuture.get();
        assertEquals(numberOfScheduledRuns, contextClassLoaders.size());
        assertThat(contextClassLoaders)
                .allSatisfy(
                        classLoader -> assertThat(classLoader).isSameAs(pretendFlinkClassLoader));
    }

    @Test
    void testRpcService_ConnectFutureCompletedWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService)) {

            final ClassLoader contextClassLoader =
                    runWithContextClassLoader(
                            () ->
                                    pekkoRpcService
                                            .connect(
                                                    testEndpoint.getAddress(),
                                                    TestEndpointGateway.class)
                                            .thenApply(
                                                    ignored ->
                                                            Thread.currentThread()
                                                                    .getContextClassLoader())
                                            .get(),
                            testClassLoader);
            assertIsFlinkClassLoader(contextClassLoader);
        }
    }

    @Test
    void testRpcService_TerminationFutureCompletedWithFlinkContextClassLoader() throws Exception {
        final ClassLoader contextClassLoader =
                runWithContextClassLoader(
                        () ->
                                pekkoRpcService
                                        .closeAsync()
                                        .thenApply(
                                                ignored ->
                                                        Thread.currentThread()
                                                                .getContextClassLoader())
                                        .get(),
                        testClassLoader);

        assertIsFlinkClassLoader(contextClassLoader);
    }

    @Test
    void testRpcActor_OnStartCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService)) {
            testEndpoint.start();
            assertIsFlinkClassLoader(testEndpoint.onStartClassLoader.get());
        }
    }

    @Test
    void testRpcActor_OnStopCalledWithFlinkContextClassLoader() throws Exception {
        final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService);
        testEndpoint.start();
        testEndpoint.close();

        assertIsFlinkClassLoader(testEndpoint.onStopClassLoader.get());
    }

    @Test
    void testRpcActor_CallAsyncCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService)) {
            testEndpoint.start();

            final CompletableFuture<ClassLoader> contextClassLoader = testEndpoint.doCallAsync();
            assertIsFlinkClassLoader(contextClassLoader.get());
        }
    }

    @Test
    void testRpcActor_RunAsyncCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService)) {
            testEndpoint.start();

            final CompletableFuture<ClassLoader> contextClassLoader = testEndpoint.doRunAsync();
            assertIsFlinkClassLoader(contextClassLoader.get());
        }
    }

    @Test
    void testRpcActor_RPCReturningVoidCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService)) {
            testEndpoint.start();

            final TestEndpointGateway testEndpointGateway =
                    pekkoRpcService
                            .connect(testEndpoint.getAddress(), TestEndpointGateway.class)
                            .get();
            testEndpointGateway.doSomethingWithoutReturningAnything();

            assertIsFlinkClassLoader(testEndpoint.voidOperationClassLoader.get());
        }
    }

    @Test
    void testRpcActor_RPCCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService)) {
            testEndpoint.start();

            final TestEndpointGateway testEndpointGateway =
                    pekkoRpcService
                            .connect(testEndpoint.getAddress(), TestEndpointGateway.class)
                            .get();
            final ClassLoader contextClassLoader =
                    testEndpointGateway.getContextClassLoader().get();
            assertIsFlinkClassLoader(contextClassLoader);
        }
    }

    @Test
    void testRpcInvocationHandler_RPCFutureCompletedWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService)) {
            testEndpoint.start();

            final TestEndpointGateway testEndpointGateway =
                    pekkoRpcService
                            .connect(testEndpoint.getAddress(), TestEndpointGateway.class)
                            .get();
            final CompletableFuture<ClassLoader> contextClassLoader =
                    runWithContextClassLoader(
                            () ->
                                    testEndpointGateway
                                            .doSomethingAsync()
                                            .thenApply(
                                                    ignored ->
                                                            Thread.currentThread()
                                                                    .getContextClassLoader()),
                            testClassLoader);
            testEndpoint.completeRPCFuture();

            assertIsFlinkClassLoader(contextClassLoader.get());
        }
    }

    @Test
    void testRpcInvocationHandler_ContextClassLoaderUsedForDeserialization() throws Exception {
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
                        PekkoRpcServiceConfiguration.defaultConfiguration(),
                        pretendFlinkClassLoader);

        try {
            final TestEndpoint rpcEndpoint =
                    new TestEndpoint(serverPekkoRpcService, new PickyObject());
            rpcEndpoint.start();

            final TestEndpointGateway rpcGateway =
                    rpcEndpoint.getSelfGateway(TestEndpointGateway.class);

            final TestEndpointGateway connect =
                    clientPekkoRpcService
                            .connect(rpcGateway.getAddress(), TestEndpointGateway.class)
                            .get();

            // if the wrong classloader is used the deserialization fails and get() throws an
            // exception
            connect.getPickyObject().get();
        } finally {
            RpcUtils.terminateRpcService(clientPekkoRpcService);
            RpcUtils.terminateRpcService(serverPekkoRpcService);
        }
    }

    @Test
    void testSupervisorActor_TerminationFutureCompletedWithFlinkContextClassLoader()
            throws Exception {
        final TestEndpoint testEndpoint = new TestEndpoint(pekkoRpcService);
        testEndpoint.start();

        final ClassLoader contextClassLoader =
                runWithContextClassLoader(
                        () ->
                                testEndpoint
                                        .closeAsync()
                                        .thenApply(
                                                ignored ->
                                                        Thread.currentThread()
                                                                .getContextClassLoader())
                                        .get(),
                        testClassLoader);

        assertIsFlinkClassLoader(contextClassLoader);
    }

    private void assertIsFlinkClassLoader(ClassLoader classLoader) {
        assertThat(classLoader)
                .satisfiesAnyOf(
                        cl -> assertThat(cl).isSameAs(pretendFlinkClassLoader),
                        cl -> assertThat(cl).isSameAs(testClassLoader));
    }

    private interface TestEndpointGateway extends RpcGateway {
        CompletableFuture<ClassLoader> getContextClassLoader();

        CompletableFuture<Void> doSomethingAsync();

        void doSomethingWithoutReturningAnything();

        CompletableFuture<PickyObject> getPickyObject();
    }

    /** An object that only allows deserialiation if its favorite ContextClassLoader is doing it. */
    private static class PickyObject implements Serializable {
        static Consumer<ClassLoader> classLoaderAssertion = null;

        private void readObject(ObjectInputStream aInputStream)
                throws ClassNotFoundException, IOException {
            classLoaderAssertion.accept(Thread.currentThread().getContextClassLoader());
        }
    }

    private static class TestEndpoint extends RpcEndpoint implements TestEndpointGateway {

        private final CompletableFuture<ClassLoader> onStartClassLoader = new CompletableFuture<>();
        private final CompletableFuture<ClassLoader> onStopClassLoader = new CompletableFuture<>();
        private final CompletableFuture<ClassLoader> voidOperationClassLoader =
                new CompletableFuture<>();
        private final CompletableFuture<Void> rpcResponseFuture = new CompletableFuture<>();

        @Nullable private final PickyObject pickyObject;

        protected TestEndpoint(RpcService rpcService) {
            this(rpcService, null);
        }

        protected TestEndpoint(RpcService rpcService, @Nullable PickyObject pickyObject) {
            super(rpcService);
            this.pickyObject = pickyObject;
        }

        @Override
        protected void onStart() throws Exception {
            onStartClassLoader.complete(Thread.currentThread().getContextClassLoader());
            super.onStart();
        }

        @Override
        protected CompletableFuture<Void> onStop() {
            onStopClassLoader.complete(Thread.currentThread().getContextClassLoader());
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> doSomethingAsync() {
            return rpcResponseFuture;
        }

        public CompletableFuture<ClassLoader> doCallAsync() {
            return callAsync(
                    () -> Thread.currentThread().getContextClassLoader(), Duration.ofSeconds(10));
        }

        public CompletableFuture<ClassLoader> doRunAsync() {
            final CompletableFuture<ClassLoader> contextClassLoader = new CompletableFuture<>();
            runAsync(
                    () ->
                            contextClassLoader.complete(
                                    Thread.currentThread().getContextClassLoader()));
            return contextClassLoader;
        }

        @Override
        public void doSomethingWithoutReturningAnything() {
            voidOperationClassLoader.complete(Thread.currentThread().getContextClassLoader());
        }

        @Override
        public CompletableFuture<PickyObject> getPickyObject() {
            return CompletableFuture.completedFuture(pickyObject);
        }

        public void completeRPCFuture() {
            rpcResponseFuture.complete(null);
        }

        @Override
        @Local
        public CompletableFuture<ClassLoader> getContextClassLoader() {
            return CompletableFuture.completedFuture(
                    Thread.currentThread().getContextClassLoader());
        }
    }
}
