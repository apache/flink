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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.akka.AkkaFutureUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.runWithContextClassLoader;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests the context class loader handling in various parts of the akka rpc system.
 *
 * <p>The tests check cases where we call from the akka rpc system into Flink, in which case the
 * context class loader must be set to the Flink class loader. This ensures that the Akka class
 * loader does not get accidentally leaked, e.g., via thread locals or thread pools on the Flink
 * side.
 */
public class ContextClassLoadingSettingTest extends TestLogger {

    private static final Time TIMEOUT = Time.milliseconds(10000L);

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
    private AkkaRpcService akkaRpcService;

    @Before
    public void setup() {
        pretendFlinkClassLoader =
                new URLClassLoader(
                        new URL[0], ContextClassLoadingSettingTest.class.getClassLoader());
        actorSystem = AkkaUtils.createDefaultActorSystem();
        akkaRpcService =
                new AkkaRpcService(
                        actorSystem,
                        AkkaRpcServiceConfiguration.defaultConfiguration(),
                        pretendFlinkClassLoader);

        PickyObject.classLoaderAssertion = this::assertIsFlinkClassLoader;
    }

    @After
    public void shutdown() throws InterruptedException, ExecutionException, TimeoutException {
        final CompletableFuture<Void> rpcTerminationFuture = akkaRpcService.stopService();
        final CompletableFuture<Terminated> actorSystemTerminationFuture =
                AkkaFutureUtils.toJava(actorSystem.terminate());

        FutureUtils.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
                .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

        actorSystem = null;
        akkaRpcService = null;
    }

    @Test
    public void testAkkaRpcService_ExecuteRunnableSetsFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<ClassLoader> contextClassLoader = new CompletableFuture<>();
        akkaRpcService.execute(
                () -> contextClassLoader.complete(Thread.currentThread().getContextClassLoader()));
        assertIsFlinkClassLoader(contextClassLoader.get());
    }

    @Test
    public void testAkkaRpcService_ExecuteCallableSetsFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<ClassLoader> contextClassLoader =
                akkaRpcService.execute(() -> Thread.currentThread().getContextClassLoader());
        assertIsFlinkClassLoader(contextClassLoader.get());
    }

    @Test
    public void testAkkaRpcService_ExecuteCallableResultCompletedWithFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {

        final CompletableFuture<Void> blocker = new CompletableFuture<>();

        final CompletableFuture<ClassLoader> contextClassLoader =
                runWithContextClassLoader(
                        () ->
                                akkaRpcService
                                        .execute((Callable<Void>) blocker::get)
                                        .thenApply(
                                                ignored ->
                                                        Thread.currentThread()
                                                                .getContextClassLoader()),
                        testClassLoader);
        blocker.complete(null);

        assertIsFlinkClassLoader(contextClassLoader.get());
    }

    @Test
    public void testAkkaRpcService_ScheduleSetsFlinkContextClassLoader()
            throws ExecutionException, InterruptedException {
        final CompletableFuture<ClassLoader> contextClassLoader = new CompletableFuture<>();
        akkaRpcService.scheduleRunnable(
                () -> contextClassLoader.complete(Thread.currentThread().getContextClassLoader()),
                5,
                TimeUnit.MILLISECONDS);
        assertThat(contextClassLoader.get(), is(pretendFlinkClassLoader));
    }

    @Test
    public void testAkkaRpcService_ConnectFutureCompletedWithFlinkContextClassLoader()
            throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService)) {

            final ClassLoader contextClassLoader =
                    runWithContextClassLoader(
                            () ->
                                    akkaRpcService
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
    public void testAkkaRpcService_TerminationFutureCompletedWithFlinkContextClassLoader()
            throws Exception {
        final ClassLoader contextClassLoader =
                runWithContextClassLoader(
                        () ->
                                akkaRpcService
                                        .stopService()
                                        .thenApply(
                                                ignored ->
                                                        Thread.currentThread()
                                                                .getContextClassLoader())
                                        .get(),
                        testClassLoader);

        assertIsFlinkClassLoader(contextClassLoader);
    }

    @Test
    public void testAkkaRpcActor_OnStartCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService)) {
            testEndpoint.start();
            assertIsFlinkClassLoader(testEndpoint.onStartClassLoader.get());
        }
    }

    @Test
    public void testAkkaRpcActor_OnStopCalledWithFlinkContextClassLoader() throws Exception {
        final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService);
        testEndpoint.start();
        testEndpoint.close();

        assertIsFlinkClassLoader(testEndpoint.onStopClassLoader.get());
    }

    @Test
    public void testAkkaRpcActor_CallAsyncCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService)) {
            testEndpoint.start();

            final CompletableFuture<ClassLoader> contextClassLoader = testEndpoint.doCallAsync();
            assertIsFlinkClassLoader(contextClassLoader.get());
        }
    }

    @Test
    public void testAkkaRpcActor_RunAsyncCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService)) {
            testEndpoint.start();

            final CompletableFuture<ClassLoader> contextClassLoader = testEndpoint.doRunAsync();
            assertIsFlinkClassLoader(contextClassLoader.get());
        }
    }

    @Test
    public void testAkkaRpcActor_RPCReturningVoidCalledWithFlinkContextClassLoader()
            throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService)) {
            testEndpoint.start();

            final TestEndpointGateway testEndpointGateway =
                    akkaRpcService
                            .connect(testEndpoint.getAddress(), TestEndpointGateway.class)
                            .get();
            testEndpointGateway.doSomethingWithoutReturningAnything();

            assertIsFlinkClassLoader(testEndpoint.voidOperationClassLoader.get());
        }
    }

    @Test
    public void testAkkaRpcActor_RPCCalledWithFlinkContextClassLoader() throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService)) {
            testEndpoint.start();

            final TestEndpointGateway testEndpointGateway =
                    akkaRpcService
                            .connect(testEndpoint.getAddress(), TestEndpointGateway.class)
                            .get();
            final ClassLoader contextClassLoader =
                    testEndpointGateway.getContextClassLoader().get();
            assertIsFlinkClassLoader(contextClassLoader);
        }
    }

    @Test
    public void testAkkaRpcInvocationHandler_RPCFutureCompletedWithFlinkContextClassLoader()
            throws Exception {
        try (final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService)) {
            testEndpoint.start();

            final TestEndpointGateway testEndpointGateway =
                    akkaRpcService
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
    public void testAkkaRpcInvocationHandler_ContextClassLoaderUsedForDeserialization()
            throws Exception {
        // setup 2 actor systems and rpc services that support remote connections (for which RPCs go
        // through serialization)
        final AkkaRpcService serverAkkaRpcService =
                new AkkaRpcService(
                        AkkaUtils.createActorSystem(
                                "serverActorSystem",
                                AkkaUtils.getAkkaConfig(
                                        new Configuration(), new HostAndPort("localhost", 0))),
                        AkkaRpcServiceConfiguration.defaultConfiguration());

        final AkkaRpcService clientAkkaRpcService =
                new AkkaRpcService(
                        AkkaUtils.createActorSystem(
                                "clientActorSystem",
                                AkkaUtils.getAkkaConfig(
                                        new Configuration(), new HostAndPort("localhost", 0))),
                        AkkaRpcServiceConfiguration.defaultConfiguration(),
                        pretendFlinkClassLoader);

        try {
            final TestEndpoint rpcEndpoint =
                    new TestEndpoint(serverAkkaRpcService, new PickyObject());
            rpcEndpoint.start();

            final TestEndpointGateway rpcGateway =
                    rpcEndpoint.getSelfGateway(TestEndpointGateway.class);

            final TestEndpointGateway connect =
                    clientAkkaRpcService
                            .connect(rpcGateway.getAddress(), TestEndpointGateway.class)
                            .get();

            // if the wrong classloader is used the deserialization fails and get() throws an
            // exception
            connect.getPickyObject().get();
        } finally {
            RpcUtils.terminateRpcService(clientAkkaRpcService, TIMEOUT);
            RpcUtils.terminateRpcService(serverAkkaRpcService, TIMEOUT);
        }
    }

    @Test
    public void testSupervisorActor_TerminationFutureCompletedWithFlinkContextClassLoader()
            throws Exception {
        final TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService);
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
        assertThat(classLoader, either(is(pretendFlinkClassLoader)).or(is(testClassLoader)));
    }

    private interface TestEndpointGateway extends RpcGateway {
        CompletableFuture<ClassLoader> getContextClassLoader();

        CompletableFuture<Void> doSomethingAsync();

        CompletableFuture<ClassLoader> doCallAsync();

        CompletableFuture<ClassLoader> doRunAsync();

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

        @Override
        public CompletableFuture<ClassLoader> doCallAsync() {
            return callAsync(
                    () -> Thread.currentThread().getContextClassLoader(),
                    Time.of(10, TimeUnit.SECONDS));
        }

        @Override
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
        public CompletableFuture<ClassLoader> getContextClassLoader() {
            return CompletableFuture.completedFuture(
                    Thread.currentThread().getContextClassLoader());
        }
    }
}
