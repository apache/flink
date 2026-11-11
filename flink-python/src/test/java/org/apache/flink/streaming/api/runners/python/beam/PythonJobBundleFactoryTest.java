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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.Context;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ServerCall;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.ClientCalls;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.ServerCalls;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PythonJobBundleFactoryTest {
    @ParameterizedTest
    @EnumSource(RpcTermination.class)
    void testRpcTerminationNotifiesAllRunners(RpcTermination termination) throws Exception {
        final PythonJobBundleFactory.DataChannelFailureHandler handler =
                new PythonJobBundleFactory.DataChannelFailureHandler();
        final CompletableFuture<Throwable> first = new CompletableFuture<>();
        final CompletableFuture<Throwable> second = new CompletableFuture<>();
        handler.register(first::complete);
        handler.register(second::complete);
        final AtomicReference<StreamObserver<BeamFnApi.Elements>> responseObserver =
                new AtomicReference<>();
        final CompletableFuture<Void> clientClosed = new CompletableFuture<>();
        final CountDownLatch received = new CountDownLatch(1);
        final CompletableFuture<Void> terminated = new CompletableFuture<>();
        final AtomicBoolean failOnNext = new AtomicBoolean();
        final AtomicBoolean corruptRequest = new AtomicBoolean();
        final BeamFnDataGrpc.BeamFnDataImplBase service =
                new BeamFnDataGrpc.BeamFnDataImplBase() {
                    @Override
                    public StreamObserver<BeamFnApi.Elements> data(
                            StreamObserver<BeamFnApi.Elements> response) {
                        responseObserver.set(response);
                        return new StreamObserver<BeamFnApi.Elements>() {
                            @Override
                            public void onNext(BeamFnApi.Elements value) {
                                if (failOnNext.get()) {
                                    throw new IllegalStateException("test data service failure");
                                }
                                received.countDown();
                            }

                            @Override
                            public void onError(Throwable error) {
                                terminated.complete(null);
                            }

                            @Override
                            public void onCompleted() {
                                response.onCompleted();
                                terminated.complete(null);
                            }
                        };
                    }
                };
        final Endpoints.ApiServiceDescriptor.Builder descriptor =
                Endpoints.ApiServiceDescriptor.newBuilder();
        final Server server =
                new PythonJobBundleFactory.DataServerFactory(ServerFactory.createDefault(), handler)
                        .allocateAddressAndCreate(Collections.singletonList(service), descriptor);
        final ManagedChannel channel =
                ManagedChannelBuilder.forTarget(descriptor.getUrl()).usePlaintext().build();
        try {
            final MethodDescriptor<BeamFnApi.Elements, BeamFnApi.Elements> method =
                    BeamFnDataGrpc.getDataMethod()
                            .toBuilder()
                            .setRequestMarshaller(
                                    new MethodDescriptor.Marshaller<BeamFnApi.Elements>() {
                                        @Override
                                        public InputStream stream(BeamFnApi.Elements value) {
                                            if (corruptRequest.get()) {
                                                // Invalid protobuf tag; parsing fails before
                                                // onNext.
                                                return new ByteArrayInputStream(new byte[] {0});
                                            }
                                            return BeamFnDataGrpc.getDataMethod()
                                                    .getRequestMarshaller().stream(value);
                                        }

                                        @Override
                                        public BeamFnApi.Elements parse(InputStream stream) {
                                            return BeamFnDataGrpc.getDataMethod()
                                                    .getRequestMarshaller()
                                                    .parse(stream);
                                        }
                                    })
                            .build();
            final StreamObserver<BeamFnApi.Elements> request =
                    ClientCalls.asyncBidiStreamingCall(
                            channel.newCall(method, CallOptions.DEFAULT),
                            new StreamObserver<BeamFnApi.Elements>() {
                                @Override
                                public void onNext(BeamFnApi.Elements value) {}

                                @Override
                                public void onError(Throwable error) {
                                    clientClosed.complete(null);
                                }

                                @Override
                                public void onCompleted() {
                                    clientClosed.complete(null);
                                }
                            });
            request.onNext(BeamFnApi.Elements.getDefaultInstance());
            assertThat(received.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(first).isNotDone();
            switch (termination) {
                case CLIENT_CANCEL:
                    request.onError(new IOException("test data RPC failure"));
                    terminated.get(10, TimeUnit.SECONDS);
                    break;
                case CLIENT_COMPLETE:
                    request.onCompleted();
                    terminated.get(10, TimeUnit.SECONDS);
                    break;
                case SERVER_ERROR:
                    responseObserver.get().onError(new IOException("server data RPC failure"));
                    break;
                case SERVER_ERROR_AFTER_CLOSE:
                    handler.close();
                    responseObserver.get().onError(new IOException("server shutting down"));
                    break;
                case SERVER_COMPLETE:
                    responseObserver.get().onCompleted();
                    break;
                case SERVER_COMPLETE_AFTER_CLOSE:
                    handler.close();
                    responseObserver.get().onCompleted();
                    break;
                case SERVER_ON_NEXT_ERROR:
                    failOnNext.set(true);
                    request.onNext(BeamFnApi.Elements.getDefaultInstance());
                    break;
                case INVALID_REQUEST:
                    corruptRequest.set(true);
                    request.onNext(BeamFnApi.Elements.getDefaultInstance());
                    break;
            }
            clientClosed.get(10, TimeUnit.SECONDS);
            if (termination == RpcTermination.SERVER_ERROR_AFTER_CLOSE
                    || termination == RpcTermination.SERVER_COMPLETE_AFTER_CLOSE) {
                assertThat(first).isNotDone();
                assertThat(second).isNotDone();
            } else {
                assertThat(second.get(10, TimeUnit.SECONDS))
                        .isSameAs(first.get(10, TimeUnit.SECONDS));
            }
        } finally {
            handler.close();
            channel.shutdownNow();
            server.shutdownNow();
            assertThat(channel.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private enum RpcTermination {
        CLIENT_CANCEL,
        CLIENT_COMPLETE,
        SERVER_ERROR,
        SERVER_ERROR_AFTER_CLOSE,
        SERVER_COMPLETE,
        SERVER_COMPLETE_AFTER_CLOSE,
        SERVER_ON_NEXT_ERROR,
        INVALID_REQUEST
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testClientCancellationWhileReceiving(boolean factoryClosed) throws Exception {
        final PythonJobBundleFactory.DataChannelFailureHandler handler =
                new PythonJobBundleFactory.DataChannelFailureHandler();
        final CompletableFuture<Throwable> failure = new CompletableFuture<>();
        handler.register(failure::complete);
        final CountDownLatch receiving = new CountDownLatch(1);
        final CountDownLatch releaseReceiver = new CountDownLatch(1);
        final CompletableFuture<Void> contextCancelled = new CompletableFuture<>();
        final CompletableFuture<Void> terminated = new CompletableFuture<>();
        final BeamFnDataGrpc.BeamFnDataImplBase service =
                new BeamFnDataGrpc.BeamFnDataImplBase() {
                    @Override
                    public StreamObserver<BeamFnApi.Elements> data(
                            StreamObserver<BeamFnApi.Elements> response) {
                        Context.current()
                                .addListener(
                                        ignored -> contextCancelled.complete(null), Runnable::run);
                        return new StreamObserver<BeamFnApi.Elements>() {
                            @Override
                            public void onNext(BeamFnApi.Elements value) {
                                receiving.countDown();
                                try {
                                    releaseReceiver.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public void onError(Throwable error) {
                                terminated.complete(null);
                            }

                            @Override
                            public void onCompleted() {
                                terminated.complete(null);
                            }
                        };
                    }
                };
        final Endpoints.ApiServiceDescriptor.Builder descriptor =
                Endpoints.ApiServiceDescriptor.newBuilder();
        final Server server =
                new PythonJobBundleFactory.DataServerFactory(ServerFactory.createDefault(), handler)
                        .allocateAddressAndCreate(Collections.singletonList(service), descriptor);
        final ManagedChannel channel =
                ManagedChannelBuilder.forTarget(descriptor.getUrl()).usePlaintext().build();
        try {
            final StreamObserver<BeamFnApi.Elements> request =
                    BeamFnDataGrpc.newStub(channel)
                            .data(
                                    new StreamObserver<BeamFnApi.Elements>() {
                                        @Override
                                        public void onNext(BeamFnApi.Elements value) {}

                                        @Override
                                        public void onError(Throwable error) {}

                                        @Override
                                        public void onCompleted() {}
                                    });
            request.onNext(BeamFnApi.Elements.getDefaultInstance());
            assertThat(receiving.await(10, TimeUnit.SECONDS)).isTrue();
            if (factoryClosed) {
                handler.close();
            }
            request.onError(new IOException("test data RPC failure"));
            contextCancelled.get(10, TimeUnit.SECONDS);
            if (factoryClosed) {
                assertThat(failure).isNotDone();
            } else {
                // Notification must not wait for the serialized onNext callback to return.
                assertThat(failure.get(10, TimeUnit.SECONDS)).isNotNull();
            }
            assertThat(terminated).isNotDone();
            releaseReceiver.countDown();
            terminated.get(10, TimeUnit.SECONDS);
            if (factoryClosed) {
                assertThat(failure).isNotDone();
            }
        } finally {
            handler.close();
            releaseReceiver.countDown();
            channel.shutdownNow();
            server.shutdownNow();
            assertThat(channel.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(server.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testFirstFailureReachesLateRegistration() {
        final PythonJobBundleFactory.DataChannelFailureHandler handler =
                new PythonJobBundleFactory.DataChannelFailureHandler();
        final IOException first = new IOException("first");
        handler.fail(first);
        handler.fail(new IOException("second"));
        final AtomicReference<Throwable> observed = new AtomicReference<>();
        handler.register(observed::set);
        assertThat(observed).hasValue(first);
    }

    @Test
    void testRemovedListenerAndNormalShutdownDoNotFailTasks() {
        final PythonJobBundleFactory.DataChannelFailureHandler handler =
                new PythonJobBundleFactory.DataChannelFailureHandler();
        final AtomicInteger calls = new AtomicInteger();
        handler.register(error -> calls.incrementAndGet()).run();
        handler.fail(new IOException("disconnected"));
        assertThat(calls).hasValue(0);
        final PythonJobBundleFactory.DataChannelFailureHandler closing =
                new PythonJobBundleFactory.DataChannelFailureHandler();
        closing.register(error -> calls.incrementAndGet());
        closing.close();
        closing.fail(new IOException("server shut down"));
        assertThat(calls).hasValue(0);
    }

    @Test
    void testCancelledContextDoesNotBlockDataServiceInitialization() throws Exception {
        final PythonJobBundleFactory.DataChannelFailureHandler handler =
                new PythonJobBundleFactory.DataChannelFailureHandler();
        final PythonJobBundleFactory.DataServerFactory factory =
                new PythonJobBundleFactory.DataServerFactory(
                        ServerFactory.createDefault(), handler);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final GrpcDataService service =
                GrpcDataService.create(
                        PipelineOptionsFactory.create(),
                        executor,
                        OutboundObserverFactory.serverDirect());
        final Context.CancellableContext context = Context.ROOT.withCancellation();
        final IOException cause = new IOException("cancelled before data service initialization");
        final Object runnerLock = new Object();
        final CountDownLatch startingBundle = new CountDownLatch(1);
        final CompletableFuture<Throwable> notified = new CompletableFuture<>();
        handler.register(
                error -> {
                    synchronized (runnerLock) {
                        notified.complete(error);
                    }
                });
        try {
            // Runner.process holds its monitor while starting the first bundle.
            final Future<?> bundle =
                    executor.submit(
                            () -> {
                                synchronized (runnerLock) {
                                    startingBundle.countDown();
                                    service.send(
                                            LogicalEndpoint.data("bundle", "input"),
                                            ByteArrayCoder.of());
                                }
                            });
            assertThat(startingBundle.await(10, TimeUnit.SECONDS)).isTrue();
            context.cancel(cause);
            final Future<?> call =
                    executor.submit(
                            () ->
                                    context.call(
                                            () ->
                                                    factory.interceptCall(
                                                            new TestingServerCall(),
                                                            new Metadata(),
                                                            ServerCalls.asyncBidiStreamingCall(
                                                                    service::data))));
            bundle.get(10, TimeUnit.SECONDS);
            call.get(10, TimeUnit.SECONDS);
            assertThat(notified.get(10, TimeUnit.SECONDS)).hasCause(cause);
        } finally {
            handler.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            context.close();
            service.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStartCallFailureNotifiesRunners(boolean fatal) {
        final PythonJobBundleFactory.DataChannelFailureHandler handler =
                new PythonJobBundleFactory.DataChannelFailureHandler();
        final PythonJobBundleFactory.DataServerFactory factory =
                new PythonJobBundleFactory.DataServerFactory(
                        ServerFactory.createDefault(), handler);
        final AtomicReference<Throwable> notified = new AtomicReference<>();
        handler.register(notified::set);
        final RuntimeException exception = new IllegalStateException("data service startup failed");
        final Error error = new AssertionError("data service startup failed");
        final Throwable expected = fatal ? error : exception;

        assertThatThrownBy(
                        () ->
                                factory.interceptCall(
                                        new TestingServerCall(),
                                        new Metadata(),
                                        (call, headers) -> {
                                            if (fatal) {
                                                throw error;
                                            }
                                            throw exception;
                                        }))
                .isSameAs(expected);
        assertThat(notified).hasValue(expected);
    }

    /** Runs the real gRPC streaming handler without transport scheduling in startup tests. */
    private static final class TestingServerCall
            extends ServerCall<BeamFnApi.Elements, BeamFnApi.Elements> {
        @Override
        public void request(int numMessages) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(BeamFnApi.Elements message) {}

        @Override
        public void close(Status status, Metadata trailers) {}

        @Override
        public boolean isCancelled() {
            return Context.current().isCancelled();
        }

        @Override
        public MethodDescriptor<BeamFnApi.Elements, BeamFnApi.Elements> getMethodDescriptor() {
            return BeamFnDataGrpc.getDataMethod();
        }
    }
}
