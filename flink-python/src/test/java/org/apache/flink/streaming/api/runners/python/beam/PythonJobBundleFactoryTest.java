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
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
        final BeamFnDataGrpc.BeamFnDataImplBase service =
                new BeamFnDataGrpc.BeamFnDataImplBase() {
                    @Override
                    public StreamObserver<BeamFnApi.Elements> data(
                            StreamObserver<BeamFnApi.Elements> response) {
                        responseObserver.set(response);
                        return new StreamObserver<BeamFnApi.Elements>() {
                            @Override
                            public void onNext(BeamFnApi.Elements value) {
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
            final StreamObserver<BeamFnApi.Elements> request =
                    BeamFnDataGrpc.newStub(channel)
                            .data(
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
            }
            clientClosed.get(10, TimeUnit.SECONDS);
            if (termination == RpcTermination.SERVER_ERROR_AFTER_CLOSE) {
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
        SERVER_ERROR_AFTER_CLOSE
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
}
