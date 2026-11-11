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

import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.BindableService;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Context;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Contexts;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingServerCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingServerCallListener;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ServerCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ServerCallHandler;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ServerInterceptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ServerInterceptors;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Notifies the runners sharing a Python worker when its data RPC terminates. */
final class PythonJobBundleFactory implements JobBundleFactory {
    private final JobBundleFactory delegate;
    private final DataChannelFailureHandler failureHandler;

    static PythonJobBundleFactory create(JobInfo jobInfo) {
        final DataChannelFailureHandler failureHandler = new DataChannelFailureHandler();
        final ProcessEnvironmentFactory.Provider provider =
                new ProcessEnvironmentFactory.Provider(
                        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())) {
                    @Override
                    public ServerFactory getServerFactory() {
                        return new DataServerFactory(super.getServerFactory(), failureHandler);
                    }
                };
        return new PythonJobBundleFactory(
                DefaultJobBundleFactory.create(
                        jobInfo,
                        Collections.singletonMap(
                                BeamUrns.getUrn(
                                        RunnerApi.StandardEnvironments.Environments.PROCESS),
                                provider)),
                failureHandler);
    }

    private PythonJobBundleFactory(
            JobBundleFactory delegate, DataChannelFailureHandler failureHandler) {
        this.delegate = delegate;
        this.failureHandler = failureHandler;
    }

    Runnable registerFailureListener(Consumer<Throwable> listener) {
        return failureHandler.register(listener);
    }

    @Override
    public StageBundleFactory forStage(ExecutableStage stage) {
        return delegate.forStage(stage);
    }

    @Override
    public void close() throws Exception {
        failureHandler.close();
        delegate.close();
    }

    /** Retains the first RPC failure and notifies registered runners, including late arrivals. */
    static final class DataChannelFailureHandler {
        private static final Logger LOG = LoggerFactory.getLogger(DataChannelFailureHandler.class);
        private final List<Consumer<Throwable>> listeners = new ArrayList<>();
        private Throwable failure;
        private boolean closed;

        Runnable register(Consumer<Throwable> listener) {
            final Throwable previousFailure;
            synchronized (this) {
                if (closed) {
                    throw new IllegalStateException("Python bundle factory is closed.");
                }
                previousFailure = failure;
                if (previousFailure == null) {
                    listeners.add(listener);
                }
            }
            if (previousFailure != null) {
                listener.accept(previousFailure);
            }
            return () -> {
                synchronized (this) {
                    listeners.remove(listener);
                }
            };
        }

        void fail(Throwable cause) {
            final List<Consumer<Throwable>> callbacks;
            synchronized (this) {
                if (closed || failure != null) {
                    return;
                }
                failure = cause;
                callbacks = new ArrayList<>(listeners);
                listeners.clear();
            }
            // Task failure may initiate resource disposal. Never invoke it while holding our lock.
            for (Consumer<Throwable> listener : callbacks) {
                try {
                    listener.accept(cause);
                } catch (Exception e) {
                    LOG.warn("Failed to notify a Python runner of a data RPC failure.", e);
                }
            }
        }

        synchronized void close() {
            closed = true;
            listeners.clear();
        }
    }

    /** Installs data RPC failure monitoring while preserving Beam's server and flow control. */
    static final class DataServerFactory extends ServerFactory implements ServerInterceptor {
        private final ServerFactory delegate;
        private final DataChannelFailureHandler failureHandler;

        DataServerFactory(ServerFactory delegate, DataChannelFailureHandler failureHandler) {
            this.delegate = delegate;
            this.failureHandler = failureHandler;
        }

        @Override
        public Server allocateAddressAndCreate(
                List<BindableService> services, Endpoints.ApiServiceDescriptor.Builder builder)
                throws IOException {
            return delegate.allocateAddressAndCreate(wrapServices(services), builder);
        }

        @Override
        public Server create(
                List<BindableService> services, Endpoints.ApiServiceDescriptor descriptor)
                throws IOException {
            return delegate.create(wrapServices(services), descriptor);
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
            final Context context = Context.current();
            final Context.CancellationListener cancellationListener =
                    cancelled ->
                            failureHandler.fail(
                                    Contexts.statusFromCancelled(cancelled).asRuntimeException());
            final ServerCall<ReqT, RespT> monitoredCall =
                    new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                        @Override
                        public void close(Status status, Metadata trailers) {
                            if (!status.isOk()) {
                                failureHandler.fail(status.asRuntimeException(trailers));
                            }
                            super.close(status, trailers);
                        }
                    };
            final ServerCall.Listener<ReqT> listener;
            try {
                listener = next.startCall(monitoredCall, headers);
            } catch (RuntimeException | Error failure) {
                failureHandler.fail(failure);
                throw failure;
            }
            // An already-cancelled context invokes the listener inline. Initialize the data service
            // first so a runner holding its lock can finish waiting for the connection.
            context.addListener(cancellationListener, Runnable::run);
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                    listener) {
                @Override
                public void onHalfClose() {
                    failUnexpectedCompletion();
                    super.onHalfClose();
                }

                @Override
                public void onCancel() {
                    try {
                        failureHandler.fail(
                                Status.CANCELLED
                                        .withDescription("Python data RPC was cancelled.")
                                        .asRuntimeException());
                        super.onCancel();
                    } finally {
                        context.removeListener(cancellationListener);
                    }
                }

                @Override
                public void onComplete() {
                    try {
                        // gRPC can close its underlying stream without calling ServerCall.close.
                        failUnexpectedCompletion();
                        super.onComplete();
                    } finally {
                        context.removeListener(cancellationListener);
                    }
                }
            };
        }

        private List<BindableService> wrapServices(List<BindableService> services) {
            return services.stream().map(this::wrapService).collect(Collectors.toList());
        }

        private BindableService wrapService(BindableService service) {
            if (!(service instanceof BeamFnDataGrpc.BeamFnDataImplBase)) {
                return service;
            }
            return () -> ServerInterceptors.intercept(service, this);
        }

        private void failUnexpectedCompletion() {
            // Flink does not enable environment expiration. Normal shutdown is managed
            // by the factory, which disables failure notifications before closing.
            failureHandler.fail(
                    new IOException("Python data RPC completed before the worker was closed."));
        }
    }
}
