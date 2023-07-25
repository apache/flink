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

import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.exceptions.EndpointNotStartedException;
import org.apache.flink.runtime.rpc.exceptions.HandshakeException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.runtime.rpc.messages.CallAsync;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.messages.RunAsync;
import org.apache.flink.runtime.rpc.pekko.exceptions.RpcInvalidStateException;
import org.apache.flink.runtime.rpc.pekko.exceptions.UnknownMessageException;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Status;
import org.apache.pekko.japi.pf.ReceiveBuilder;
import org.apache.pekko.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import static org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Pekko rpc actor which receives {@link RpcInvocation}, {@link RunAsync} and {@link CallAsync}
 * {@link ControlMessages} messages.
 *
 * <p>The {@link RpcInvocation} designates a rpc and is dispatched to the given {@link RpcEndpoint}
 * instance.
 *
 * <p>The {@link RunAsync} and {@link CallAsync} messages contain executable code which is executed
 * in the context of the actor thread.
 *
 * <p>The {@link ControlMessages} message controls the processing behaviour of the pekko rpc actor.
 * A {@link ControlMessages#START} starts processing incoming messages. A {@link
 * ControlMessages#STOP} message stops processing messages. All messages which arrive when the
 * processing is stopped, will be discarded.
 *
 * @param <T> Type of the {@link RpcEndpoint}
 */
class PekkoRpcActor<T extends RpcEndpoint & RpcGateway> extends AbstractActor {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /** the endpoint to invoke the methods on. */
    protected final T rpcEndpoint;

    private final ClassLoader flinkClassLoader;

    /** the helper that tracks whether calls come from the main thread. */
    private final MainThreadValidatorUtil mainThreadValidator;

    private final CompletableFuture<Boolean> terminationFuture;

    private final int version;

    private final long maximumFramesize;

    private final AtomicBoolean rpcEndpointStopped;

    private final boolean forceSerialization;

    private volatile RpcEndpointTerminationResult rpcEndpointTerminationResult;

    @Nonnull private State state;

    PekkoRpcActor(
            final T rpcEndpoint,
            final CompletableFuture<Boolean> terminationFuture,
            final int version,
            final long maximumFramesize,
            final boolean forceSerialization,
            final ClassLoader flinkClassLoader) {

        checkArgument(maximumFramesize > 0, "Maximum framesize must be positive.");
        this.rpcEndpoint = checkNotNull(rpcEndpoint, "rpc endpoint");
        this.flinkClassLoader = checkNotNull(flinkClassLoader);
        this.forceSerialization = forceSerialization;
        this.mainThreadValidator = new MainThreadValidatorUtil(rpcEndpoint);
        this.terminationFuture = checkNotNull(terminationFuture);
        this.version = version;
        this.maximumFramesize = maximumFramesize;
        this.rpcEndpointStopped = new AtomicBoolean(false);
        this.rpcEndpointTerminationResult =
                RpcEndpointTerminationResult.failure(
                        new RpcException(
                                String.format(
                                        "RpcEndpoint %s has not been properly stopped.",
                                        rpcEndpoint.getEndpointId())));
        this.state = StoppedState.STOPPED;
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();

        if (rpcEndpointTerminationResult.isSuccess()) {
            log.debug("The RpcEndpoint {} terminated successfully.", rpcEndpoint.getEndpointId());
            terminationFuture.complete(null);
        } else {
            log.info(
                    "The RpcEndpoint {} failed.",
                    rpcEndpoint.getEndpointId(),
                    rpcEndpointTerminationResult.getFailureCause());
            terminationFuture.completeExceptionally(rpcEndpointTerminationResult.getFailureCause());
        }

        state = state.finishTermination();
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(RemoteHandshakeMessage.class, this::handleHandshakeMessage)
                .match(ControlMessages.class, this::handleControlMessage)
                .matchAny(this::handleMessage)
                .build();
    }

    private void handleMessage(final Object message) {
        if (state.isRunning()) {
            mainThreadValidator.enterMainThread();

            try {
                handleRpcMessage(message);
            } finally {
                mainThreadValidator.exitMainThread();
            }
        } else {
            log.info(
                    "The rpc endpoint {} has not been started yet. Discarding message {} until processing is started.",
                    rpcEndpoint.getClass().getName(),
                    message);

            sendErrorIfSender(
                    new EndpointNotStartedException(
                            String.format(
                                    "Discard message %s, because the rpc endpoint %s has not been started yet.",
                                    message, rpcEndpoint.getAddress())));
        }
    }

    private void handleControlMessage(ControlMessages controlMessage) {
        try {
            switch (controlMessage) {
                case START:
                    state = state.start(this, flinkClassLoader);
                    break;
                case STOP:
                    state = state.stop();
                    break;
                case TERMINATE:
                    state = state.terminate(this, flinkClassLoader);
                    break;
                default:
                    handleUnknownControlMessage(controlMessage);
            }
        } catch (Exception e) {
            this.rpcEndpointTerminationResult = RpcEndpointTerminationResult.failure(e);
            throw e;
        }
    }

    private void handleUnknownControlMessage(ControlMessages controlMessage) {
        final String message =
                String.format(
                        "Received unknown control message %s. Dropping this message!",
                        controlMessage);
        log.warn(message);
        sendErrorIfSender(new UnknownMessageException(message));
    }

    protected void handleRpcMessage(Object message) {
        if (message instanceof RunAsync) {
            handleRunAsync((RunAsync) message);
        } else if (message instanceof CallAsync) {
            handleCallAsync((CallAsync) message);
        } else if (message instanceof RpcInvocation) {
            handleRpcInvocation((RpcInvocation) message);
        } else {
            log.warn(
                    "Received message of unknown type {} with value {}. Dropping this message!",
                    message.getClass().getName(),
                    message);

            sendErrorIfSender(
                    new UnknownMessageException(
                            "Received unknown message "
                                    + message
                                    + " of type "
                                    + message.getClass().getSimpleName()
                                    + '.'));
        }
    }

    private void handleHandshakeMessage(RemoteHandshakeMessage handshakeMessage) {
        if (!isCompatibleVersion(handshakeMessage.getVersion())) {
            sendErrorIfSender(
                    new HandshakeException(
                            String.format(
                                    "Version mismatch between source (%s) and target (%s) rpc component. Please verify that all components have the same version.",
                                    handshakeMessage.getVersion(), getVersion())));
        } else if (!isGatewaySupported(handshakeMessage.getRpcGateway())) {
            sendErrorIfSender(
                    new HandshakeException(
                            String.format(
                                    "The rpc endpoint does not support the gateway %s.",
                                    handshakeMessage.getRpcGateway().getSimpleName())));
        } else {
            getSender().tell(new Status.Success(HandshakeSuccessMessage.INSTANCE), getSelf());
        }
    }

    private boolean isGatewaySupported(Class<?> rpcGateway) {
        return rpcGateway.isAssignableFrom(rpcEndpoint.getClass());
    }

    private boolean isCompatibleVersion(int sourceVersion) {
        return sourceVersion == getVersion();
    }

    private int getVersion() {
        return version;
    }

    /**
     * Handle rpc invocations by looking up the rpc method on the rpc endpoint and calling this
     * method with the provided method arguments. If the method has a return value, it is returned
     * to the sender of the call.
     *
     * @param rpcInvocation Rpc invocation message
     */
    private void handleRpcInvocation(RpcInvocation rpcInvocation) {
        Method rpcMethod = null;

        try {
            String methodName = rpcInvocation.getMethodName();
            Class<?>[] parameterTypes = rpcInvocation.getParameterTypes();

            rpcMethod = lookupRpcMethod(methodName, parameterTypes);
        } catch (final NoSuchMethodException e) {
            log.error("Could not find rpc method for rpc invocation.", e);

            RpcConnectionException rpcException =
                    new RpcConnectionException("Could not find rpc method for rpc invocation.", e);
            getSender().tell(new Status.Failure(rpcException), getSelf());
        }

        if (rpcMethod != null) {
            try {
                // this supports declaration of anonymous classes
                rpcMethod.setAccessible(true);

                final Method capturedRpcMethod = rpcMethod;
                if (rpcMethod.getReturnType().equals(Void.TYPE)) {
                    // No return value to send back
                    runWithContextClassLoader(
                            () -> capturedRpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs()),
                            flinkClassLoader);
                } else {
                    final Object result;
                    try {
                        result =
                                runWithContextClassLoader(
                                        () ->
                                                capturedRpcMethod.invoke(
                                                        rpcEndpoint, rpcInvocation.getArgs()),
                                        flinkClassLoader);
                    } catch (InvocationTargetException e) {
                        log.debug(
                                "Reporting back error thrown in remote procedure {}", rpcMethod, e);

                        // tell the sender about the failure
                        getSender().tell(new Status.Failure(e.getTargetException()), getSelf());
                        return;
                    }

                    final String methodName = rpcMethod.getName();
                    final boolean isLocalRpcInvocation =
                            rpcMethod.getAnnotation(Local.class) != null;

                    if (result instanceof CompletableFuture) {
                        final CompletableFuture<?> responseFuture = (CompletableFuture<?>) result;
                        sendAsyncResponse(responseFuture, methodName, isLocalRpcInvocation);
                    } else {
                        sendSyncResponse(result, methodName, isLocalRpcInvocation);
                    }
                }
            } catch (Throwable e) {
                log.error("Error while executing remote procedure call {}.", rpcMethod, e);
                // tell the sender about the failure
                getSender().tell(new Status.Failure(e), getSelf());
            }
        }
    }

    private void sendSyncResponse(
            Object response, String methodName, boolean isLocalRpcInvocation) {
        if (isRemoteSender(getSender()) || (forceSerialization && !isLocalRpcInvocation)) {
            Either<RpcSerializedValue, RpcException> serializedResult =
                    serializeRemoteResultAndVerifySize(response, methodName);

            if (serializedResult.isLeft()) {
                getSender().tell(new Status.Success(serializedResult.left()), getSelf());
            } else {
                getSender().tell(new Status.Failure(serializedResult.right()), getSelf());
            }
        } else {
            getSender().tell(new Status.Success(response), getSelf());
        }
    }

    private void sendAsyncResponse(
            CompletableFuture<?> asyncResponse, String methodName, boolean isLocalRpcInvocation) {
        final ActorRef sender = getSender();
        Promise.DefaultPromise<Object> promise = new Promise.DefaultPromise<>();

        FutureUtils.assertNoException(
                asyncResponse.handle(
                        (value, throwable) -> {
                            if (throwable != null) {
                                promise.failure(throwable);
                            } else {
                                if (isRemoteSender(sender)
                                        || (forceSerialization && !isLocalRpcInvocation)) {
                                    Either<RpcSerializedValue, RpcException> serializedResult =
                                            serializeRemoteResultAndVerifySize(value, methodName);

                                    if (serializedResult.isLeft()) {
                                        promise.success(serializedResult.left());
                                    } else {
                                        promise.failure(serializedResult.right());
                                    }
                                } else {
                                    promise.success(new Status.Success(value));
                                }
                            }

                            // consume the provided throwable
                            return null;
                        }));

        Patterns.pipe(promise.future(), getContext().dispatcher()).to(sender);
    }

    private boolean isRemoteSender(ActorRef sender) {
        return !sender.path().address().hasLocalScope();
    }

    private Either<RpcSerializedValue, RpcException> serializeRemoteResultAndVerifySize(
            Object result, String methodName) {
        try {
            RpcSerializedValue serializedResult = RpcSerializedValue.valueOf(result);

            long resultSize = serializedResult.getSerializedDataLength();
            if (resultSize > maximumFramesize) {
                return Either.Right(
                        new RpcException(
                                "The method "
                                        + methodName
                                        + "'s result size "
                                        + resultSize
                                        + " exceeds the maximum size "
                                        + maximumFramesize
                                        + " ."));
            } else {
                return Either.Left(serializedResult);
            }
        } catch (IOException e) {
            return Either.Right(
                    new RpcException(
                            "Failed to serialize the result for RPC call : " + methodName + '.',
                            e));
        }
    }

    /**
     * Handle asynchronous {@link Callable}. This method simply executes the given {@link Callable}
     * in the context of the actor thread.
     *
     * @param callAsync Call async message
     */
    private void handleCallAsync(CallAsync callAsync) {
        try {
            Object result =
                    runWithContextClassLoader(
                            () -> callAsync.getCallable().call(), flinkClassLoader);

            getSender().tell(new Status.Success(result), getSelf());
        } catch (Throwable e) {
            getSender().tell(new Status.Failure(e), getSelf());
        }
    }

    /**
     * Handle asynchronous {@link Runnable}. This method simply executes the given {@link Runnable}
     * in the context of the actor thread.
     *
     * @param runAsync Run async message
     */
    private void handleRunAsync(RunAsync runAsync) {
        final long timeToRun = runAsync.getTimeNanos();
        final long delayNanos;

        if (timeToRun == 0 || (delayNanos = timeToRun - System.nanoTime()) <= 0) {
            // run immediately
            try {
                runWithContextClassLoader(() -> runAsync.getRunnable().run(), flinkClassLoader);
            } catch (Throwable t) {
                log.error("Caught exception while executing runnable in main thread.", t);
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            }
        } else {
            // schedule for later. send a new message after the delay, which will then be
            // immediately executed
            FiniteDuration delay = new FiniteDuration(delayNanos, TimeUnit.NANOSECONDS);
            RunAsync message = new RunAsync(runAsync.getRunnable(), timeToRun);

            final Object envelopedSelfMessage = envelopeSelfMessage(message);

            getContext()
                    .system()
                    .scheduler()
                    .scheduleOnce(
                            delay,
                            getSelf(),
                            envelopedSelfMessage,
                            getContext().dispatcher(),
                            ActorRef.noSender());
        }
    }

    /**
     * Look up the rpc method on the given {@link RpcEndpoint} instance.
     *
     * @param methodName Name of the method
     * @param parameterTypes Parameter types of the method
     * @return Method of the rpc endpoint
     * @throws NoSuchMethodException Thrown if the method with the given name and parameter types
     *     cannot be found at the rpc endpoint
     */
    private Method lookupRpcMethod(final String methodName, final Class<?>[] parameterTypes)
            throws NoSuchMethodException {
        return rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
    }

    /**
     * Send throwable to sender if the sender is specified.
     *
     * @param throwable to send to the sender
     */
    protected void sendErrorIfSender(Throwable throwable) {
        if (!getSender().equals(ActorRef.noSender())) {
            getSender().tell(new Status.Failure(throwable), getSelf());
        }
    }

    /**
     * Hook to envelope self messages.
     *
     * @param message to envelope
     * @return enveloped message
     */
    protected Object envelopeSelfMessage(Object message) {
        return message;
    }

    /** Stop the actor immediately. */
    private void stop(RpcEndpointTerminationResult rpcEndpointTerminationResult) {
        if (rpcEndpointStopped.compareAndSet(false, true)) {
            this.rpcEndpointTerminationResult = rpcEndpointTerminationResult;
            getContext().stop(getSelf());
        }
    }

    // ---------------------------------------------------------------------------
    // Internal state machine
    // ---------------------------------------------------------------------------

    interface State {
        default State start(PekkoRpcActor<?> pekkoRpcActor, ClassLoader flinkClassLoader) {
            throw new RpcInvalidStateException(invalidStateTransitionMessage(StartedState.STARTED));
        }

        default State stop() {
            throw new RpcInvalidStateException(invalidStateTransitionMessage(StoppedState.STOPPED));
        }

        default State terminate(PekkoRpcActor<?> pekkoRpcActor, ClassLoader flinkClassLoader) {
            throw new RpcInvalidStateException(
                    invalidStateTransitionMessage(TerminatingState.TERMINATING));
        }

        default State finishTermination() {
            return TerminatedState.TERMINATED;
        }

        default boolean isRunning() {
            return false;
        }

        default String invalidStateTransitionMessage(State targetState) {
            return String.format(
                    "RpcActor is currently in state %s and cannot go into state %s.",
                    this, targetState);
        }
    }

    @SuppressWarnings("Singleton")
    enum StartedState implements State {
        STARTED;

        @Override
        public State start(PekkoRpcActor<?> pekkoRpcActor, ClassLoader flinkClassLoader) {
            return STARTED;
        }

        @Override
        public State stop() {
            return StoppedState.STOPPED;
        }

        @Override
        public State terminate(PekkoRpcActor<?> pekkoRpcActor, ClassLoader flinkClassLoader) {
            pekkoRpcActor.mainThreadValidator.enterMainThread();

            CompletableFuture<Void> terminationFuture;
            try {
                terminationFuture =
                        runWithContextClassLoader(
                                () -> pekkoRpcActor.rpcEndpoint.internalCallOnStop(),
                                flinkClassLoader);
            } catch (Throwable t) {
                terminationFuture =
                        FutureUtils.completedExceptionally(
                                new RpcException(
                                        String.format(
                                                "Failure while stopping RpcEndpoint %s.",
                                                pekkoRpcActor.rpcEndpoint.getEndpointId()),
                                        t));
            } finally {
                pekkoRpcActor.mainThreadValidator.exitMainThread();
            }

            // IMPORTANT: This only works if we don't use a restarting supervisor strategy.
            // Otherwise
            // we would complete the future and let the actor system restart the actor with a
            // completed
            // future.
            // Complete the termination future so that others know that we've stopped.

            terminationFuture.whenComplete(
                    (ignored, throwable) ->
                            pekkoRpcActor.stop(RpcEndpointTerminationResult.of(throwable)));

            return TerminatingState.TERMINATING;
        }

        @Override
        public boolean isRunning() {
            return true;
        }
    }

    @SuppressWarnings("Singleton")
    enum StoppedState implements State {
        STOPPED;

        @Override
        public State start(PekkoRpcActor<?> pekkoRpcActor, ClassLoader flinkClassLoader) {
            pekkoRpcActor.mainThreadValidator.enterMainThread();

            try {
                runWithContextClassLoader(
                        () -> pekkoRpcActor.rpcEndpoint.internalCallOnStart(), flinkClassLoader);
            } catch (Throwable throwable) {
                pekkoRpcActor.stop(
                        RpcEndpointTerminationResult.failure(
                                new RpcException(
                                        String.format(
                                                "Could not start RpcEndpoint %s.",
                                                pekkoRpcActor.rpcEndpoint.getEndpointId()),
                                        throwable)));
            } finally {
                pekkoRpcActor.mainThreadValidator.exitMainThread();
            }

            return StartedState.STARTED;
        }

        @Override
        public State stop() {
            return STOPPED;
        }

        @Override
        public State terminate(PekkoRpcActor<?> pekkoRpcActor, ClassLoader flinkClassLoader) {
            pekkoRpcActor.stop(RpcEndpointTerminationResult.success());

            return TerminatingState.TERMINATING;
        }
    }

    @SuppressWarnings("Singleton")
    enum TerminatingState implements State {
        TERMINATING;

        @Override
        public State terminate(PekkoRpcActor<?> pekkoRpcActor, ClassLoader flinkClassLoader) {
            return TERMINATING;
        }

        @Override
        public boolean isRunning() {
            return true;
        }
    }

    enum TerminatedState implements State {
        TERMINATED
    }

    private static final class RpcEndpointTerminationResult {

        private static final RpcEndpointTerminationResult SUCCESS =
                new RpcEndpointTerminationResult(null);

        @Nullable private final Throwable failureCause;

        private RpcEndpointTerminationResult(@Nullable Throwable failureCause) {
            this.failureCause = failureCause;
        }

        public boolean isSuccess() {
            return failureCause == null;
        }

        public Throwable getFailureCause() {
            Preconditions.checkState(failureCause != null);
            return failureCause;
        }

        private static RpcEndpointTerminationResult success() {
            return SUCCESS;
        }

        private static RpcEndpointTerminationResult failure(Throwable failureCause) {
            return new RpcEndpointTerminationResult(failureCause);
        }

        private static RpcEndpointTerminationResult of(@Nullable Throwable failureCause) {
            if (failureCause == null) {
                return success();
            } else {
                return failure(failureCause);
            }
        }
    }
}
