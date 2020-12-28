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

package org.apache.flink.runtime.registration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This utility class implements the basis of registering one component at another component, for
 * example registering the TaskExecutor at the ResourceManager. This {@code RetryingRegistration}
 * implements both the initial address resolution and the retries-with-backoff strategy.
 *
 * <p>The registration gives access to a future that is completed upon successful registration. The
 * registration can be canceled, for example when the target where it tries to register at looses
 * leader status.
 *
 * @param <F> The type of the fencing token
 * @param <G> The type of the gateway to connect to.
 * @param <S> The type of the successful registration responses.
 */
public abstract class RetryingRegistration<
        F extends Serializable, G extends RpcGateway, S extends RegistrationResponse.Success> {

    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private final Logger log;

    private final RpcService rpcService;

    private final String targetName;

    private final Class<G> targetType;

    private final String targetAddress;

    private final F fencingToken;

    private final CompletableFuture<Tuple2<G, S>> completionFuture;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    private volatile boolean canceled;

    // ------------------------------------------------------------------------

    public RetryingRegistration(
            Logger log,
            RpcService rpcService,
            String targetName,
            Class<G> targetType,
            String targetAddress,
            F fencingToken,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

        this.log = checkNotNull(log);
        this.rpcService = checkNotNull(rpcService);
        this.targetName = checkNotNull(targetName);
        this.targetType = checkNotNull(targetType);
        this.targetAddress = checkNotNull(targetAddress);
        this.fencingToken = checkNotNull(fencingToken);
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

        this.completionFuture = new CompletableFuture<>();
    }

    // ------------------------------------------------------------------------
    //  completion and cancellation
    // ------------------------------------------------------------------------

    public CompletableFuture<Tuple2<G, S>> getFuture() {
        return completionFuture;
    }

    /** Cancels the registration procedure. */
    public void cancel() {
        canceled = true;
        completionFuture.cancel(false);
    }

    /**
     * Checks if the registration was canceled.
     *
     * @return True if the registration was canceled, false otherwise.
     */
    public boolean isCanceled() {
        return canceled;
    }

    // ------------------------------------------------------------------------
    //  registration
    // ------------------------------------------------------------------------

    protected abstract CompletableFuture<RegistrationResponse> invokeRegistration(
            G gateway, F fencingToken, long timeoutMillis) throws Exception;

    /**
     * This method resolves the target address to a callable gateway and starts the registration
     * after that.
     */
    @SuppressWarnings("unchecked")
    public void startRegistration() {
        if (canceled) {
            // we already got canceled
            return;
        }

        try {
            // trigger resolution of the target address to a callable gateway
            final CompletableFuture<G> rpcGatewayFuture;

            if (FencedRpcGateway.class.isAssignableFrom(targetType)) {
                rpcGatewayFuture =
                        (CompletableFuture<G>)
                                rpcService.connect(
                                        targetAddress,
                                        fencingToken,
                                        targetType.asSubclass(FencedRpcGateway.class));
            } else {
                rpcGatewayFuture = rpcService.connect(targetAddress, targetType);
            }

            // upon success, start the registration attempts
            CompletableFuture<Void> rpcGatewayAcceptFuture =
                    rpcGatewayFuture.thenAcceptAsync(
                            (G rpcGateway) -> {
                                log.info("Resolved {} address, beginning registration", targetName);
                                register(
                                        rpcGateway,
                                        1,
                                        retryingRegistrationConfiguration
                                                .getInitialRegistrationTimeoutMillis());
                            },
                            rpcService.getExecutor());

            // upon failure, retry, unless this is cancelled
            rpcGatewayAcceptFuture.whenCompleteAsync(
                    (Void v, Throwable failure) -> {
                        if (failure != null && !canceled) {
                            final Throwable strippedFailure =
                                    ExceptionUtils.stripCompletionException(failure);
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Could not resolve {} address {}, retrying in {} ms.",
                                        targetName,
                                        targetAddress,
                                        retryingRegistrationConfiguration.getErrorDelayMillis(),
                                        strippedFailure);
                            } else {
                                log.info(
                                        "Could not resolve {} address {}, retrying in {} ms: {}",
                                        targetName,
                                        targetAddress,
                                        retryingRegistrationConfiguration.getErrorDelayMillis(),
                                        strippedFailure.getMessage());
                            }

                            startRegistrationLater(
                                    retryingRegistrationConfiguration.getErrorDelayMillis());
                        }
                    },
                    rpcService.getExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    /**
     * This method performs a registration attempt and triggers either a success notification or a
     * retry, depending on the result.
     */
    @SuppressWarnings("unchecked")
    private void register(final G gateway, final int attempt, final long timeoutMillis) {
        // eager check for canceling to avoid some unnecessary work
        if (canceled) {
            return;
        }

        try {
            log.debug(
                    "Registration at {} attempt {} (timeout={}ms)",
                    targetName,
                    attempt,
                    timeoutMillis);
            CompletableFuture<RegistrationResponse> registrationFuture =
                    invokeRegistration(gateway, fencingToken, timeoutMillis);

            // if the registration was successful, let the TaskExecutor know
            CompletableFuture<Void> registrationAcceptFuture =
                    registrationFuture.thenAcceptAsync(
                            (RegistrationResponse result) -> {
                                if (!isCanceled()) {
                                    if (result instanceof RegistrationResponse.Success) {
                                        // registration successful!
                                        S success = (S) result;
                                        completionFuture.complete(Tuple2.of(gateway, success));
                                    } else {
                                        // registration refused or unknown
                                        if (result instanceof RegistrationResponse.Decline) {
                                            RegistrationResponse.Decline decline =
                                                    (RegistrationResponse.Decline) result;
                                            log.info(
                                                    "Registration at {} was declined: {}",
                                                    targetName,
                                                    decline.getReason());
                                        } else {
                                            log.error(
                                                    "Received unknown response to registration attempt: {}",
                                                    result);
                                        }

                                        log.info(
                                                "Pausing and re-attempting registration in {} ms",
                                                retryingRegistrationConfiguration
                                                        .getRefusedDelayMillis());
                                        registerLater(
                                                gateway,
                                                1,
                                                retryingRegistrationConfiguration
                                                        .getInitialRegistrationTimeoutMillis(),
                                                retryingRegistrationConfiguration
                                                        .getRefusedDelayMillis());
                                    }
                                }
                            },
                            rpcService.getExecutor());

            // upon failure, retry
            registrationAcceptFuture.whenCompleteAsync(
                    (Void v, Throwable failure) -> {
                        if (failure != null && !isCanceled()) {
                            if (ExceptionUtils.stripCompletionException(failure)
                                    instanceof TimeoutException) {
                                // we simply have not received a response in time. maybe the timeout
                                // was
                                // very low (initial fast registration attempts), maybe the target
                                // endpoint is
                                // currently down.
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "Registration at {} ({}) attempt {} timed out after {} ms",
                                            targetName,
                                            targetAddress,
                                            attempt,
                                            timeoutMillis);
                                }

                                long newTimeoutMillis =
                                        Math.min(
                                                2 * timeoutMillis,
                                                retryingRegistrationConfiguration
                                                        .getMaxRegistrationTimeoutMillis());
                                register(gateway, attempt + 1, newTimeoutMillis);
                            } else {
                                // a serious failure occurred. we still should not give up, but keep
                                // trying
                                log.error(
                                        "Registration at {} failed due to an error",
                                        targetName,
                                        failure);
                                log.info(
                                        "Pausing and re-attempting registration in {} ms",
                                        retryingRegistrationConfiguration.getErrorDelayMillis());

                                registerLater(
                                        gateway,
                                        1,
                                        retryingRegistrationConfiguration
                                                .getInitialRegistrationTimeoutMillis(),
                                        retryingRegistrationConfiguration.getErrorDelayMillis());
                            }
                        }
                    },
                    rpcService.getExecutor());
        } catch (Throwable t) {
            completionFuture.completeExceptionally(t);
            cancel();
        }
    }

    private void registerLater(
            final G gateway, final int attempt, final long timeoutMillis, long delay) {
        rpcService.scheduleRunnable(
                new Runnable() {
                    @Override
                    public void run() {
                        register(gateway, attempt, timeoutMillis);
                    }
                },
                delay,
                TimeUnit.MILLISECONDS);
    }

    private void startRegistrationLater(final long delay) {
        rpcService.scheduleRunnable(this::startRegistration, delay, TimeUnit.MILLISECONDS);
    }
}
