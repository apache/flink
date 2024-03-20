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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for leader election service. Composed with different {@link
 * LeaderElectionDriver}, we could perform a leader election for the contender, and then persist the
 * leader information to various storage.
 *
 * <p>{@code DefaultLeaderElectionService} handles a single {@link LeaderContender}.
 */
public class DefaultLeaderElectionService extends DefaultLeaderElection.ParentService
        implements LeaderElectionService, LeaderElectionDriver.Listener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    private static final String LEADER_ACQUISITION_EVENT_LOG_NAME = "Leader Acquisition";
    private static final String LEADER_REVOCATION_EVENT_LOG_NAME = "Leader Revocation";
    private final Object lock = new Object();

    private final LeaderElectionDriverFactory leaderElectionDriverFactory;

    @GuardedBy("lock")
    private final Map<String, LeaderContender> leaderContenderRegistry = new HashMap<>();

    /**
     * Saves the session ID which was issued by the {@link LeaderElectionDriver} if and only if the
     * leadership is acquired by this service. {@code issuedLeaderSessionID} being {@code null}
     * indicates that this service isn't the leader right now (i.e. {@link
     * #onGrantLeadership(UUID)}) wasn't called, yet (independently of what {@code
     * leaderElectionDriver#hasLeadership()} returns).
     */
    @GuardedBy("lock")
    @Nullable
    private UUID issuedLeaderSessionID;

    /**
     * Saves the {@link LeaderInformation} for the registered {@link LeaderContender}s. There's no
     * semantic difference between an entry with an empty {@code LeaderInformation} and no entry
     * being present at all here. Both mean that no confirmed {@code LeaderInformation} is available
     * for the corresponding {@code componentId}.
     */
    @GuardedBy("lock")
    private LeaderInformationRegister confirmedLeaderInformation;

    @GuardedBy("lock")
    private boolean running;

    /**
     * The driver's lifecycle is bound to the {@link #leaderContenderRegistry}: {@code
     * leaderElectionDriver} is {@code null} if no contender is registered: A new driver is created
     * as soon as the first contender is added to the empty {@code leaderContenderRegistry}. Only
     * then, a connection to the {@code DefaultLeaderElectionService} backend is established. The
     * service resets and closes the driver with the removal of the last contender.
     */
    @GuardedBy("lock")
    private LeaderElectionDriver leaderElectionDriver;

    /**
     * This {@link ExecutorService} is used for running the leader event handling logic. Production
     * code should rely on a single-threaded executor to ensure the sequential execution of the
     * events.
     *
     * <p>The executor is guarded by this instance's {@link #running} state.
     */
    private final ExecutorService leadershipOperationExecutor;

    private final FatalErrorHandler fallbackErrorHandler;

    public DefaultLeaderElectionService(LeaderElectionDriverFactory leaderElectionDriverFactory) {
        this(
                leaderElectionDriverFactory,
                t ->
                        LOG.debug(
                                "Ignoring error notification since there's no contender registered."));
    }

    @VisibleForTesting
    public DefaultLeaderElectionService(
            LeaderElectionDriverFactory leaderElectionDriverFactory,
            FatalErrorHandler fallbackErrorHandler) {
        this(
                leaderElectionDriverFactory,
                fallbackErrorHandler,
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "DefaultLeaderElectionService-leadershipOperationExecutor")));
    }

    @VisibleForTesting
    DefaultLeaderElectionService(
            LeaderElectionDriverFactory leaderElectionDriverFactory,
            FatalErrorHandler fallbackErrorHandler,
            ExecutorService leadershipOperationExecutor) {
        this.leaderElectionDriverFactory = checkNotNull(leaderElectionDriverFactory);

        this.fallbackErrorHandler = checkNotNull(fallbackErrorHandler);

        this.issuedLeaderSessionID = null;

        this.leaderElectionDriver = null;

        this.confirmedLeaderInformation = LeaderInformationRegister.empty();

        this.leadershipOperationExecutor = Preconditions.checkNotNull(leadershipOperationExecutor);

        this.running = true;
    }

    @Override
    public LeaderElection createLeaderElection(String componentId) {
        synchronized (lock) {
            Preconditions.checkState(
                    !leadershipOperationExecutor.isShutdown(),
                    "The service was already closed and cannot be reused.");
            Preconditions.checkState(
                    !leaderContenderRegistry.containsKey(componentId),
                    "There shouldn't be any contender registered under the passed component '%s'.",
                    componentId);
            return new DefaultLeaderElection(this, componentId);
        }
    }

    @GuardedBy("lock")
    private void createLeaderElectionDriver() throws Exception {
        Preconditions.checkState(
                leaderContenderRegistry.isEmpty(),
                "No LeaderContender should have been registered, yet.");
        Preconditions.checkState(
                leaderElectionDriver == null,
                "This DefaultLeaderElectionService cannot be reused. Calling startLeaderElectionBackend can only be called once to establish the connection to the HA backend.");

        leaderElectionDriver = leaderElectionDriverFactory.create(this);

        LOG.info(
                "A connection to the HA backend was established through LeaderElectionDriver {}.",
                leaderElectionDriver);
    }

    @Override
    protected void register(String componentId, LeaderContender contender) throws Exception {
        checkNotNull(componentId, "componentId must not be null.");
        checkNotNull(contender, "Contender must not be null.");

        synchronized (lock) {
            Preconditions.checkState(
                    running,
                    "The DefaultLeaderElectionService should have established a connection to the backend before it's started.");

            if (leaderElectionDriver == null) {
                createLeaderElectionDriver();
            }

            Preconditions.checkState(
                    leaderContenderRegistry.put(componentId, contender) == null,
                    "There shouldn't be any contender registered under the passed component '%s'.",
                    componentId);

            LOG.info(
                    "LeaderContender has been registered under component '{}' for {}.",
                    componentId,
                    leaderElectionDriver);

            if (issuedLeaderSessionID != null) {
                // notifying the LeaderContender shouldn't happen in the contender's main thread
                FutureUtils.handleUncaughtException(
                        runInLeaderEventThreadIfStarted(
                                componentId,
                                LEADER_ACQUISITION_EVENT_LOG_NAME,
                                () ->
                                        notifyLeaderContenderOfLeadership(
                                                componentId, issuedLeaderSessionID)),
                        this::forwardErrorToLeaderContender);
            }
        }
    }

    @Override
    protected final void remove(String componentId) throws Exception {
        AutoCloseable driverToClose = null;
        synchronized (lock) {
            if (!leaderContenderRegistry.containsKey(componentId)) {
                LOG.debug(
                        "There is no contender registered under component '{}' anymore. No action necessary.",
                        componentId);
                return;
            }
            Preconditions.checkState(
                    leaderElectionDriver != null,
                    "The LeaderElectionDriver should be instantiated.");

            LOG.info(
                    "Deregistering contender with component '{}' from the DefaultLeaderElectionService.",
                    componentId);

            final LeaderContender leaderContender = leaderContenderRegistry.remove(componentId);
            Preconditions.checkNotNull(
                    leaderContender,
                    "There should be a LeaderContender registered under the given component '%s'.",
                    componentId);
            if (issuedLeaderSessionID != null) {
                notifyLeaderContenderOfLeadershipLoss(componentId, leaderContender);
                LOG.debug(
                        "The contender associated with component '{}' is deregistered while the service has the leadership acquired. The revoke event is forwarded to the LeaderContender.",
                        componentId);

                if (leaderElectionDriver.hasLeadership()) {
                    leaderElectionDriver.deleteLeaderInformation(componentId);
                    LOG.debug(
                            "Leader information is cleaned up while deregistering the contender for component '{}' from the service.",
                            componentId);
                }
            } else {
                Preconditions.checkState(
                        confirmedLeaderInformation.hasNoLeaderInformation(),
                        "The confirmed leader information should have been cleared during leadership revocation.");

                LOG.debug(
                        "Contender associated with component '{}' is deregistered while the service doesn't have the leadership acquired. No cleanup necessary.",
                        componentId);
            }

            if (leaderContenderRegistry.isEmpty()) {
                driverToClose = deregisterDriver();
            }
        }

        if (driverToClose != null) {
            driverToClose.close();
        }
    }

    /**
     * Returns the driver as an {@link AutoCloseable} for the sake of closing the driver outside of
     * the lock.
     */
    @GuardedBy("lock")
    private AutoCloseable deregisterDriver() {
        Preconditions.checkState(
                leaderContenderRegistry.isEmpty(),
                "No contender should be registered when deregistering the driver.");
        Preconditions.checkState(
                leaderElectionDriver != null,
                "There should be a driver instantiated that's ready to be closed.");

        issuedLeaderSessionID = null;
        final AutoCloseable driverToClose = leaderElectionDriver;
        leaderElectionDriver = null;

        return driverToClose;
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            Preconditions.checkState(
                    leaderContenderRegistry.isEmpty(),
                    "The DefaultLeaderElectionService should have been stopped before closing the instance.");
            Preconditions.checkState(
                    leaderElectionDriver == null, "The driver should have been closed.");

            if (running) {
                running = false;
            } else {
                LOG.debug("The HA backend connection isn't established. No actions taken.");
                return;
            }
        }

        // interrupt any outstanding events
        final List<Runnable> outstandingEventHandlingCalls =
                leadershipOperationExecutor.shutdownNow();
        if (!outstandingEventHandlingCalls.isEmpty()) {
            LOG.debug(
                    "The DefaultLeaderElectionService was closed with {} event(s) still not being processed. No further action necessary.",
                    outstandingEventHandlingCalls.size());
        }
    }

    @Override
    protected void confirmLeadership(
            String componentId, UUID leaderSessionID, String leaderAddress) {
        Preconditions.checkArgument(leaderContenderRegistry.containsKey(componentId));
        LOG.debug(
                "The leader session for component '{}' is confirmed with session ID {} and address {}.",
                componentId,
                leaderSessionID,
                leaderAddress);

        checkNotNull(leaderSessionID);

        runAsyncIfLeader(
                        componentId,
                        leaderSessionID,
                        () ->
                                confirmLeadershipInternal(
                                        componentId, leaderSessionID, leaderAddress),
                        "Confirm leadership")
                .exceptionally(
                        t -> {
                            if (ExceptionUtils.findThrowable(t, LeadershipLostException.class)
                                    .isPresent()) {
                                LOG.debug(
                                        "Confirmation of the leadership failed because the leadership was lost in the mean time.",
                                        t);
                            } else {
                                this.forwardErrorToLeaderContender(Thread.currentThread(), t);
                            }
                            return null;
                        });
    }

    @GuardedBy("lock")
    private void confirmLeadershipInternal(
            String componentId, UUID leaderSessionID, String leaderAddress) {
        Preconditions.checkState(
                leaderElectionDriver != null,
                "The leadership check should only return true if a driver is instantiated.");
        Preconditions.checkState(
                !confirmedLeaderInformation.hasLeaderInformation(componentId),
                "No confirmation should have happened, yet.");

        final LeaderInformation newConfirmedLeaderInformation =
                LeaderInformation.known(leaderSessionID, leaderAddress);
        confirmedLeaderInformation =
                LeaderInformationRegister.merge(
                        confirmedLeaderInformation, componentId, newConfirmedLeaderInformation);
        leaderElectionDriver.publishLeaderInformation(componentId, newConfirmedLeaderInformation);
    }

    @Override
    protected CompletableFuture<Void> runAsyncIfLeader(
            String componentId,
            UUID leaderSessionID,
            ThrowingRunnable<? extends Throwable> synchronizedCallback,
            String eventLabelToLog) {
        return runInLeaderEventThreadIfStarted(
                componentId,
                eventLabelToLog,
                () -> runIfLeader(componentId, leaderSessionID, synchronizedCallback));
    }

    @GuardedBy("lock")
    private void runIfLeader(
            String componentId,
            UUID leaderSessionID,
            ThrowingRunnable<? extends Throwable> synchronizedCallback)
            throws Throwable {
        if (hasLeadership(componentId, leaderSessionID)) {
            synchronizedCallback.run();
        } else {
            final String errorMessage;
            if (!leaderSessionID.equals(this.issuedLeaderSessionID)) {
                errorMessage =
                        String.format(
                                "Received an old confirmation call of leader session ID %s for component '%s' (current issued session ID is %s).",
                                leaderSessionID, componentId, issuedLeaderSessionID);
            } else {
                errorMessage =
                        String.format(
                                "The leader session ID %s for component '%s' was confirmed even though the corresponding "
                                        + "service was not elected as the leader or has been stopped already.",
                                componentId, leaderSessionID);
            }

            throw new LeadershipLostException(errorMessage);
        }
    }

    @GuardedBy("lock")
    private boolean hasLeadership(String componentId, UUID leaderSessionId) {
        if (leaderElectionDriver != null) {
            if (leaderContenderRegistry.containsKey(componentId)) {
                return leaderElectionDriver.hasLeadership()
                        && leaderSessionId.equals(issuedLeaderSessionID);
            } else {
                LOG.debug(
                        "hasLeadership is called for component '{}' while there is no contender registered under that ID in the service, returning false.",
                        componentId);
                return false;
            }
        } else {
            LOG.debug("hasLeadership is called after the service is closed, returning false.");
            return false;
        }
    }

    @GuardedBy("lock")
    private void onGrantLeadershipInternal(UUID newLeaderSessionId) {
        Preconditions.checkNotNull(newLeaderSessionId);

        Preconditions.checkState(
                issuedLeaderSessionID == null,
                "The leadership should have been granted while not having the leadership acquired.");

        issuedLeaderSessionID = newLeaderSessionId;

        leaderContenderRegistry
                .keySet()
                .forEach(
                        componentId ->
                                notifyLeaderContenderOfLeadership(
                                        componentId, issuedLeaderSessionID));
    }

    @GuardedBy("lock")
    private void notifyLeaderContenderOfLeadership(String componentId, UUID sessionID) {
        if (!leaderContenderRegistry.containsKey(componentId)) {
            LOG.debug(
                    "The grant leadership notification for session ID {} is not forwarded because the DefaultLeaderElectionService ({}) has no contender registered.",
                    sessionID,
                    leaderElectionDriver);
            return;
        } else if (!sessionID.equals(issuedLeaderSessionID)) {
            LOG.debug(
                    "An out-dated leadership-acquired event with session ID {} was triggered. The current leader session ID is {}. The event will be ignored.",
                    sessionID,
                    issuedLeaderSessionID);
            return;
        }

        Preconditions.checkState(
                !confirmedLeaderInformation.hasLeaderInformation(componentId),
                "The leadership should have been granted while not having the leadership acquired.");

        LOG.debug(
                "Granting leadership to the contender registered under component '{}' with session ID {}.",
                componentId,
                issuedLeaderSessionID);

        leaderContenderRegistry.get(componentId).grantLeadership(issuedLeaderSessionID);
    }

    @GuardedBy("lock")
    private void onRevokeLeadershipInternal() {
        Preconditions.checkState(
                issuedLeaderSessionID != null,
                "The leadership should have been revoked while having the leadership acquired.");

        if (!leaderContenderRegistry.isEmpty()) {
            leaderContenderRegistry.forEach(this::notifyLeaderContenderOfLeadershipLoss);
        } else {
            LOG.debug(
                    "The revoke leadership notification for session {} is not forwarded because the DefaultLeaderElectionService({}) has no contender registered.",
                    issuedLeaderSessionID,
                    leaderElectionDriver);
        }

        issuedLeaderSessionID = null;
    }

    @GuardedBy("lock")
    private void notifyLeaderContenderOfLeadershipLoss(
            String componentId, LeaderContender leaderContender) {
        Preconditions.checkState(
                leaderContender != null,
                "The LeaderContender should be always set when calling this method.");

        if (!confirmedLeaderInformation.hasLeaderInformation(componentId)) {
            LOG.debug(
                    "Revoking leadership for component '{}' while a previous leadership grant wasn't confirmed, yet.",
                    componentId);
        } else {
            LOG.debug(
                    "Revoking leadership to component '{}' for previously confirmed leader information {}.",
                    componentId,
                    LeaderElectionUtils.convertToString(
                            confirmedLeaderInformation.forComponentIdOrEmpty(componentId)));
        }

        confirmedLeaderInformation =
                LeaderInformationRegister.clear(confirmedLeaderInformation, componentId);
        leaderContender.revokeLeadership();
    }

    @GuardedBy("lock")
    private void notifyLeaderInformationChangeInternal(
            String componentId,
            LeaderInformation externallyChangedLeaderInformation,
            LeaderInformation confirmedLeaderInformation) {
        if (leaderElectionDriver == null) {
            LOG.debug(
                    "The LeaderElectionDriver was disconnected. Any incoming events will be ignored.");
            return;
        }

        if (confirmedLeaderInformation.equals(externallyChangedLeaderInformation)) {
            LOG.trace(
                    "LeaderInformation change event received but changed LeaderInformation actually matches the locally confirmed one: {}",
                    confirmedLeaderInformation);
            return;
        }

        if (confirmedLeaderInformation.isEmpty()) {
            LOG.trace(
                    "Leader information changed while there's no confirmation available by the contender for component '{}', yet. Changed leader information {} will be reset.",
                    componentId,
                    LeaderElectionUtils.convertToString(externallyChangedLeaderInformation));
        } else if (externallyChangedLeaderInformation.isEmpty()) {
            LOG.debug(
                    "Re-writing leader information ({}) for component '{}' to overwrite the empty leader information in the external storage.",
                    LeaderElectionUtils.convertToString(confirmedLeaderInformation),
                    componentId);
        } else {
            // the changed LeaderInformation does not match the confirmed LeaderInformation
            LOG.debug(
                    "Correcting leader information for component '{}' (local: {}, external storage: {}).",
                    componentId,
                    LeaderElectionUtils.convertToString(confirmedLeaderInformation),
                    LeaderElectionUtils.convertToString(externallyChangedLeaderInformation));
        }

        leaderElectionDriver.publishLeaderInformation(componentId, confirmedLeaderInformation);
    }

    private CompletableFuture<Void> runInLeaderEventThreadIfStarted(
            String leaderElectionEventLabel,
            ThrowingRunnable<? extends Throwable> synchronizedCallback) {
        return runInLeaderEventThreadIfStartedWithProperEventLogging(
                "for all components", null, leaderElectionEventLabel, synchronizedCallback);
    }

    private CompletableFuture<Void> runInLeaderEventThreadIfStarted(
            String componentId,
            String leaderElectionEventLabel,
            ThrowingRunnable<? extends Throwable> synchronizedCallback) {
        return runInLeaderEventThreadIfStartedWithProperEventLogging(
                "from " + componentId, componentId, leaderElectionEventLabel, synchronizedCallback);
    }

    private CompletableFuture<Void> runInLeaderEventThreadIfStartedWithProperEventLogging(
            String componentLabel,
            @Nullable String componentId,
            String leaderElectionEventLabel,
            ThrowingRunnable<? extends Throwable> synchronizedCallback) {
        final CompletableFuture<Void> forwardedAsyncOperationFuture = new CompletableFuture<>();
        final boolean asyncCallIsTriggered =
                synchronizedRunCallback(
                        "Handling",
                        leaderElectionEventLabel,
                        componentId,
                        componentLabel,
                        () -> {
                            final CompletableFuture<Void> asyncOperationFuture =
                                    CompletableFuture.runAsync(
                                            () ->
                                                    synchronizedRunCallback(
                                                            "Processing",
                                                            leaderElectionEventLabel,
                                                            componentId,
                                                            componentLabel,
                                                            () -> {
                                                                try {
                                                                    synchronizedCallback.run();
                                                                } catch (Throwable t) {
                                                                    throw new CompletionException(
                                                                            t);
                                                                }
                                                            }),
                                            leadershipOperationExecutor);
                            FutureUtils.forward(
                                    asyncOperationFuture, forwardedAsyncOperationFuture);
                        });

        if (!asyncCallIsTriggered) {
            forwardedAsyncOperationFuture.complete(null);
        }

        return forwardedAsyncOperationFuture;
    }

    private boolean synchronizedRunCallback(
            String logPrefix,
            String leaderElectionEventLabel,
            @Nullable String componentId,
            String componentIdLabel,
            Runnable callback) {
        synchronized (lock) {
            final String logMessageFormat =
                    String.format(
                            "%s '%s' event %s {}. The event will be ignored.",
                            logPrefix, leaderElectionEventLabel, componentIdLabel);
            if (!running) {
                LOG.debug(logMessageFormat, "while the DefaultLeaderElectionService is closed");
            } else if (leaderElectionDriver == null) {
                Preconditions.checkState(
                        leaderContenderRegistry.isEmpty(),
                        "All contenders should be deregistered when the driver is removed.");
                LOG.debug(
                        logMessageFormat,
                        "while the driver is not started and no contenders are registered");
            } else if (componentId != null && !leaderContenderRegistry.containsKey(componentId)) {
                LOG.debug(
                        logMessageFormat, "while the triggering component is already deregistered");
            } else {
                LOG.debug(
                        "{} '{}' event {} is triggered.",
                        logPrefix,
                        leaderElectionEventLabel,
                        componentIdLabel);
                callback.run();
                return true;
            }

            return false;
        }
    }

    private void forwardErrorToLeaderContender(Thread ignored, Throwable t) {
        synchronized (lock) {
            if (leaderContenderRegistry.isEmpty()) {
                fallbackErrorHandler.onFatalError(t);
                return;
            }

            leaderContenderRegistry
                    .values()
                    .forEach(
                            leaderContender -> {
                                if (t instanceof LeaderElectionException) {
                                    leaderContender.handleError((LeaderElectionException) t);
                                } else {
                                    leaderContender.handleError(new LeaderElectionException(t));
                                }
                            });
        }
    }

    @Override
    public void onGrantLeadership(UUID leaderSessionID) {
        FutureUtils.handleUncaughtException(
                runInLeaderEventThreadIfStarted(
                        LEADER_ACQUISITION_EVENT_LOG_NAME,
                        () -> onGrantLeadershipInternal(leaderSessionID)),
                this::forwardErrorToLeaderContender);
    }

    @Override
    public void onRevokeLeadership() {
        FutureUtils.handleUncaughtException(
                runInLeaderEventThreadIfStarted(
                        LEADER_REVOCATION_EVENT_LOG_NAME, this::onRevokeLeadershipInternal),
                this::forwardErrorToLeaderContender);
    }

    @Override
    public void onLeaderInformationChange(String componentId, LeaderInformation leaderInformation) {
        synchronized (lock) {
            notifyLeaderInformationChangeInternal(
                    componentId,
                    leaderInformation,
                    confirmedLeaderInformation.forComponentIdOrEmpty(componentId));
        }
    }

    @Override
    public void onLeaderInformationChange(LeaderInformationRegister changedLeaderInformation) {
        synchronized (lock) {
            leaderContenderRegistry.forEach(
                    (componentId, leaderContender) -> {
                        final LeaderInformation externallyChangedLeaderInformationForContender =
                                changedLeaderInformation
                                        .forComponentId(componentId)
                                        .orElse(LeaderInformation.empty());
                        final LeaderInformation confirmedLeaderInformationForContender =
                                confirmedLeaderInformation.forComponentIdOrEmpty(componentId);

                        notifyLeaderInformationChangeInternal(
                                componentId,
                                externallyChangedLeaderInformationForContender,
                                confirmedLeaderInformationForContender);
                    });
        }
    }

    @Override
    public void onError(Throwable t) {
        forwardErrorToLeaderContender(Thread.currentThread(), t);
    }
}
