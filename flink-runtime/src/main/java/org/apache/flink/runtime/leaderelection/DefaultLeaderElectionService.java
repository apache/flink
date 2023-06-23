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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for leader election service. Composed with different {@link
 * MultipleComponentLeaderElectionDriver}, we could perform a leader election for the contender, and
 * then persist the leader information to various storage.
 *
 * <p>{@code DefaultLeaderElectionService} handles a single {@link LeaderContender}.
 */
public class DefaultLeaderElectionService extends AbstractLeaderElectionService
        implements MultipleComponentLeaderElectionDriver.Listener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    private static final String LEADER_ACQUISITION_EVENT_LOG_NAME = "Leader Acquisition";
    private static final String LEADER_REVOCATION_EVENT_LOG_NAME = "Leader Revocation";
    private static final String SINGLE_LEADER_INFORMATION_CHANGE_EVENT_LOG_NAME =
            "Single LeaderInformation Change";
    private static final String ALL_LEADER_INFORMATION_CHANGE_EVENT_LOG_NAME =
            "All LeaderInformation Change";

    private final Object lock = new Object();

    private final MultipleComponentLeaderElectionDriverFactory leaderElectionDriverFactory;

    @GuardedBy("lock")
    private final Map<String, LeaderContender> leaderContenderRegistry = new HashMap<>();

    /**
     * Saves the session ID which was issued by the {@link MultipleComponentLeaderElectionDriver} if
     * and only if the leadership is acquired by this service. {@code issuedLeaderSessionID} being
     * {@code null} indicates that this service isn't the leader right now (i.e. {@link
     * #isLeader(UUID)}) wasn't called, yet (independently of what {@code
     * leaderElectionDriver#hasLeadership()} returns).
     */
    @GuardedBy("lock")
    @Nullable
    private UUID issuedLeaderSessionID;

    /**
     * Saves the {@link LeaderInformation} for the registered {@link LeaderContender}s. There's no
     * semantical difference between an entry with an empty {@code LeaderInformation} and no entry
     * being present at all here. Both mean that no confirmed {@code LeaderInformation} is available
     * for the corresponding {@code contenderID}.
     */
    @GuardedBy("lock")
    private LeaderInformationRegister confirmedLeaderInformation;

    @GuardedBy("lock")
    private boolean running;

    /**
     * {@code leaderElectionDriver} being {@code null} indicates that the connection to the
     * LeaderElection backend isn't established, yet. See {@link #startLeaderElectionBackend()} and
     * {@link #close()} for lifecycle management. The lifecycle of the driver should have been
     * established before registering a {@link LeaderContender} and stopped after the contender has
     * been removed.
     *
     * <p>{@code @Nullable} isn't used here to avoid having multiple warnings spread over this class
     * in a supporting IDE.
     *
     * <p>The driver is guarded by this instance's {@link #running} state.
     */
    private MultipleComponentLeaderElectionDriver leaderElectionDriver;

    /**
     * This {@link ExecutorService} is used for running the leader event handling logic. Production
     * code should rely on a single-threaded executor to ensure the sequential execution of the
     * events.
     *
     * <p>The executor is guarded by this instance's {@link #running} state.
     */
    private final ExecutorService leadershipOperationExecutor;

    private final FatalErrorHandler fallbackErrorHandler;

    public DefaultLeaderElectionService(
            MultipleComponentLeaderElectionDriverFactory leaderElectionDriverFactory) {
        this(
                leaderElectionDriverFactory,
                t ->
                        LOG.debug(
                                "Ignoring error notification since there's no contender registered."));
    }

    @VisibleForTesting
    public DefaultLeaderElectionService(
            MultipleComponentLeaderElectionDriverFactory leaderElectionDriverFactory,
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
            MultipleComponentLeaderElectionDriverFactory leaderElectionDriverFactory,
            FatalErrorHandler fallbackErrorHandler,
            ExecutorService leadershipOperationExecutor) {
        this.leaderElectionDriverFactory = checkNotNull(leaderElectionDriverFactory);

        this.fallbackErrorHandler = checkNotNull(fallbackErrorHandler);

        this.issuedLeaderSessionID = null;

        this.leaderElectionDriver = null;

        this.confirmedLeaderInformation = LeaderInformationRegister.empty();

        this.leadershipOperationExecutor = Preconditions.checkNotNull(leadershipOperationExecutor);

        this.running = false;
    }

    @Override
    public LeaderElection createLeaderElection(String contenderID) {
        synchronized (lock) {
            Preconditions.checkState(
                    !leaderContenderRegistry.containsKey(contenderID),
                    "There is no contender already registered under the passed contender ID '%s'.",
                    contenderID);
            return new DefaultLeaderElection(this, contenderID);
        }
    }

    /**
     * Starts the leader election process. This method has to be called before registering a {@link
     * LeaderContender}. This method could be moved into the {@code DefaultLeaderElectionService}'s
     * constructor with FLINK-31837.
     */
    public void startLeaderElectionBackend() throws Exception {
        synchronized (lock) {
            Preconditions.checkState(
                    leaderContenderRegistry.isEmpty(),
                    "No LeaderContender should have been registered, yet.");
            Preconditions.checkState(
                    leaderElectionDriver == null,
                    "This DefaultLeaderElectionService cannot be reused. Calling startLeaderElectionBackend can only be called once to establish the connection to the HA backend.");

            running = true;

            leaderElectionDriver =
                    leaderElectionDriverFactory.create(this, new LeaderElectionFatalErrorHandler());

            LOG.info(
                    "A connection to the HA backend was established through LeaderElectionDriver {}.",
                    leaderElectionDriver);
        }
    }

    @Override
    protected void register(String contenderID, LeaderContender contender) throws Exception {
        checkNotNull(contenderID, "ContenderID must not be null.");
        checkNotNull(contender, "Contender must not be null.");

        synchronized (lock) {
            Preconditions.checkState(
                    running,
                    "The DefaultLeaderElectionService should have established a connection to the backend before it's started.");

            Preconditions.checkState(
                    leaderContenderRegistry.put(contenderID, contender) == null,
                    "There is no contender already registered under the passed contender ID '%s'.",
                    contenderID);

            LOG.info(
                    "LeaderContender {} has been registered for {}.",
                    contenderID,
                    leaderElectionDriver);

            if (issuedLeaderSessionID != null) {
                // notifying the LeaderContender shouldn't happen in the contender's main thread
                runInLeaderEventThread(
                        LEADER_ACQUISITION_EVENT_LOG_NAME,
                        () ->
                                notifyLeaderContenderOfLeadership(
                                        contenderID, issuedLeaderSessionID));
            }
        }
    }

    @Override
    protected final void remove(String contenderID) {
        synchronized (lock) {
            if (!leaderContenderRegistry.containsKey(contenderID)) {
                LOG.debug(
                        "There is no contender registered under contenderID '{}' anymore. No action necessary.",
                        contenderID);
                return;
            }

            LOG.info(
                    "Deregistering contender with ID '{}' from the DefaultLeaderElectionService.",
                    contenderID);

            final LeaderContender leaderContender = leaderContenderRegistry.remove(contenderID);
            Preconditions.checkNotNull(
                    leaderContender,
                    "There should be a LeaderContender registered under the given contenderID '%s'.",
                    contenderID);
            if (issuedLeaderSessionID != null) {
                notifyLeaderContenderOfLeadershipLoss(contenderID, leaderContender);
                LOG.debug(
                        "The contender registered under contenderID '{}' is deregistered while the service has the leadership acquired. The revoke event is forwarded to the LeaderContender.",
                        contenderID);

                if (leaderElectionDriver.hasLeadership()) {
                    leaderElectionDriver.deleteLeaderInformation(contenderID);
                    LOG.debug(
                            "Leader information is cleaned up while deregistering the contender '{}' from the service.",
                            contenderID);
                }
            } else {
                Preconditions.checkState(
                        confirmedLeaderInformation.hasNoLeaderInformation(),
                        "The confirmed leader information should have been cleared during leadership revocation.");

                LOG.debug(
                        "Contender registered under contenderID '{}' is deregistered while the service doesn't have the leadership acquired. No cleanup necessary.",
                        contenderID);
            }
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            Preconditions.checkState(
                    leaderElectionDriver != null, "The HA backend wasn't initialized.");

            Preconditions.checkState(
                    leaderContenderRegistry.isEmpty(),
                    "The DefaultLeaderElectionService should have been stopped before closing the instance.");

            issuedLeaderSessionID = null;

            if (running) {
                running = false;
            } else {
                LOG.debug("The HA backend connection isn't established. No actions taken.");
            }
        }

        leaderElectionDriver.close();

        // interrupt any outstanding events
        final List<Runnable> outstandingEventHandlingCalls =
                Preconditions.checkNotNull(leadershipOperationExecutor).shutdownNow();
        if (!outstandingEventHandlingCalls.isEmpty()) {
            LOG.debug(
                    "The DefaultLeaderElectionService was closed with {} event(s) still not being processed. No further action necessary.",
                    outstandingEventHandlingCalls.size());
        }
    }

    @Override
    protected void confirmLeadership(
            String contenderID, UUID leaderSessionID, String leaderAddress) {
        Preconditions.checkArgument(leaderContenderRegistry.containsKey(contenderID));
        LOG.debug(
                "The leader session for contender '{}' is confirmed with session ID {} and address {}.",
                contenderID,
                leaderSessionID,
                leaderAddress);

        checkNotNull(leaderSessionID);

        synchronized (lock) {
            if (hasLeadership(contenderID, leaderSessionID)) {
                Preconditions.checkState(
                        !confirmedLeaderInformation.hasLeaderInformation(contenderID),
                        "No confirmation should have happened, yet.");

                final LeaderInformation newConfirmedLeaderInformation =
                        LeaderInformation.known(leaderSessionID, leaderAddress);
                confirmedLeaderInformation =
                        LeaderInformationRegister.merge(
                                confirmedLeaderInformation,
                                contenderID,
                                newConfirmedLeaderInformation);
                leaderElectionDriver.publishLeaderInformation(
                        contenderID, newConfirmedLeaderInformation);
            } else {
                if (!leaderSessionID.equals(this.issuedLeaderSessionID)) {
                    LOG.debug(
                            "Received an old confirmation call of leader session ID {} for contender '{}' (current issued session ID is {}).",
                            leaderSessionID,
                            contenderID,
                            issuedLeaderSessionID);
                } else {
                    LOG.warn(
                            "The leader session ID {} for contender '{}' was confirmed even though the "
                                    + "corresponding service was not elected as the leader or has been stopped already.",
                            contenderID,
                            leaderSessionID);
                }
            }
        }
    }

    @Override
    protected boolean hasLeadership(String contenderID, UUID leaderSessionId) {
        synchronized (lock) {
            if (leaderElectionDriver != null) {
                if (leaderContenderRegistry.containsKey(contenderID)) {
                    return leaderElectionDriver.hasLeadership()
                            && leaderSessionId.equals(issuedLeaderSessionID);
                } else {
                    LOG.debug(
                            "hasLeadership is called for contender ID '{}' while there is no contender registered under that ID in the service, returning false.",
                            contenderID);
                    return false;
                }
            } else {
                LOG.debug("hasLeadership is called after the service is closed, returning false.");
                return false;
            }
        }
    }

    /**
     * Returns the current leader session ID for the given {@code contenderID} or {@code null}, if
     * the session wasn't confirmed.
     */
    @VisibleForTesting
    @Nullable
    public UUID getLeaderSessionID(String contenderID) {
        synchronized (lock) {
            return leaderContenderRegistry.containsKey(contenderID)
                    ? confirmedLeaderInformation
                            .forContenderIdOrEmpty(contenderID)
                            .getLeaderSessionID()
                    : null;
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
                        contenderID ->
                                notifyLeaderContenderOfLeadership(
                                        contenderID, issuedLeaderSessionID));
    }

    @GuardedBy("lock")
    private void notifyLeaderContenderOfLeadership(String contenderID, UUID sessionID) {
        if (!leaderContenderRegistry.containsKey(contenderID)) {
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
                !confirmedLeaderInformation.hasLeaderInformation(contenderID),
                "The leadership should have been granted while not having the leadership acquired.");

        LOG.debug(
                "Granting leadership to contender {} with session ID {}.",
                contenderID,
                issuedLeaderSessionID);

        leaderContenderRegistry.get(contenderID).grantLeadership(issuedLeaderSessionID);
    }

    @GuardedBy("lock")
    private void onRevokeLeadershipInternal() {
        // TODO: FLINK-31814 covers adding this Precondition
        // Preconditions.checkState(issuedLeaderSessionID != null,"The leadership should have
        // been revoked while having the leadership acquired.");

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
            String contenderID, LeaderContender leaderContender) {
        Preconditions.checkState(
                leaderContender != null,
                "The LeaderContender should be always set when calling this method.");

        if (!confirmedLeaderInformation.hasLeaderInformation(contenderID)) {
            LOG.debug(
                    "Revoking leadership to contender {} while a previous leadership grant wasn't confirmed, yet.",
                    contenderID);
        } else {
            LOG.debug(
                    "Revoking leadership to contender {} for {}.",
                    contenderID,
                    LeaderElectionUtils.convertToString(
                            confirmedLeaderInformation.forContenderIdOrEmpty(contenderID)));
        }

        confirmedLeaderInformation =
                LeaderInformationRegister.clear(confirmedLeaderInformation, contenderID);
        leaderContender.revokeLeadership();
    }

    @GuardedBy("lock")
    private void notifyLeaderInformationChangeInternal(
            String contenderID,
            LeaderInformation externallyChangedLeaderInformation,
            LeaderInformation confirmedLeaderInformation) {
        if (confirmedLeaderInformation.equals(externallyChangedLeaderInformation)) {
            LOG.trace(
                    "LeaderInformation change event received but changed LeaderInformation actually matches the locally confirmed one: {}",
                    confirmedLeaderInformation);
            return;
        }

        if (confirmedLeaderInformation.isEmpty()) {
            LOG.trace(
                    "Leader information changed while there's no confirmation available by the contender for contender ID '{}', yet. Changed leader information {} will be reset.",
                    contenderID,
                    LeaderElectionUtils.convertToString(externallyChangedLeaderInformation));
        } else if (externallyChangedLeaderInformation.isEmpty()) {
            LOG.debug(
                    "Re-writing leader information ({}) for contender '{}' to overwrite the empty leader information in the external storage.",
                    LeaderElectionUtils.convertToString(confirmedLeaderInformation),
                    contenderID);
        } else {
            // the changed LeaderInformation does not match the confirmed LeaderInformation
            LOG.debug(
                    "Correcting leader information for contender '{}' (local: {}, external storage: {}).",
                    contenderID,
                    LeaderElectionUtils.convertToString(confirmedLeaderInformation),
                    LeaderElectionUtils.convertToString(externallyChangedLeaderInformation));
        }

        leaderElectionDriver.publishLeaderInformation(contenderID, confirmedLeaderInformation);
    }

    private void runInLeaderEventThread(String leaderElectionEventName, Runnable callback) {
        synchronized (lock) {
            if (running) {
                LOG.debug("'{}' event processing triggered.", leaderElectionEventName);
                FutureUtils.handleUncaughtException(
                        CompletableFuture.runAsync(
                                () -> {
                                    synchronized (lock) {
                                        if (running) {
                                            LOG.debug(
                                                    "Processing '{}' event.",
                                                    leaderElectionEventName);
                                            callback.run();
                                        } else {
                                            LOG.debug(
                                                    "Processing '{}' event omitted due to the service not being in running state, anymore.",
                                                    leaderElectionEventName);
                                        }
                                    }
                                },
                                leadershipOperationExecutor),
                        (thread, error) -> forwardErrorToLeaderContender(error));
            } else {
                LOG.debug(
                        "'{}' event processing was triggered while the DefaultLeaderElectionService is closed. The event will be ignored.",
                        leaderElectionEventName);
            }
        }
    }

    private void forwardErrorToLeaderContender(Throwable t) {
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
    public void isLeader(UUID leaderSessionID) {
        runInLeaderEventThread(
                LEADER_ACQUISITION_EVENT_LOG_NAME,
                () -> onGrantLeadershipInternal(leaderSessionID));
    }

    @Override
    public void notLeader() {
        runInLeaderEventThread(LEADER_REVOCATION_EVENT_LOG_NAME, this::onRevokeLeadershipInternal);
    }

    @Override
    public void notifyLeaderInformationChange(
            String contenderID, LeaderInformation leaderInformation) {
        synchronized (lock) {
            notifyLeaderInformationChangeInternal(
                    contenderID,
                    leaderInformation,
                    confirmedLeaderInformation.forContenderIdOrEmpty(contenderID));
        }
    }

    @Override
    public void notifyAllKnownLeaderInformation(
            LeaderInformationRegister changedLeaderInformation) {
        synchronized (lock) {
            leaderContenderRegistry.forEach(
                    (contenderID, leaderContender) -> {
                        final LeaderInformation externallyChangedLeaderInformationForContender =
                                changedLeaderInformation
                                        .forContenderID(contenderID)
                                        .orElse(LeaderInformation.empty());
                        final LeaderInformation confirmedLeaderInformationForContender =
                                confirmedLeaderInformation.forContenderIdOrEmpty(contenderID);

                        notifyLeaderInformationChangeInternal(
                                contenderID,
                                externallyChangedLeaderInformationForContender,
                                confirmedLeaderInformationForContender);
                    });
        }
    }

    private class LeaderElectionFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable throwable) {
            forwardErrorToLeaderContender(throwable);
        }
    }
}
