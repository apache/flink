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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
public class DefaultLeaderElectionService extends AbstractLeaderElectionService
        implements LeaderElectionEventHandler, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    private final Object lock = new Object();

    private final LeaderElectionDriverFactory leaderElectionDriverFactory;

    /**
     * {@code leaderContender} being {@code null} indicates that no {@link LeaderContender} is
     * registered that participates in the leader election, yet. See {@link
     * #register(LeaderContender)} and {@link #remove(LeaderContender)} for lifecycle management.
     *
     * <p>{@code @Nullable} isn't used here to avoid having multiple warnings spread over this class
     * in a supporting IDE.
     */
    @GuardedBy("lock")
    private LeaderContender leaderContender;

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
     * Saves the leader information for a registered {@link LeaderContender} after this contender
     * confirmed the leadership.
     */
    @GuardedBy("lock")
    private LeaderInformation confirmedLeaderInformation;

    /**
     * {@code leaderElectionDriver} being {@code null} indicates that the connection to the
     * LeaderElection backend isn't established, yet. See {@link #startLeaderElectionBackend()} and
     * {@link #close()} for lifecycle management. The lifecycle of the driver should have been
     * established before registering a {@link LeaderContender} and stopped after the contender has
     * been removed.
     *
     * <p>{@code @Nullable} isn't used here to avoid having multiple warnings spread over this class
     * in a supporting IDE.
     */
    @GuardedBy("lock")
    private LeaderElectionDriver leaderElectionDriver;

    @GuardedBy("lock")
    private final ExecutorService leadershipOperationExecutor;

    public DefaultLeaderElectionService(LeaderElectionDriverFactory leaderElectionDriverFactory) {
        this(
                leaderElectionDriverFactory,
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "DefaultLeaderElectionService-leadershipOperationExecutor")));
    }

    @VisibleForTesting
    DefaultLeaderElectionService(
            LeaderElectionDriverFactory leaderElectionDriverFactory,
            ExecutorService leadershipOperationExecutor) {
        this.leaderElectionDriverFactory = checkNotNull(leaderElectionDriverFactory);

        this.leaderContender = null;

        this.issuedLeaderSessionID = null;

        this.leaderElectionDriver = null;

        this.confirmedLeaderInformation = LeaderInformation.empty();

        this.leadershipOperationExecutor = Preconditions.checkNotNull(leadershipOperationExecutor);
    }

    /**
     * Starts the leader election process. This method has to be called before registering a {@link
     * LeaderContender}. This method could be moved into the {@code DefaultLeaderElectionService}'s
     * constructor with FLINK-31837.
     */
    public void startLeaderElectionBackend() throws Exception {
        synchronized (lock) {
            Preconditions.checkState(
                    leaderContender == null,
                    "No LeaderContender should have been registered, yet.");

            leaderElectionDriver =
                    leaderElectionDriverFactory.createLeaderElectionDriver(
                            this, new LeaderElectionFatalErrorHandler());

            LOG.info("Instantiating DefaultLeaderElectionService with {}.", leaderElectionDriver);
        }
    }

    @Override
    protected void register(LeaderContender contender) throws Exception {
        checkNotNull(contender, "Contender must not be null.");

        synchronized (lock) {
            Preconditions.checkState(
                    leaderContender == null,
                    "Only one LeaderContender is allowed to be registered to this service.");
            Preconditions.checkState(
                    leaderElectionDriver != null,
                    "The DefaultLeaderElectionService should have established a connection to the backend before it's started.");

            leaderContender = contender;

            LOG.info(
                    "LeaderContender {} has been registered for {}.",
                    contender.getDescription(),
                    leaderElectionDriver);

            if (issuedLeaderSessionID != null) {
                // notifying the LeaderContender shouldn't happen in the contender's main thread
                runInLeaderEventThread(
                        () -> notifyLeaderContenderOfLeadership(issuedLeaderSessionID));
            }
        }
    }

    @Override
    protected final void remove(LeaderContender contender) {
        Preconditions.checkArgument(contender == this.leaderContender);
        LOG.info("Stopping DefaultLeaderElectionService.");

        synchronized (lock) {
            if (leaderContender == null) {
                LOG.debug(
                        "The stop procedure was called on an already stopped DefaultLeaderElectionService instance. No action necessary.");
                return;
            }

            if (issuedLeaderSessionID != null) {
                notifyLeaderContenderOfLeadershipLoss();
                LOG.debug(
                        "DefaultLeaderElectionService is stopping while having the leadership acquired. The revoke event is forwarded to the LeaderContender.");

                if (leaderElectionDriver.hasLeadership()) {
                    leaderElectionDriver.writeLeaderInformation(LeaderInformation.empty());
                    LOG.debug("Leader information is cleaned up while stopping.");
                }
            } else {
                Preconditions.checkState(
                        confirmedLeaderInformation.isEmpty(),
                        "The confirmed leader information should have been cleared.");

                LOG.debug(
                        "DefaultLeaderElectionService is stopping while not having the leadership acquired. No cleanup necessary.");
            }

            leaderContender = null;
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            Preconditions.checkState(
                    leaderContender == null,
                    "The DefaultLeaderElectionService should have been stopped before closing the instance.");

            issuedLeaderSessionID = null;

            if (leaderElectionDriver != null) {
                leaderElectionDriver.close();
                leaderElectionDriver = null;

                // The shutdown of the thread pool needs to be done forcefully because we want its
                // lifecycle being coupled to the driver (which require it to be shut down within
                // the lock) to allow null checks in runInLeaderEventThread method. The outstanding
                // event handling callbacks are going to be ignored, anyway.
                final List<Runnable> outstandingEventHandlingCalls =
                        Preconditions.checkNotNull(leadershipOperationExecutor).shutdownNow();
                if (!outstandingEventHandlingCalls.isEmpty()) {
                    LOG.debug(
                            "The DefaultLeaderElectionService was closed with {} still not being processed. No further action necessary.",
                            outstandingEventHandlingCalls.size() == 1
                                    ? "one event"
                                    : (outstandingEventHandlingCalls.size() + " events"));
                }
            } else {
                LOG.debug("The HA backend connection isn't established. No actions taken.");
            }
        }
    }

    @Override
    protected void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        LOG.debug("Confirm leader session ID {} for leader {}.", leaderSessionID, leaderAddress);

        checkNotNull(leaderSessionID);

        synchronized (lock) {
            if (hasLeadership(leaderSessionID)) {
                Preconditions.checkState(
                        confirmedLeaderInformation.isEmpty(),
                        "No confirmation should have happened, yet.");

                confirmedLeaderInformation =
                        LeaderInformation.known(leaderSessionID, leaderAddress);
                leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
            } else {
                if (!leaderSessionID.equals(this.issuedLeaderSessionID)) {
                    LOG.debug(
                            "Receive an old confirmation call of leader session ID {}, current issued session ID is {}",
                            leaderSessionID,
                            issuedLeaderSessionID);
                } else {
                    LOG.warn(
                            "The leader session ID {} was confirmed even though the "
                                    + "corresponding service was not elected as the leader or has been stopped already.",
                            leaderSessionID);
                }
            }
        }
    }

    @Override
    protected boolean hasLeadership(UUID leaderSessionId) {
        synchronized (lock) {
            if (leaderElectionDriver != null) {
                if (leaderContender != null) {
                    return leaderElectionDriver.hasLeadership()
                            && leaderSessionId.equals(issuedLeaderSessionID);
                } else {
                    LOG.debug(
                            "hasLeadership is called after the service is stopped, returning false.");
                    return false;
                }
            } else {
                LOG.debug("hasLeadership is called after the service is closed, returning false.");
                return false;
            }
        }
    }

    /** Returns the current leader session ID or {@code null}, if the session wasn't confirmed. */
    @VisibleForTesting
    @Nullable
    public UUID getLeaderSessionID() {
        synchronized (lock) {
            return confirmedLeaderInformation.getLeaderSessionID();
        }
    }

    @Override
    public void onGrantLeadership(UUID newLeaderSessionId) {
        runInLeaderEventThread(() -> onGrantLeadershipInternal(newLeaderSessionId));
    }

    @GuardedBy("lock")
    private void onGrantLeadershipInternal(UUID newLeaderSessionId) {
        Preconditions.checkNotNull(newLeaderSessionId);

        Preconditions.checkState(
                issuedLeaderSessionID == null,
                "The leadership should have been granted while not having the leadership acquired.");

        issuedLeaderSessionID = newLeaderSessionId;

        notifyLeaderContenderOfLeadership(issuedLeaderSessionID);
    }

    @GuardedBy("lock")
    private void notifyLeaderContenderOfLeadership(UUID sessionID) {
        if (leaderContender == null) {
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
                confirmedLeaderInformation.isEmpty(),
                "The leadership should have been granted while not having the leadership acquired.");

        LOG.debug(
                "Granting leadership to contender {} with session ID {}.",
                leaderContender.getDescription(),
                issuedLeaderSessionID);

        leaderContender.grantLeadership(issuedLeaderSessionID);
    }

    @Override
    public void onRevokeLeadership() {
        runInLeaderEventThread(this::onRevokeLeadershipInternal);
    }

    @GuardedBy("lock")
    private void onRevokeLeadershipInternal() {
        // TODO: FLINK-31814 covers adding this Precondition
        // Preconditions.checkState(issuedLeaderSessionID != null,"The leadership should have
        // been revoked while having the leadership acquired.");

        if (leaderContender != null) {
            notifyLeaderContenderOfLeadershipLoss();
        } else {
            LOG.debug(
                    "The revoke leadership for session {} notification is not forwarded because the DefaultLeaderElectionService({}) has no contender registered.",
                    issuedLeaderSessionID,
                    leaderElectionDriver);
        }

        issuedLeaderSessionID = null;
    }

    @GuardedBy("lock")
    private void notifyLeaderContenderOfLeadershipLoss() {
        Preconditions.checkState(
                leaderContender != null,
                "The LeaderContender should be always set when calling this method.");

        if (confirmedLeaderInformation.isEmpty()) {
            LOG.debug(
                    "Revoking leadership to contender {} while a previous leadership grant wasn't confirmed, yet.",
                    leaderContender.getDescription());
        } else {
            LOG.debug(
                    "Revoking leadership to contender {} for {}.",
                    leaderContender.getDescription(),
                    LeaderElectionUtils.convertToString(confirmedLeaderInformation));
        }

        confirmedLeaderInformation = LeaderInformation.empty();
        leaderContender.revokeLeadership();
    }

    @Override
    public void onLeaderInformationChange(LeaderInformation leaderInformation) {
        runInLeaderEventThread(() -> onLeaderInformationChangeInternal(leaderInformation));
    }

    @GuardedBy("lock")
    private void onLeaderInformationChangeInternal(LeaderInformation leaderInformation) {
        if (leaderContender != null) {
            LOG.trace(
                    "Leader node changed while {} is the leader with {}. New leader information {}.",
                    leaderContender.getDescription(),
                    LeaderElectionUtils.convertToString(confirmedLeaderInformation),
                    LeaderElectionUtils.convertToString(leaderInformation));
            if (!confirmedLeaderInformation.isEmpty()) {
                final LeaderInformation confirmedLeaderInfo = this.confirmedLeaderInformation;
                if (leaderInformation.isEmpty()) {
                    LOG.debug(
                            "Writing leader information by {} since the external storage is empty.",
                            leaderContender.getDescription());
                    leaderElectionDriver.writeLeaderInformation(confirmedLeaderInfo);
                } else if (!leaderInformation.equals(confirmedLeaderInfo)) {
                    // the data field does not correspond to the expected leader information
                    LOG.debug(
                            "Correcting leader information by {}.",
                            leaderContender.getDescription());
                    leaderElectionDriver.writeLeaderInformation(confirmedLeaderInfo);
                }
            }
        } else {
            LOG.debug(
                    "Ignoring change notification since the {} has already been stopped.",
                    leaderElectionDriver);
        }
    }

    private void runInLeaderEventThread(Runnable callback) {
        synchronized (lock) {
            if (!leadershipOperationExecutor.isShutdown()) {
                FutureUtils.handleUncaughtException(
                        CompletableFuture.runAsync(
                                () -> {
                                    synchronized (lock) {
                                        callback.run();
                                    }
                                },
                                leadershipOperationExecutor),
                        (thread, error) -> forwardErrorToLeaderContender(error));
            } else {
                LOG.debug(
                        "Leader event handling was triggered after the DefaultLeaderElectionService is closed. The event will be ignored.");
            }
        }
    }

    private void forwardErrorToLeaderContender(Throwable t) {
        synchronized (lock) {
            if (leaderContender == null) {
                LOG.debug("Ignoring error notification since there's no contender registered.");
                return;
            }

            if (t instanceof LeaderElectionException) {
                leaderContender.handleError((LeaderElectionException) t);
            } else {
                leaderContender.handleError(new LeaderElectionException(t));
            }
        }
    }

    private class LeaderElectionFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable throwable) {
            forwardErrorToLeaderContender(throwable);
        }
    }
}
