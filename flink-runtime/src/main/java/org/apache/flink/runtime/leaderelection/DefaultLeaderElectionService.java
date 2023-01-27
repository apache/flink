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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for leader election service. Composed with different {@link
 * LeaderElectionDriver}, we could perform a leader election for the contender, and then persist the
 * leader information to various storage.
 */
public class DefaultLeaderElectionService extends AbstractLeaderElectionService
        implements LeaderElectionEventHandler, MultipleComponentLeaderElectionDriver.Listener {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    private final Object lock = new Object();

    private final LeaderElectionDriverFactory leaderElectionDriverFactory;

    @GuardedBy("lock")
    private volatile String contenderID;

    /** The leader contender which applies for leadership. */
    @GuardedBy("lock")
    // @Nullable is commented-out to avoid having multiple warnings spread over this class
    // this.running=true ensures that leaderContender != null
    private volatile LeaderContender leaderContender;

    @GuardedBy("lock")
    @Nullable
    private volatile UUID issuedLeaderSessionID;

    @GuardedBy("lock")
    private volatile LeaderInformation confirmedLeaderInformation;

    @GuardedBy("lock")
    private volatile boolean running;

    // @Nullable is commented-out to avoid having multiple warnings spread over this class
    // this.running=true ensures that leaderContender != null
    private LeaderElectionDriver leaderElectionDriver;

    public DefaultLeaderElectionService(LeaderElectionDriverFactory leaderElectionDriverFactory) {
        this.leaderElectionDriverFactory = checkNotNull(leaderElectionDriverFactory);

        this.leaderContender = null;

        this.issuedLeaderSessionID = null;

        this.leaderElectionDriver = null;

        this.confirmedLeaderInformation = LeaderInformation.empty();

        this.running = false;
    }

    @Override
    public void startLeaderElectionBackend() throws Exception {
        synchronized (lock) {
            leaderElectionDriver =
                    leaderElectionDriverFactory.createLeaderElectionDriver(
                            // TODO: FLINK-26522 - leaderContenderDescription is only used within
                            // the ZooKeeperLeaderElectionDriver for when the connection is lost
                            // This can be removed later on by the contender ID
                            this, new LeaderElectionFatalErrorHandler(), "unused");
        }
    }

    @Override
    protected void register(String contenderID, LeaderContender contender) throws Exception {
        checkNotNull(contender, "Contender must not be null.");
        Preconditions.checkState(leaderContender == null, "Contender was already set.");
        Preconditions.checkState(
                this.contenderID == null, "There shouldn't be any contenderID set, yet.");

        synchronized (lock) {
            running = true;
            this.contenderID = contenderID;
            leaderContender = contender;
            LOG.info("Starting DefaultLeaderElectionService with {}.", leaderElectionDriver);
        }
    }

    @Override
    protected void remove(String contenderID) {
        Preconditions.checkNotNull(contenderID);
        Preconditions.checkArgument(contenderID.equals(this.contenderID));

        synchronized (lock) {
            leaderContender.revokeLeadership();
            this.contenderID = null;
            this.leaderContender = null;
            LOG.info("LeaderContender for {} was removed.", contenderID);
        }
    }

    @Override
    public final void close() throws Exception {
        LOG.info("Stopping DefaultLeaderElectionService.");

        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;
            handleLeadershipLoss();
        }

        leaderElectionDriver.close();
    }

    @Override
    protected void confirmLeadership(String contenderID, UUID leaderSessionID, String leaderAddress) {
        LOG.debug("Confirm leader session ID {} for leader {}.", leaderSessionID, leaderAddress);

        checkNotNull(leaderSessionID);

        synchronized (lock) {
            if (hasLeadership(contenderID, leaderSessionID)) {
                if (running) {
                    confirmLeaderInformation(leaderSessionID, leaderAddress);
                } else {
                    LOG.debug(
                            "Ignoring the leader session Id {} confirmation, since the LeaderElectionService has already been stopped.",
                            leaderSessionID);
                }
            } else {
                // Received an old confirmation call
                if (!leaderSessionID.equals(this.issuedLeaderSessionID)) {
                    LOG.debug(
                            "Receive an old confirmation call of leader session ID {}, current issued session ID is {}",
                            leaderSessionID,
                            issuedLeaderSessionID);
                } else {
                    LOG.warn(
                            "The leader session ID {} was confirmed even though the "
                                    + "corresponding JobManager was not elected as the leader.",
                            leaderSessionID);
                }
            }
        }
    }

    @Override
    protected boolean hasLeadership(String contenderID, UUID leaderSessionId) {
        synchronized (lock) {
            if (running) {
                return contenderID.equals(this.contenderID)
                        && leaderElectionDriver.hasLeadership()
                        && leaderSessionId.equals(issuedLeaderSessionID);
            } else {
                LOG.debug("hasLeadership is called after the service is stopped, returning false.");
                return false;
            }
        }
    }

    /**
     * Returns the current leader session ID or null, if the contender is not the leader.
     *
     * @return The last leader session ID or null, if the contender is not the leader
     */
    @VisibleForTesting
    @Nullable
    public UUID getLeaderSessionID() {
        return confirmedLeaderInformation.getLeaderSessionID();
    }

    @GuardedBy("lock")
    private void confirmLeaderInformation(UUID leaderSessionID, String leaderAddress) {
        confirmedLeaderInformation = LeaderInformation.known(leaderSessionID, leaderAddress);
        leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
    }

    @Override
    public void onGrantLeadership(UUID newLeaderSessionId) {
        synchronized (lock) {
            if (running) {
                issuedLeaderSessionID = newLeaderSessionId;
                confirmedLeaderInformation = LeaderInformation.empty();

                LOG.debug(
                        "Grant leadership to contender {} with session ID {}.",
                        leaderContender.getDescription(),
                        issuedLeaderSessionID);

                leaderContender.grantLeadership(issuedLeaderSessionID);
            } else {
                LOG.debug(
                        "Ignoring the grant leadership notification since the {} has already been closed.",
                        leaderElectionDriver);
            }
        }
    }

    @Override
    public void onRevokeLeadership() {
        synchronized (lock) {
            if (running) {
                handleLeadershipLoss();
            } else {
                LOG.debug(
                        "Ignoring the revoke leadership notification since the {} "
                                + "has already been closed.",
                        leaderElectionDriver);
            }
        }
    }

    @GuardedBy("lock")
    private void handleLeadershipLoss() {
        LOG.debug(
                "Revoke leadership of {} ({}@{}).",
                leaderContender.getDescription(),
                confirmedLeaderInformation.getLeaderSessionID(),
                confirmedLeaderInformation.getLeaderAddress());

        issuedLeaderSessionID = null;
        confirmedLeaderInformation = LeaderInformation.empty();

        leaderContender.revokeLeadership();

        LOG.debug("Clearing the leader information on {}.", leaderElectionDriver);

        // Clear the old leader information on the external storage
        leaderElectionDriver.writeLeaderInformation(LeaderInformation.empty());
    }

    @Override
    public void onLeaderInformationChange(
            Map<String, LeaderInformation> leaderInformationPerContender) {
        synchronized (lock) {
            if (running) {
                // FLINK-26522: Intermediate state where DefaultLeaderElectionService doesn't
                // support multiple contenders
                final LeaderInformation leaderInformation;
                if (leaderInformationPerContender.isEmpty()) {
                    leaderInformation = LeaderInformation.empty();
                } else if (leaderInformationPerContender.size() == 1) {
                    leaderInformation = leaderInformationPerContender.values().iterator().next();
                } else {
                    throw new IllegalArgumentException(
                            "Invalid number of LeaderInformation records.");
                }

                LOG.trace(
                        "Leader node changed while {} is the leader with session ID {}. New leader information {}.",
                        leaderContender.getDescription(),
                        confirmedLeaderInformation.getLeaderSessionID(),
                        leaderInformation);
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
                        "Ignoring change notification since the {} has " + "already been closed.",
                        leaderElectionDriver);
            }
        }
    }

    @Override
    public void isLeader() {
        onGrantLeadership(UUID.randomUUID());
    }

    @Override
    public void notLeader() {
        onRevokeLeadership();
    }

    @Override
    public void notifyLeaderInformationChange(
            String componentId, LeaderInformation leaderInformation) {
        onLeaderInformationChange(Collections.singletonMap(componentId, leaderInformation));
    }

    @Override
    public void notifyAllKnownLeaderInformation(
            Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds) {
        onLeaderInformationChange(
                leaderInformationWithComponentIds.stream()
                        .collect(
                                Collectors.toMap(
                                        LeaderInformationWithComponentId::getComponentId,
                                        LeaderInformationWithComponentId::getLeaderInformation)));
    }

    private class LeaderElectionFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable throwable) {
            synchronized (lock) {
                if (!running) {
                    LOG.debug("Ignoring error notification since the service has been stopped.");
                    return;
                }

                if (throwable instanceof LeaderElectionException) {
                    leaderContender.handleError((LeaderElectionException) throwable);
                } else {
                    leaderContender.handleError(new LeaderElectionException(throwable));
                }
            }
        }
    }
}
