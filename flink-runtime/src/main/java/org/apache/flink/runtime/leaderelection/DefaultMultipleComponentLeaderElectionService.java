/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Default implementation of a {@link MultipleComponentLeaderElectionService} that allows to
 * register multiple {@link LeaderElectionEventHandler}.
 */
public class DefaultMultipleComponentLeaderElectionService
        implements MultipleComponentLeaderElectionService,
                MultipleComponentLeaderElectionDriver.Listener {
    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultMultipleComponentLeaderElectionService.class);

    private final Object lock = new Object();

    private final MultipleComponentLeaderElectionDriver multipleComponentLeaderElectionDriver;

    private final FatalErrorHandler fatalErrorHandler;

    @GuardedBy("lock")
    private final ExecutorService leadershipOperationExecutor;

    @GuardedBy("lock")
    private final Map<String, LeaderElectionEventHandler> leaderElectionEventHandlers;

    @GuardedBy("lock")
    private boolean running = true;

    @Nullable
    @GuardedBy("lock")
    private UUID currentLeaderSessionId = null;

    @VisibleForTesting
    DefaultMultipleComponentLeaderElectionService(
            FatalErrorHandler fatalErrorHandler,
            MultipleComponentLeaderElectionDriverFactory
                    multipleComponentLeaderElectionDriverFactory,
            ExecutorService leadershipOperationExecutor)
            throws Exception {
        this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);

        this.leadershipOperationExecutor = Preconditions.checkNotNull(leadershipOperationExecutor);

        leaderElectionEventHandlers = new HashMap<>();

        multipleComponentLeaderElectionDriver =
                multipleComponentLeaderElectionDriverFactory.create(this);
    }

    public DefaultMultipleComponentLeaderElectionService(
            FatalErrorHandler fatalErrorHandler,
            MultipleComponentLeaderElectionDriverFactory
                    multipleComponentLeaderElectionDriverFactory)
            throws Exception {
        this(
                fatalErrorHandler,
                multipleComponentLeaderElectionDriverFactory,
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("leadershipOperationExecutor")));
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;

            LOG.info("Closing {}.", this.getClass().getSimpleName());

            ExecutorUtils.gracefulShutdown(10L, TimeUnit.SECONDS, leadershipOperationExecutor);

            multipleComponentLeaderElectionDriver.close();
        }
    }

    @Override
    public LeaderElectionDriverFactory createDriverFactory(String componentId) {
        return new MultipleComponentLeaderElectionDriverAdapterFactory(componentId, this);
    }

    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation) {
        try {
            multipleComponentLeaderElectionDriver.publishLeaderInformation(
                    componentId, leaderInformation);
        } catch (Exception e) {
            fatalErrorHandler.onFatalError(
                    new FlinkException(
                            String.format(
                                    "Could not write leader information %s for leader %s.",
                                    leaderInformation, componentId),
                            e));
        }
    }

    @Override
    public void registerLeaderElectionEventHandler(
            String componentId, LeaderElectionEventHandler leaderElectionEventHandler) {

        synchronized (lock) {
            Preconditions.checkArgument(
                    !leaderElectionEventHandlers.containsKey(componentId),
                    "Do not support duplicate LeaderElectionEventHandler registration under %s",
                    componentId);
            leaderElectionEventHandlers.put(componentId, leaderElectionEventHandler);

            if (currentLeaderSessionId != null) {
                final UUID leaderSessionId = currentLeaderSessionId;
                leadershipOperationExecutor.execute(
                        () -> leaderElectionEventHandler.onGrantLeadership(leaderSessionId));
            }
        }
    }

    @Override
    public void unregisterLeaderElectionEventHandler(String componentId) throws Exception {
        final LeaderElectionEventHandler unregisteredLeaderElectionEventHandler;
        synchronized (lock) {
            unregisteredLeaderElectionEventHandler =
                    leaderElectionEventHandlers.remove(componentId);

            if (unregisteredLeaderElectionEventHandler != null) {
                leadershipOperationExecutor.execute(
                        unregisteredLeaderElectionEventHandler::onRevokeLeadership);
            } else {
                LOG.debug(
                        "Could not find leader election event handler for componentId {}. Ignoring the unregister call.",
                        componentId);
            }
        }

        multipleComponentLeaderElectionDriver.deleteLeaderInformation(componentId);
    }

    @Override
    public boolean hasLeadership(String componentId) {
        synchronized (lock) {
            Preconditions.checkState(running);

            return leaderElectionEventHandlers.containsKey(componentId)
                    && multipleComponentLeaderElectionDriver.hasLeadership();
        }
    }

    @Override
    public void isLeader() {
        final UUID newLeaderSessionId = UUID.randomUUID();
        synchronized (lock) {
            if (!running) {
                return;
            }

            currentLeaderSessionId = UUID.randomUUID();

            forEachLeaderElectionEventHandler(
                    leaderElectionEventHandler ->
                            leaderElectionEventHandler.onGrantLeadership(newLeaderSessionId));
        }
    }

    @Override
    public void notLeader() {
        synchronized (lock) {
            if (!running) {
                return;
            }

            currentLeaderSessionId = null;

            forEachLeaderElectionEventHandler(LeaderElectionEventHandler::onRevokeLeadership);
        }
    }

    @GuardedBy("lock")
    private void forEachLeaderElectionEventHandler(
            Consumer<? super LeaderElectionEventHandler> action) {

        for (LeaderElectionEventHandler leaderElectionEventHandler :
                leaderElectionEventHandlers.values()) {
            leadershipOperationExecutor.execute(() -> action.accept(leaderElectionEventHandler));
        }
    }

    @Override
    public void notifyLeaderInformationChange(
            String componentId, LeaderInformation leaderInformation) {
        synchronized (lock) {
            if (!running) {
                return;
            }

            final LeaderElectionEventHandler leaderElectionEventHandler =
                    leaderElectionEventHandlers.get(componentId);

            if (leaderElectionEventHandler != null) {
                sendLeaderInformationChange(leaderElectionEventHandler, leaderInformation);
            }
        }
    }

    @GuardedBy("lock")
    private void sendLeaderInformationChange(
            LeaderElectionEventHandler leaderElectionEventHandler,
            LeaderInformation leaderInformation) {
        leadershipOperationExecutor.execute(
                () -> leaderElectionEventHandler.onLeaderInformationChange(leaderInformation));
    }

    @Override
    public void notifyAllKnownLeaderInformation(
            Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds) {
        synchronized (lock) {
            if (!running) {
                return;
            }

            final Map<String, LeaderInformation> leaderInformationByName =
                    leaderInformationWithComponentIds.stream()
                            .collect(
                                    Collectors.toMap(
                                            LeaderInformationWithComponentId::getComponentId,
                                            LeaderInformationWithComponentId
                                                    ::getLeaderInformation));

            for (Map.Entry<String, LeaderElectionEventHandler>
                    leaderNameLeaderElectionEventHandlerPair :
                            leaderElectionEventHandlers.entrySet()) {
                final String leaderName = leaderNameLeaderElectionEventHandlerPair.getKey();
                if (leaderInformationByName.containsKey(leaderName)) {
                    sendLeaderInformationChange(
                            leaderNameLeaderElectionEventHandlerPair.getValue(),
                            leaderInformationByName.get(leaderName));
                } else {
                    sendLeaderInformationChange(
                            leaderNameLeaderElectionEventHandlerPair.getValue(),
                            LeaderInformation.empty());
                }
            }
        }
    }
}
