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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.curator5.com.google.common.collect.Sets;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.api.CuratorEvent;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.api.CuratorEventType;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointIDCounter} instances for JobManagers running in {@link
 * HighAvailabilityMode#ZOOKEEPER}.
 *
 * <p>Each counter creates a ZNode:
 *
 * <pre>
 * +----O /flink/checkpoint-counter/&lt;job-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/checkpoint-counter/&lt;job-id&gt; N [persistent]
 * </pre>
 *
 * <p>The checkpoints IDs are required to be ascending (per job). In order to guarantee this in case
 * of job manager failures we use ZooKeeper to have a shared counter across job manager instances.
 */
public class ZooKeeperCheckpointIDCounter implements CheckpointIDCounter {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCheckpointIDCounter.class);

    /** Curator ZooKeeper client. */
    private final CuratorFramework client;

    /** Path of the shared count. */
    private final String counterPath;

    /** Curator recipe for shared counts. */
    private final SharedCount sharedCount;

    private final LastStateConnectionStateListener connectionStateListener;

    private final Object startStopLock = new Object();

    @GuardedBy("startStopLock")
    private boolean isStarted;

    /**
     * Creates a {@link ZooKeeperCheckpointIDCounter} instance.
     *
     * @param client Curator ZooKeeper client
     */
    public ZooKeeperCheckpointIDCounter(
            CuratorFramework client, LastStateConnectionStateListener connectionStateListener) {
        this.client = checkNotNull(client, "Curator client");
        this.counterPath = ZooKeeperUtils.getCheckpointIdCounterPath();
        this.sharedCount = new SharedCount(client, counterPath, INITIAL_CHECKPOINT_ID);
        this.connectionStateListener = connectionStateListener;
    }

    @Override
    public void start() throws Exception {
        synchronized (startStopLock) {
            if (!isStarted) {
                sharedCount.start();

                client.getConnectionStateListenable().addListener(connectionStateListener);

                isStarted = true;
            }
        }
    }

    @Override
    public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
        synchronized (startStopLock) {
            if (isStarted) {
                LOG.info("Shutting down.");
                try {
                    sharedCount.close();
                } catch (IOException e) {
                    return FutureUtils.completedExceptionally(e);
                }

                client.getConnectionStateListenable().removeListener(connectionStateListener);

                if (jobStatus.isGloballyTerminalState()) {
                    LOG.info("Removing {} from ZooKeeper", counterPath);
                    try {
                        final CompletableFuture<Void> deletionFuture = new CompletableFuture<>();
                        client.delete()
                                .inBackground(
                                        (curatorFramework, curatorEvent) ->
                                                handleDeletionOfCounterPath(
                                                        curatorEvent, deletionFuture))
                                .forPath(counterPath);
                        return deletionFuture;
                    } catch (Exception e) {
                        return FutureUtils.completedExceptionally(e);
                    }
                }

                isStarted = false;
            }
        }

        return FutureUtils.completedVoidFuture();
    }

    private void handleDeletionOfCounterPath(
            CuratorEvent curatorEvent, CompletableFuture<Void> deletionFuture) {
        Preconditions.checkArgument(
                curatorEvent.getType() == CuratorEventType.DELETE,
                "An unexpected CuratorEvent was monitored: " + curatorEvent.getType());
        Preconditions.checkArgument(
                counterPath.endsWith(curatorEvent.getPath()),
                "An unexpected path was selected for deletion: " + curatorEvent.getPath());

        final KeeperException.Code eventCode =
                KeeperException.Code.get(curatorEvent.getResultCode());
        if (Sets.immutableEnumSet(KeeperException.Code.OK, KeeperException.Code.NONODE)
                .contains(eventCode)) {
            deletionFuture.complete(null);
        } else {
            final String namespacedCounterPath =
                    ZooKeeperUtils.generateZookeeperPath(client.getNamespace(), counterPath);
            deletionFuture.completeExceptionally(
                    new FlinkException(
                            String.format(
                                    "An error occurred while shutting down the CheckpointIDCounter in path '%s'.",
                                    namespacedCounterPath),
                            KeeperException.create(eventCode, namespacedCounterPath)));
        }
    }

    @Override
    public long getAndIncrement() throws Exception {
        while (true) {
            checkConnectionState();

            VersionedValue<Integer> current = sharedCount.getVersionedValue();
            int newCount = current.getValue() + 1;

            if (newCount < 0) {
                // overflow and wrap around
                throw new Exception(
                        "Checkpoint counter overflow. ZooKeeper checkpoint counter only supports "
                                + "checkpoints Ids up to "
                                + Integer.MAX_VALUE);
            }

            if (sharedCount.trySetCount(current, newCount)) {
                return current.getValue();
            }
        }
    }

    @Override
    public long get() {
        checkConnectionState();

        return sharedCount.getVersionedValue().getValue();
    }

    @Override
    public void setCount(long newId) throws Exception {
        checkConnectionState();

        if (newId > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "ZooKeeper checkpoint counter only supports "
                            + "checkpoints Ids up to "
                            + Integer.MAX_VALUE
                            + ", but given value is"
                            + newId);
        }

        sharedCount.setCount((int) newId);
    }

    private void checkConnectionState() {
        final Optional<ConnectionState> optionalLastState = connectionStateListener.getLastState();

        optionalLastState.ifPresent(
                lastState -> {
                    if (lastState != ConnectionState.CONNECTED
                            && lastState != ConnectionState.RECONNECTED) {
                        throw new IllegalStateException("Connection state: " + lastState);
                    }
                });
    }

    @VisibleForTesting
    String getPath() {
        return counterPath;
    }
}
