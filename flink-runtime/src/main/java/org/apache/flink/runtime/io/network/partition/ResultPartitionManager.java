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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.netty.NettyPartitionRequestListener;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a task
 * manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

    private final Map<ResultPartitionID, ResultPartition> registeredPartitions = new HashMap<>(16);

    private final Map<ResultPartitionID, PartitionRequestListenerManager> listenerManagers =
            new HashMap<>(16);

    @Nullable private final ScheduledExecutor partitionListenerTimeoutChecker;

    private final int partitionListenerTimeout;

    private boolean isShutdown;

    @VisibleForTesting
    public ResultPartitionManager() {
        this(0, null);
    }

    public ResultPartitionManager(
            int partitionListenerTimeout, ScheduledExecutor partitionListenerTimeoutChecker) {
        this.partitionListenerTimeoutChecker = partitionListenerTimeoutChecker;
        this.partitionListenerTimeout = partitionListenerTimeout;
        if (partitionListenerTimeout > 0 && partitionListenerTimeoutChecker != null) {
            this.partitionListenerTimeoutChecker.schedule(
                    this::checkRequestPartitionListeners,
                    partitionListenerTimeout,
                    TimeUnit.MILLISECONDS);
        }
    }

    public void registerResultPartition(ResultPartition partition) throws IOException {
        synchronized (registeredPartitions) {
            checkState(!isShutdown, "Result partition manager already shut down.");

            ResultPartition previous =
                    registeredPartitions.put(partition.getPartitionId(), partition);

            if (previous != null) {
                throw new IllegalStateException("Result partition already registered.");
            }

            PartitionRequestListenerManager listenerManager =
                    listenerManagers.remove(partition.getPartitionId());
            if (listenerManager != null) {
                for (PartitionRequestListener listener :
                        listenerManager.getPartitionRequestListeners()) {
                    listener.notifyPartitionCreated(partition);
                }
            }

            LOG.debug("Registered {}.", partition);
        }
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            int subpartitionIndex,
            BufferAvailabilityListener availabilityListener)
            throws IOException {

        final ResultSubpartitionView subpartitionView;
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                throw new PartitionNotFoundException(partitionId);
            }

            LOG.debug("Requesting subpartition {} of {}.", subpartitionIndex, partition);

            subpartitionView =
                    partition.createSubpartitionView(subpartitionIndex, availabilityListener);
        }

        return subpartitionView;
    }

    @Override
    public Optional<ResultSubpartitionView> createSubpartitionViewOrRegisterListener(
            ResultPartitionID partitionId,
            int subpartitionIndex,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestListener listener)
            throws IOException {

        final ResultSubpartitionView subpartitionView;
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                listenerManagers
                        .computeIfAbsent(partitionId, key -> new PartitionRequestListenerManager())
                        .registerListener(listener);
                subpartitionView = null;
            } else {

                LOG.debug("Requesting subpartition {} of {}.", subpartitionIndex, partition);

                subpartitionView =
                        partition.createSubpartitionView(subpartitionIndex, availabilityListener);
            }
        }

        return subpartitionView == null ? Optional.empty() : Optional.of(subpartitionView);
    }

    @Override
    public void releasePartitionRequestListener(NettyPartitionRequestListener listener) {
        synchronized (registeredPartitions) {
            PartitionRequestListenerManager listeners =
                    listenerManagers.get(listener.getResultPartitionId());
            if (listeners != null) {
                listeners.remove(listener.getReceiverId());
                if (listeners.isEmpty()) {
                    listenerManagers.remove(listener.getResultPartitionId());
                }
            }
        }
    }

    public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
        synchronized (registeredPartitions) {
            ResultPartition resultPartition = registeredPartitions.remove(partitionId);
            if (resultPartition != null) {
                resultPartition.release(cause);
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
            }
            PartitionRequestListenerManager listenerManager = listenerManagers.remove(partitionId);
            if (listenerManager != null && !listenerManager.isEmpty()) {
                for (PartitionRequestListener listener :
                        listenerManager.getPartitionRequestListeners()) {
                    listener.notifyPartitionCreatedTimeout();
                }
            }
        }
    }

    public void shutdown() {
        synchronized (registeredPartitions) {
            LOG.debug(
                    "Releasing {} partitions because of shutdown.",
                    registeredPartitions.values().size());

            for (ResultPartition partition : registeredPartitions.values()) {
                partition.release();
            }

            registeredPartitions.clear();

            releaseListenerManagers();

            isShutdown = true;

            LOG.debug("Successful shutdown.");
        }
    }

    private void releaseListenerManagers() {
        for (PartitionRequestListenerManager listenerManager : listenerManagers.values()) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreatedTimeout();
            }
        }
        listenerManagers.clear();
    }

    /** Check whether the partition request listener is timeout. */
    private void checkRequestPartitionListeners() {
        List<PartitionRequestListener> timeoutPartitionRequestListeners = new LinkedList<>();
        synchronized (registeredPartitions) {
            if (isShutdown) {
                return;
            }
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<ResultPartitionID, PartitionRequestListenerManager>> iterator =
                    listenerManagers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ResultPartitionID, PartitionRequestListenerManager> entry =
                        iterator.next();
                PartitionRequestListenerManager partitionRequestListeners = entry.getValue();
                partitionRequestListeners.removeExpiration(
                        now, partitionListenerTimeout, timeoutPartitionRequestListeners);
                if (partitionRequestListeners.isEmpty()) {
                    iterator.remove();
                }
            }
        }
        for (PartitionRequestListener partitionRequestListener : timeoutPartitionRequestListeners) {
            partitionRequestListener.notifyPartitionCreatedTimeout();
        }
        if (partitionListenerTimeoutChecker != null) {
            partitionListenerTimeoutChecker.schedule(
                    this::checkRequestPartitionListeners,
                    partitionListenerTimeout,
                    TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    Map<ResultPartitionID, PartitionRequestListenerManager> getListenerManagers() {
        return listenerManagers;
    }

    // ------------------------------------------------------------------------
    // Notifications
    // ------------------------------------------------------------------------

    void onConsumedPartition(ResultPartition partition) {
        LOG.debug("Received consume notification from {}.", partition);

        synchronized (registeredPartitions) {
            final ResultPartition previous =
                    registeredPartitions.remove(partition.getPartitionId());
            // Release the partition if it was successfully removed
            if (partition == previous) {
                partition.release();
                ResultPartitionID partitionId = partition.getPartitionId();
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
            }
            PartitionRequestListenerManager listenerManager =
                    listenerManagers.remove(partition.getPartitionId());
            checkState(
                    listenerManager == null || listenerManager.isEmpty(),
                    "The partition request listeners is not empty for "
                            + partition.getPartitionId());
        }
    }

    public Collection<ResultPartitionID> getUnreleasedPartitions() {
        synchronized (registeredPartitions) {
            return registeredPartitions.keySet();
        }
    }
}
