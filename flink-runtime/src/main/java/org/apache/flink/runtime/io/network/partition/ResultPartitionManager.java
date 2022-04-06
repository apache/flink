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
import org.apache.flink.runtime.io.network.netty.NettyPartitionRequestNotifier;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a task
 * manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

    private final Map<ResultPartitionID, ResultPartition> registeredPartitions = new HashMap<>(16);

    private final Map<ResultPartitionID, InputRequestNotifierManager> requestPartitionNotifiers = new HashMap<>(16);

    private final ScheduledExecutor partitionNotifierTimeoutChecker;

    private final Duration partitionNotifierTimeout;

    private boolean isShutdown;

    @VisibleForTesting
    public ResultPartitionManager() {
        this(
                Duration.ZERO,
                new ScheduledExecutorServiceAdapter(
                        Executors.newSingleThreadScheduledExecutor(
                                new ExecutorThreadFactory("partition-notifier-timeout-checker"))));
    }

    public ResultPartitionManager(Duration partitionNotifierTimeout, ScheduledExecutor partitionNotifierTimeoutChecker) {
        this.partitionNotifierTimeoutChecker = partitionNotifierTimeoutChecker;
        this.partitionNotifierTimeout = partitionNotifierTimeout;
        if (partitionNotifierTimeout != null && partitionNotifierTimeout.toMillis() > 0) {
            this.partitionNotifierTimeoutChecker.schedule(
                    this::checkRequestPartitionNotifiers,
                    partitionNotifierTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    public void registerResultPartition(ResultPartition partition) {
        synchronized (registeredPartitions) {
            checkState(!isShutdown, "Result partition manager already shut down.");

            ResultPartition previous =
                    registeredPartitions.put(partition.getPartitionId(), partition);

            if (previous != null) {
                throw new IllegalStateException("Result partition already registered.");
            }

            InputRequestNotifierManager notifiers = requestPartitionNotifiers.remove(partition.getPartitionId());
            if (notifiers != null) {
                for (PartitionRequestNotifier notifier : notifiers.getPartitionRequestNotifiers()) {
                    try {
                        notifier.notifyPartitionRequest(partition);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
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
    public ResultSubpartitionView createSubpartitionViewOrNotify(
            ResultPartitionID partitionId,
            int subpartitionIndex,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestNotifier notifier) throws IOException {

        final ResultSubpartitionView subpartitionView;
        synchronized (registeredPartitions) {
            final ResultPartition partition = registeredPartitions.get(partitionId);

            if (partition == null) {
                requestPartitionNotifiers.computeIfAbsent(partitionId, key -> new InputRequestNotifierManager()).addNotifier(notifier);
                return null;
            }

            LOG.debug("Requesting subpartition {} of {}.", subpartitionIndex, partition);

            subpartitionView =
                    partition.createSubpartitionView(subpartitionIndex, availabilityListener);
        }

        return subpartitionView;
    }

    @Override
    public void releasePartitionRequestNotifier(NettyPartitionRequestNotifier notifier) {
        synchronized (registeredPartitions) {
            InputRequestNotifierManager notifiers = requestPartitionNotifiers.get(notifier.getResultPartitionId());
            if (notifiers != null) {
                notifiers.remove(notifier.getReceiverId());
                if (notifiers.isEmpty()) {
                    requestPartitionNotifiers.remove(notifier.getResultPartitionId());
                }
            }
        }
    }

    public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
        synchronized (registeredPartitions) {
            requestPartitionNotifiers.remove(partitionId);
            ResultPartition resultPartition = registeredPartitions.remove(partitionId);
            if (resultPartition != null) {
                resultPartition.release(cause);
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
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

            requestPartitionNotifiers.clear();

            isShutdown = true;

            LOG.debug("Successful shutdown.");
        }
    }

    /**
     * Check whether the partition notifier is timeout.
     */
    private void checkRequestPartitionNotifiers() {
        List<PartitionRequestNotifier> timeoutPartitionRequestNotifiers = new LinkedList<>();
        synchronized (registeredPartitions) {
            if (isShutdown) {
                return;
            }
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<ResultPartitionID, InputRequestNotifierManager>> iterator = requestPartitionNotifiers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ResultPartitionID, InputRequestNotifierManager> entry = iterator.next();
                InputRequestNotifierManager partitionRequestNotifiers = entry.getValue();
                partitionRequestNotifiers.removeExpiration(now, partitionNotifierTimeout.toMillis(), timeoutPartitionRequestNotifiers);
                if (partitionRequestNotifiers.isEmpty()) {
                    iterator.remove();
                }
            }
        }
        for (PartitionRequestNotifier partitionRequestNotifier : timeoutPartitionRequestNotifiers) {
            partitionRequestNotifier.notifyPartitionRequestTimeout();
        }
        partitionNotifierTimeoutChecker.schedule(this::checkRequestPartitionNotifiers, partitionNotifierTimeout.toMillis(), TimeUnit.MILLISECONDS);
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
            requestPartitionNotifiers.remove(partition.getPartitionId());
        }
    }

    public Collection<ResultPartitionID> getUnreleasedPartitions() {
        synchronized (registeredPartitions) {
            return registeredPartitions.keySet();
        }
    }
}
