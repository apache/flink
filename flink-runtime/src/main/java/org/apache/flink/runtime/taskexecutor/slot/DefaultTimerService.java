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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Service to register timeouts for a given key. The timeouts are identified by a ticket so that
 * newly registered timeouts for the same key can be distinguished from older timeouts.
 *
 * @param <K> Type of the key
 */
public class DefaultTimerService<K> implements TimerService<K> {

    /** Executor service for the scheduled timeouts. */
    private final ScheduledExecutorService scheduledExecutorService;

    /** Timeout for the shutdown of the service. */
    private final long shutdownTimeout;

    /** Map of currently active timeouts. */
    private final Map<K, Timeout<K>> timeouts;

    /** Listener which is notified about occurring timeouts. */
    private TimeoutListener<K> timeoutListener;

    public DefaultTimerService(
            final ScheduledExecutorService scheduledExecutorService, final long shutdownTimeout) {
        this.scheduledExecutorService = Preconditions.checkNotNull(scheduledExecutorService);

        Preconditions.checkArgument(
                shutdownTimeout >= 0L,
                "The shut down timeout must be larger than or equal than 0.");
        this.shutdownTimeout = shutdownTimeout;

        this.timeouts = CollectionUtil.newHashMapWithExpectedSize(16);
        this.timeoutListener = null;
    }

    @Override
    public void start(TimeoutListener<K> initialTimeoutListener) {
        // sanity check; We only allow to assign a timeout listener once
        Preconditions.checkState(!scheduledExecutorService.isShutdown());
        Preconditions.checkState(timeoutListener == null);

        this.timeoutListener = Preconditions.checkNotNull(initialTimeoutListener);
    }

    @Override
    public void stop() {
        unregisterAllTimeouts();

        timeoutListener = null;

        ExecutorUtils.gracefulShutdown(
                shutdownTimeout, TimeUnit.MILLISECONDS, scheduledExecutorService);
    }

    @Override
    public void registerTimeout(final K key, final long delay, final TimeUnit unit) {
        Preconditions.checkState(
                timeoutListener != null,
                "The " + getClass().getSimpleName() + " has not been started.");

        if (timeouts.containsKey(key)) {
            unregisterTimeout(key);
        }

        timeouts.put(
                key, new Timeout<>(timeoutListener, key, delay, unit, scheduledExecutorService));
    }

    @Override
    public void unregisterTimeout(K key) {
        Timeout<K> timeout = timeouts.remove(key);

        if (timeout != null) {
            timeout.cancel();
        }
    }

    /** Unregister all timeouts. */
    protected void unregisterAllTimeouts() {
        for (Timeout<K> timeout : timeouts.values()) {
            timeout.cancel();
        }
        timeouts.clear();
    }

    @Override
    public boolean isValid(K key, UUID ticket) {
        if (timeouts.containsKey(key)) {
            Timeout<K> timeout = timeouts.get(key);

            return timeout.getTicket().equals(ticket);
        } else {
            return false;
        }
    }

    @VisibleForTesting
    Map<K, Timeout<K>> getTimeouts() {
        return timeouts;
    }

    // ---------------------------------------------------------------------
    // Static utility classes
    // ---------------------------------------------------------------------

    @VisibleForTesting
    static final class Timeout<K> implements Runnable {

        private final TimeoutListener<K> timeoutListener;
        private final K key;
        private final ScheduledFuture<?> scheduledTimeout;
        private final UUID ticket;

        Timeout(
                final TimeoutListener<K> timeoutListener,
                final K key,
                final long delay,
                final TimeUnit unit,
                final ScheduledExecutorService scheduledExecutorService) {

            Preconditions.checkNotNull(scheduledExecutorService);

            this.timeoutListener = Preconditions.checkNotNull(timeoutListener);
            this.key = Preconditions.checkNotNull(key);
            this.scheduledTimeout = scheduledExecutorService.schedule(this, delay, unit);
            this.ticket = UUID.randomUUID();
        }

        UUID getTicket() {
            return ticket;
        }

        void cancel() {
            scheduledTimeout.cancel(true);
        }

        @Override
        public void run() {
            timeoutListener.notifyTimeout(key, ticket);
        }
    }
}
