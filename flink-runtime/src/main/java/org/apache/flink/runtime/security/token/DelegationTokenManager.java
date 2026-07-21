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

package org.apache.flink.runtime.security.token;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

/**
 * Manager for delegation tokens in a Flink cluster.
 *
 * <p>When delegation token renewal is enabled, this manager will make sure long-running apps can
 * run without interruption while accessing secured services. It must contact all the configured
 * secure services to obtain delegation tokens to be distributed to the rest of the application.
 */
@Internal
public interface DelegationTokenManager {
    /**
     * Listener for events in the {@link DelegationTokenManager}.
     *
     * <p>By registering it in the manager one can receive callbacks when events are happening.
     */
    @Internal
    interface Listener {
        /** Callback function when new delegation tokens obtained. */
        void onNewTokensObtained(byte[] tokens) throws Exception;
    }

    /**
     * Obtains new tokens in a one-time fashion and leaves it up to the caller to distribute them.
     */
    void obtainDelegationTokens(DelegationTokenContainer container) throws Exception;

    /**
     * Obtains new tokens in a one-time fashion and automatically distributes them to all local JVM
     * receivers.
     */
    void obtainDelegationTokens() throws Exception;

    /**
     * Creates a re-occurring task which obtains new tokens and automatically distributes them to
     * all receivers (in local JVM as well as in registered task managers too). Task manager
     * distribution must be implemented in the listener logic in order to keep the manager logic
     * clean.
     */
    void start(Listener listener) throws Exception;

    /**
     * Stops the re-occurring token obtain task. Implementations also release any per-job provider
     * state accumulated through {@link #registerJob(JobID, Configuration)}, so stale registrations
     * cannot outlive the stop (a job that is still running re-registers through the normal
     * JobMaster registration retry).
     */
    void stop();

    /**
     * Requests an immediate, asynchronous token-obtain-and-distribute cycle, bringing the next
     * cycle forward instead of waiting for the periodic renewal. May be called from any thread. It
     * is a no-op on a manager constructed without executors (the one-shot obtain path). Concurrent
     * requests are coalesced and a configurable cooldown may apply, so a call does not necessarily
     * map to exactly one obtain.
     *
     * <p>Backs {@link
     * org.apache.flink.core.security.token.DelegationTokenManagerCallback#reobtainDelegationTokens()}.
     */
    default void reobtainDelegationTokens() {}

    /**
     * Called when a job has started. Fans the event out to all loaded {@link
     * org.apache.flink.core.security.token.DelegationTokenProvider}s. On failure of the job's first
     * registration, the job is unregistered from all providers and the exception is rethrown so the
     * caller can reject the registration. A failed re-registration rethrows but keeps the job
     * registered, so a running job's tokens are not dropped. A provider that needs the new job's
     * tokens distributed immediately requests it via {@link
     * org.apache.flink.core.security.token.DelegationTokenManagerCallback#reobtainDelegationTokens()}.
     *
     * @param jobId The job id which just started.
     * @param jobConfiguration The job's configuration.
     */
    default void registerJob(JobID jobId, Configuration jobConfiguration) throws Exception {}

    /**
     * Called when a job is being removed. Fans the event out to all loaded providers. Must be
     * idempotent. Per-provider failures are caught and logged (one provider's failure does not
     * abort cleanup of the others), so in practice this does not throw for provider failures.
     *
     * @param jobId The job id of the job.
     */
    default void unregisterJob(JobID jobId) throws Exception {}
}
