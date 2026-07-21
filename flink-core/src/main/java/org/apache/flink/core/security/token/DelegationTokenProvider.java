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

package org.apache.flink.core.security.token;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

/**
 * Delegation token provider API. Instances of {@link DelegationTokenProvider}s are loaded by
 * DelegationTokenManager through service loader. Basically the implementation of this interface is
 * responsible to produce the serialized form of tokens which will be handled by {@link
 * DelegationTokenReceiver} instances both on JobManager and TaskManager side.
 *
 * <p><b>Threading contract.</b> A single instance per provider implementation is created and {@link
 * #init(Configuration, DelegationTokenManagerCallback) initialized} once and then shared for the
 * lifetime of the manager. {@link #obtainDelegationTokens()} usually runs on the manager's IO
 * executor, but the first cycle runs on the thread that starts the manager (the ResourceManager
 * main thread) and one-shot obtains run on the caller's thread, so implementations must not assume
 * a particular thread. {@link #registerJob(JobID, Configuration)} and {@link #unregisterJob(JobID)}
 * are invoked from the ResourceManager main thread. These can therefore run concurrently with
 * {@link #obtainDelegationTokens()}. {@link
 * DelegationTokenManagerCallback#reobtainDelegationTokens()} may be invoked from any thread.
 * Implementations must keep any per-job state thread-safe, and {@code registerJob}/{@code
 * unregisterJob} must be non-blocking so they do not stall the ResourceManager — defer real work to
 * {@link #obtainDelegationTokens()}.
 */
@Experimental
public interface DelegationTokenProvider {

    /** Config prefix of providers. */
    String CONFIG_PREFIX = "security.delegation.token.provider";

    /** Container for obtained delegation tokens. */
    class ObtainedDelegationTokens {
        /** Serialized form of delegation tokens. */
        private byte[] tokens;

        /**
         * Time until the tokens are valid, if valid forever then `Optional.empty()` should be
         * returned.
         */
        private Optional<Long> validUntil;

        public ObtainedDelegationTokens(byte[] tokens, Optional<Long> validUntil) {
            this.tokens = tokens;
            this.validUntil = validUntil;
        }

        public byte[] getTokens() {
            return tokens;
        }

        public Optional<Long> getValidUntil() {
            return validUntil;
        }
    }

    /** Name of the service to provide delegation tokens. This name should be unique. */
    String serviceName();

    /** Config prefix of the service. */
    default String serviceConfigPrefix() {
        return String.format("%s.%s", CONFIG_PREFIX, serviceName());
    }

    /**
     * Called by DelegationTokenManager to initialize provider after construction.
     *
     * @param configuration Configuration to initialize the provider.
     */
    void init(Configuration configuration) throws Exception;

    /**
     * Called by DelegationTokenManager to initialize the provider after construction, additionally
     * handing it a {@link DelegationTokenManagerCallback} it can use to request a token re-obtain.
     *
     * <p>This is the entry point the manager actually calls. The default implementation ignores the
     * callback and delegates to {@link #init(Configuration)}, so providers that do not need to
     * trigger re-obtains keep implementing only {@link #init(Configuration)}. A provider that wants
     * to request re-obtains overrides this method, retains the callback, and invokes {@link
     * DelegationTokenManagerCallback#reobtainDelegationTokens()} when needed.
     *
     * @param configuration Configuration to initialize the provider.
     * @param callback Used to ask the manager to re-obtain tokens. May be retained and called
     *     later.
     */
    default void init(Configuration configuration, DelegationTokenManagerCallback callback)
            throws Exception {
        init(configuration);
    }

    /**
     * Return whether delegation tokens are required for this service.
     *
     * @return true if delegation tokens are required.
     */
    boolean delegationTokensRequired() throws Exception;

    /**
     * Obtain delegation tokens for this service.
     *
     * @return the obtained delegation tokens.
     */
    ObtainedDelegationTokens obtainDelegationTokens() throws Exception;

    /**
     * Called when a job has started, before its tasks are scheduled, with its configuration.
     *
     * <p>To get the job's tokens distributed without waiting for the periodic renewal, call {@link
     * DelegationTokenManagerCallback#reobtainDelegationTokens()} on the callback handed to {@link
     * #init(Configuration, DelegationTokenManagerCallback)} to request an immediate obtain cycle.
     *
     * <p>A provider that requests a re-obtain must record this job's per-job state <em>before</em>
     * invoking {@link DelegationTokenManagerCallback#reobtainDelegationTokens()}. That call merely
     * schedules (or coalesces into) an obtain cycle that runs later on another thread. Recording
     * first establishes the happens-before that lets the serving cycle observe this job's state.
     * Recording afterwards races with the cycle and the job's tokens may be skipped until the next
     * periodic renewal.
     *
     * <p>Must be idempotent: it may be called more than once for the same {@code jobId} (e.g. on
     * JobManager or ResourceManager failover, when the JobMaster re-registers).
     *
     * <p>Should not throw: a thrown (unchecked) exception or linkage error rejects the job's
     * registration (the job does not start) and triggers {@link #unregisterJob(JobID)} on all
     * providers to roll back. Prefer deferring the real fetch to the (retrying) obtain cycle over a
     * synchronous fetch, so a transient failure does not fail the job.
     *
     * @param jobId The job id of the job.
     * @param jobConfiguration The job configuration.
     */
    default void registerJob(JobID jobId, Configuration jobConfiguration) {}

    /**
     * Called when the job is being removed — it reached a globally terminal state, or its
     * job-leader registration timed out — and its per-job state should be released. Must be
     * idempotent. Exceptions and linkage errors are caught and logged by the framework (one
     * provider's failure does not abort cleanup of the others), but implementations should still
     * avoid throwing.
     *
     * @param jobId The job id of the job.
     */
    default void unregisterJob(JobID jobId) {}

    /**
     * Stops the provider. Any resources should be closed.
     *
     * <p>Called once during manager shutdown. Note that an obtain-and-broadcast cycle started just
     * before shutdown may still be running on another thread when this is invoked, so {@code
     * stop()} may overlap an in-flight {@link #obtainDelegationTokens()}; implementations must
     * release resources in a way that is safe with respect to that overlap.
     */
    default void stop() {}
}
