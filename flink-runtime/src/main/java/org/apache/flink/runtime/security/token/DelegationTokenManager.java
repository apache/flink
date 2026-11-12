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
     * Starts obtaining tokens for the lifetime of the process: obtains tokens inline, propagates
     * them to all local JVM receivers and schedules the next obtain. Fails if the first obtain
     * fails.
     *
     * <p>Tokens are obtained independently of any leadership, so a standby JobManager keeps its
     * local receivers fresh too. Distribution to other processes is attached per leader session
     * with {@link #attachListener(Listener)}.
     */
    void start() throws Exception;

    /**
     * Attaches the listener which distributes tokens to the rest of the cluster (task managers) for
     * the duration of a leader session. The most recently obtained tokens are handed to the
     * listener before this method returns, without contacting the token services. Tokens obtained
     * later are delivered to the listener as well, never older tokens after newer ones.
     */
    void attachListener(Listener listener) throws Exception;

    /**
     * Detaches the listener at the end of a leader session. Has no effect if the listener is not
     * the attached one. A callback to the listener which is in progress is waited for, so none runs
     * after this method returns. Obtaining tokens, and with it the update of the local JVM
     * receivers, continues.
     */
    void detachListener(Listener listener);

    /** Stops obtaining tokens and releases the attached listener. */
    void stop();
}
