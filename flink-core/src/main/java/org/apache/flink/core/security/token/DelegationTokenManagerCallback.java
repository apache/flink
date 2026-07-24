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

/**
 * Handed to a {@link DelegationTokenProvider} at {@link
 * DelegationTokenProvider#init(org.apache.flink.configuration.Configuration,
 * DelegationTokenManagerCallback) init} time, giving the provider a way to ask the delegation token
 * manager to re-obtain tokens. The provider may retain the callback and invoke it later, outside
 * the {@code init}/{@code registerJob} call stack.
 */
@Experimental
public interface DelegationTokenManagerCallback {

    /**
     * Requests an asynchronous token re-obtain and redistribution to all receivers, bringing the
     * next obtain cycle forward instead of waiting for the periodic renewal.
     *
     * <p>May be called from any thread at any time after {@code init}. The manager coalesces
     * requests and may apply a cooldown, so a call does not necessarily map to one obtain cycle.
     * Returns immediately and does not wait for completion.
     */
    void reobtainDelegationTokens();
}
