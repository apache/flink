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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.security.Credentials;

import java.util.Optional;

/**
 * Delegation token provider API. Instances of token providers are loaded by {@link
 * DelegationTokenManager} through service loader.
 */
@Experimental
public interface DelegationTokenProvider {
    /** Name of the service to provide delegation tokens. This name should be unique. */
    String serviceName();

    /**
     * Called by {@link DelegationTokenManager} to initialize provider after construction.
     *
     * @param configuration Configuration to initialize the provider.
     */
    void init(Configuration configuration) throws Exception;

    /**
     * Return whether delegation tokens are required for this service.
     *
     * @return true if delegation tokens are required.
     */
    boolean delegationTokensRequired() throws Exception;

    /**
     * Obtain delegation tokens for this service.
     *
     * @param credentials Credentials to add tokens and security keys to.
     * @return If the returned tokens are renewable and can be renewed, return the time of the next
     *     renewal, otherwise `Optional.empty()` should be returned.
     */
    Optional<Long> obtainDelegationTokens(Credentials credentials) throws Exception;
}
