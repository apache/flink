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
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

/**
 * Delegation token provider API. Instances of {@link DelegationTokenProvider}s are loaded by
 * DelegationTokenManager through service loader. Basically the implementation of this interface is
 * responsible to produce the serialized form of tokens which will be handled by {@link
 * DelegationTokenReceiver} instances both on JobManager and TaskManager side.
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
}
