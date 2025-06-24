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

/**
 * Delegation token receiver API. Instances of {@link DelegationTokenReceiver}s are loaded both on
 * JobManager and TaskManager side through service loader. Basically the implementation of this
 * interface is responsible to receive the serialized form of tokens produced by {@link
 * DelegationTokenProvider}.
 */
@Experimental
public interface DelegationTokenReceiver {

    /** Config prefix of receivers. */
    String CONFIG_PREFIX = "security.delegation.token.receiver";

    /**
     * Name of the service to receive delegation tokens for. This name should be unique and the same
     * as the one provided in the corresponding {@link DelegationTokenProvider}.
     */
    String serviceName();

    /** Config prefix of the service. */
    default String serviceConfigPrefix() {
        return String.format("%s.%s", CONFIG_PREFIX, serviceName());
    }

    /**
     * Called to initialize receiver after construction.
     *
     * @param configuration Configuration to initialize the receiver.
     */
    void init(Configuration configuration) throws Exception;

    /**
     * Callback function when new delegation tokens obtained.
     *
     * @param tokens Serialized form of delegation tokens. Must be deserialized the reverse way
     *     which is implemented in {@link DelegationTokenProvider}.
     */
    void onNewTokensObtained(byte[] tokens) throws Exception;
}
