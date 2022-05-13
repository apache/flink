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

package org.apache.flink.client.program.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A configuration object for {@link RestClusterClient}s. */
public final class RestClusterClientConfiguration {

    private final RestClientConfiguration restClientConfiguration;

    private final long awaitLeaderTimeout;

    private final int retryMaxAttempts;

    private final long retryDelay;

    private RestClusterClientConfiguration(
            final RestClientConfiguration endpointConfiguration,
            final long awaitLeaderTimeout,
            final int retryMaxAttempts,
            final long retryDelay) {
        checkArgument(
                awaitLeaderTimeout >= 0, "awaitLeaderTimeout must be equal to or greater than 0");
        checkArgument(retryMaxAttempts >= 0, "retryMaxAttempts must be equal to or greater than 0");
        checkArgument(retryDelay >= 0, "retryDelay must be equal to or greater than 0");

        this.restClientConfiguration = Preconditions.checkNotNull(endpointConfiguration);
        this.awaitLeaderTimeout = awaitLeaderTimeout;
        this.retryMaxAttempts = retryMaxAttempts;
        this.retryDelay = retryDelay;
    }

    public RestClientConfiguration getRestClientConfiguration() {
        return restClientConfiguration;
    }

    /** @see RestOptions#AWAIT_LEADER_TIMEOUT */
    public long getAwaitLeaderTimeout() {
        return awaitLeaderTimeout;
    }

    /** @see RestOptions#RETRY_MAX_ATTEMPTS */
    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    /** @see RestOptions#RETRY_DELAY */
    public long getRetryDelay() {
        return retryDelay;
    }

    public static RestClusterClientConfiguration fromConfiguration(Configuration config)
            throws ConfigurationException {
        RestClientConfiguration restClientConfiguration =
                RestClientConfiguration.fromConfiguration(config);

        final long awaitLeaderTimeout = config.getLong(RestOptions.AWAIT_LEADER_TIMEOUT);
        final int retryMaxAttempts = config.getInteger(RestOptions.RETRY_MAX_ATTEMPTS);
        final long retryDelay = config.getLong(RestOptions.RETRY_DELAY);

        return new RestClusterClientConfiguration(
                restClientConfiguration, awaitLeaderTimeout, retryMaxAttempts, retryDelay);
    }
}
