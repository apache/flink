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

package org.apache.flink.runtime.registration;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Configuration for the cluster components. */
public class RetryingRegistrationConfiguration {

    private final long initialRegistrationTimeoutMillis;

    private final long maxRegistrationTimeoutMillis;

    private final long errorDelayMillis;

    private final long refusedDelayMillis;

    public RetryingRegistrationConfiguration(
            long initialRegistrationTimeoutMillis,
            long maxRegistrationTimeoutMillis,
            long errorDelayMillis,
            long refusedDelayMillis) {
        checkArgument(
                initialRegistrationTimeoutMillis > 0,
                "initial registration timeout must be greater than zero");
        checkArgument(
                maxRegistrationTimeoutMillis > 0,
                "maximum registration timeout must be greater than zero");
        checkArgument(errorDelayMillis >= 0, "delay on error must be non-negative");
        checkArgument(
                refusedDelayMillis >= 0, "delay on refused registration must be non-negative");

        this.initialRegistrationTimeoutMillis = initialRegistrationTimeoutMillis;
        this.maxRegistrationTimeoutMillis = maxRegistrationTimeoutMillis;
        this.errorDelayMillis = errorDelayMillis;
        this.refusedDelayMillis = refusedDelayMillis;
    }

    public long getInitialRegistrationTimeoutMillis() {
        return initialRegistrationTimeoutMillis;
    }

    public long getMaxRegistrationTimeoutMillis() {
        return maxRegistrationTimeoutMillis;
    }

    public long getErrorDelayMillis() {
        return errorDelayMillis;
    }

    public long getRefusedDelayMillis() {
        return refusedDelayMillis;
    }

    public static RetryingRegistrationConfiguration fromConfiguration(
            final Configuration configuration) {
        long initialRegistrationTimeoutMillis =
                configuration.getLong(ClusterOptions.INITIAL_REGISTRATION_TIMEOUT);
        long maxRegistrationTimeoutMillis =
                configuration.getLong(ClusterOptions.MAX_REGISTRATION_TIMEOUT);
        long errorDelayMillis = configuration.getLong(ClusterOptions.ERROR_REGISTRATION_DELAY);
        long refusedDelayMillis = configuration.getLong(ClusterOptions.REFUSED_REGISTRATION_DELAY);

        return new RetryingRegistrationConfiguration(
                initialRegistrationTimeoutMillis,
                maxRegistrationTimeoutMillis,
                errorDelayMillis,
                refusedDelayMillis);
    }

    public static RetryingRegistrationConfiguration defaultConfiguration() {
        return fromConfiguration(new Configuration());
    }
}
