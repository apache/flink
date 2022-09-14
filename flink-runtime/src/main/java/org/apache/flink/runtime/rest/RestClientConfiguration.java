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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A configuration object for {@link RestClient}s. */
public final class RestClientConfiguration {

    @Nullable private final SSLHandlerFactory sslHandlerFactory;

    private final long connectionTimeout;

    private final long idlenessTimeout;

    private final int maxContentLength;

    private RestClientConfiguration(
            @Nullable final SSLHandlerFactory sslHandlerFactory,
            final long connectionTimeout,
            final long idlenessTimeout,
            final int maxContentLength) {
        checkArgument(
                maxContentLength > 0,
                "maxContentLength must be positive, was: %s",
                maxContentLength);
        this.sslHandlerFactory = sslHandlerFactory;
        this.connectionTimeout = connectionTimeout;
        this.idlenessTimeout = idlenessTimeout;
        this.maxContentLength = maxContentLength;
    }

    /**
     * Returns the {@link SSLEngine} that the REST client endpoint should use.
     *
     * @return SSLEngine that the REST client endpoint should use, or null if SSL was disabled
     */
    @Nullable
    public SSLHandlerFactory getSslHandlerFactory() {
        return sslHandlerFactory;
    }

    /** {@see RestOptions#CONNECTION_TIMEOUT}. */
    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    /** {@see RestOptions#IDLENESS_TIMEOUT}. */
    public long getIdlenessTimeout() {
        return idlenessTimeout;
    }

    /**
     * Returns the max content length that the REST client endpoint could handle.
     *
     * @return max content length that the REST client endpoint could handle
     */
    public int getMaxContentLength() {
        return maxContentLength;
    }

    /**
     * Creates and returns a new {@link RestClientConfiguration} from the given {@link
     * Configuration}.
     *
     * @param config configuration from which the REST client endpoint configuration should be
     *     created from
     * @return REST client endpoint configuration
     * @throws ConfigurationException if SSL was configured incorrectly
     */
    public static RestClientConfiguration fromConfiguration(Configuration config)
            throws ConfigurationException {
        Preconditions.checkNotNull(config);

        final SSLHandlerFactory sslHandlerFactory;
        if (SecurityOptions.isRestSSLEnabled(config)) {
            try {
                sslHandlerFactory = SSLUtils.createRestClientSSLEngineFactory(config);
            } catch (Exception e) {
                throw new ConfigurationException(
                        "Failed to initialize SSLContext for the REST client", e);
            }
        } else {
            sslHandlerFactory = null;
        }

        final long connectionTimeout = config.getLong(RestOptions.CONNECTION_TIMEOUT);

        final long idlenessTimeout = config.getLong(RestOptions.IDLENESS_TIMEOUT);

        int maxContentLength = config.getInteger(RestOptions.CLIENT_MAX_CONTENT_LENGTH);

        return new RestClientConfiguration(
                sslHandlerFactory, connectionTimeout, idlenessTimeout, maxContentLength);
    }
}
