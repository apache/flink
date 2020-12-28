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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.runtime.net.SSLUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

/** Utility class for the HistoryServer. */
public enum HistoryServerUtils {
    ;

    private static final Logger LOG = LoggerFactory.getLogger(HistoryServerUtils.class);

    public static boolean isSSLEnabled(Configuration config) {
        return config.getBoolean(HistoryServerOptions.HISTORY_SERVER_WEB_SSL_ENABLED)
                && SSLUtils.isRestSSLEnabled(config);
    }

    public static Optional<URL> getHistoryServerURL(Configuration configuration) {
        final String hostname = getHostname(configuration);

        if (hostname != null) {
            final String protocol = getProtocol(configuration);
            final int port = getPort(configuration);

            try {
                return Optional.of(new URL(protocol, hostname, port, ""));
            } catch (MalformedURLException e) {
                LOG.debug(
                        "Could not create the HistoryServer's URL from protocol: {}, hostname: {} and port: {}.",
                        protocol,
                        hostname,
                        port,
                        e);
                return Optional.empty();
            }
        } else {
            LOG.debug(
                    "Not hostname has been specified for the HistoryServer. This indicates that it has not been started.");
            return Optional.empty();
        }
    }

    private static int getPort(Configuration configuration) {
        return configuration.getInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT);
    }

    @Nullable
    private static String getHostname(Configuration configuration) {
        return configuration.getString(HistoryServerOptions.HISTORY_SERVER_WEB_ADDRESS);
    }

    private static String getProtocol(Configuration configuration) {
        if (isSSLEnabled(configuration)) {
            return "https";
        } else {
            return "http";
        }
    }
}
