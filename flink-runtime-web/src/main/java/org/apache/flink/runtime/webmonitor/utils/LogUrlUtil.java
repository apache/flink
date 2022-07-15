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

package org.apache.flink.runtime.webmonitor.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** Util for log url pattern. */
public class LogUrlUtil {

    private static final String SCHEME_SEPARATOR = "://";
    private static final String HTTP_SCHEME = "http";
    private static final String HTTPS_SCHEME = "https";

    private static final Logger LOG = LoggerFactory.getLogger(LogUrlUtil.class);

    /** Validate and normalize log url pattern. */
    public static Optional<String> getValidLogUrlPattern(
            final Configuration config, final ConfigOption<String> option) {
        String pattern = config.getString(option);
        if (StringUtils.isNullOrWhitespaceOnly(pattern)) {
            return Optional.empty();
        }

        pattern = pattern.trim();

        String scheme = pattern.substring(0, Math.max(pattern.indexOf(SCHEME_SEPARATOR), 0));
        if (scheme.isEmpty()) {
            return Optional.of(HTTP_SCHEME + SCHEME_SEPARATOR + pattern);
        } else if (HTTP_SCHEME.equalsIgnoreCase(scheme) || HTTPS_SCHEME.equalsIgnoreCase(scheme)) {
            return Optional.of(pattern);
        } else {
            LOG.warn(
                    "Ignore configured value for '{}': unsupported scheme {}",
                    option.key(),
                    scheme);
            return Optional.empty();
        }
    }

    private LogUrlUtil() {}
}
