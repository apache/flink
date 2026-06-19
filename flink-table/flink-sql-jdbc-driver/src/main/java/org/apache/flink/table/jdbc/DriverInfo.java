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

package org.apache.flink.table.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static org.apache.flink.table.jdbc.utils.DriverUtils.checkArgument;
import static org.apache.flink.table.jdbc.utils.DriverUtils.checkNotNull;
import static org.apache.flink.table.jdbc.utils.DriverUtils.isNullOrWhitespaceOnly;

/**
 * Driver info for flink driver, it reads driver name and version from driver.properties which will
 * be updated by flink version.
 */
final class DriverInfo {
    private static final Logger LOG = LoggerFactory.getLogger(DriverInfo.class);

    private static final String DRIVER_NAME_OPTION = "flink.driver.name";
    private static final String DRIVER_VERSION_OPTION = "flink.driver.version";
    static final String DRIVER_NAME;
    static final String DRIVER_VERSION;
    static final int DRIVER_VERSION_MAJOR;
    static final int DRIVER_VERSION_MINOR;

    static {
        try {
            Properties properties = new Properties();
            URL url = DriverInfo.class.getResource("driver.properties");
            try (InputStream in = checkNotNull(url, "Cannot find driver.properties").openStream()) {
                properties.load(in);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            DRIVER_NAME =
                    checkNotNull(
                            properties.getProperty(DRIVER_NAME_OPTION),
                            String.format("%s is null or empty", DRIVER_NAME_OPTION));
            DRIVER_VERSION =
                    checkNotNull(
                            properties.getProperty(DRIVER_VERSION_OPTION),
                            String.format("%s is null or empty", DRIVER_VERSION_OPTION));

            Matcher matcher =
                    Pattern.compile("^(\\d+)(\\.(\\d+))($|[.-])?").matcher(DRIVER_VERSION);
            checkArgument(
                    matcher.find(),
                    String.format("%s is invalid: %s", DRIVER_VERSION_OPTION, DRIVER_VERSION));

            DRIVER_VERSION_MAJOR = parseInt(matcher.group(1));
            final String minor = matcher.group(3);
            DRIVER_VERSION_MINOR = parseInt(isNullOrWhitespaceOnly(minor) ? "0" : minor);
        } catch (RuntimeException e) {
            LOG.error("Failed to load flink driver info", e);
            throw e;
        }
    }

    private DriverInfo() {}
}
