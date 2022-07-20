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

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Utility for working with {@link JdbcDialect}. */
@Internal
public final class JdbcDialectLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialectLoader.class);

    private JdbcDialectLoader() {}

    /**
     * Loads the unique JDBC Dialect that can handle the given database url.
     *
     * @param url A database URL.
     * @throws IllegalStateException if the loader cannot find exactly one dialect that can
     *     unambiguously process the given database URL.
     * @return The loaded dialect.
     */
    public static JdbcDialect load(String url) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<JdbcDialectFactory> foundFactories = discoverFactories(cl);

        if (foundFactories.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any jdbc dialect factories that implement '%s' in the classpath.",
                            JdbcDialectFactory.class.getName()));
        }

        final List<JdbcDialectFactory> matchingFactories =
                foundFactories.stream().filter(f -> f.acceptsURL(url)).collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any jdbc dialect factory that can handle url '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factories are:\n\n"
                                    + "%s",
                            url,
                            JdbcDialectFactory.class.getName(),
                            foundFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple jdbc dialect factories can handle url '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            url,
                            JdbcDialectFactory.class.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingFactories.get(0).create();
    }

    private static List<JdbcDialectFactory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<JdbcDialectFactory> result = new LinkedList<>();
            ServiceLoader.load(JdbcDialectFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for jdbc dialects factory.", e);
            throw new RuntimeException(
                    "Could not load service provider for jdbc dialects factory.", e);
        }
    }
}
