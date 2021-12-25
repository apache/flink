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

import org.apache.flink.annotation.PublicEvolving;

/**
 * A factory to create a specific {@link JdbcDialect}. This factory is used with Java's Service
 * Provider Interfaces (SPI) for discovering.
 *
 * <p>Classes that implement this interface can be added to the
 * "META_INF/services/org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory" file of a JAR file
 * in the current classpath to be found.
 *
 * @see JdbcDialect
 */
@PublicEvolving
public interface JdbcDialectFactory {

    /**
     * Retrieves whether the dialect thinks that it can open a connection to the given URL.
     * Typically, dialects will return <code>true</code> if they understand the sub-protocol
     * specified in the URL and <code>false</code> if they do not.
     *
     * @param url the URL of the database
     * @return <code>true</code> if this dialect understands the given URL; <code>false</code>
     *     otherwise.
     */
    boolean acceptsURL(String url);

    /** @return Creates a new instance of the {@link JdbcDialect}. */
    JdbcDialect create();
}
