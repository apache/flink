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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Interface for a connection stored in a catalog.
 *
 * <p>A {@link CatalogConnection} contains non-sensitive connection configuration that can be safely
 * persisted in the catalog. Sensitive credentials are extracted and replaced with identifier
 * references to external secret management systems. See {@link SensitiveConnection} for connection
 * containing sensitive information.
 */
@PublicEvolving
public interface CatalogConnection {

    /**
     * Returns a map of string-based connection options.
     *
     * <p>These options contain non-sensitive configuration. Any sensitive values (passwords,
     * tokens, etc.) are replaced with references to external secret stores.
     *
     * @return connection options without plain secrets
     */
    Map<String, String> getOptions();

    /**
     * Get comment of the connection.
     *
     * @return comment of the connection
     */
    @Nullable
    String getComment();

    /**
     * Get a deep copy of the CatalogConnection instance.
     *
     * @return a copy of the CatalogConnection instance
     */
    CatalogConnection copy();

    /**
     * Creates a basic implementation of this interface.
     *
     * @param options connection options without plain secrets
     * @param comment optional comment
     */
    static CatalogConnection of(Map<String, String> options, @Nullable String comment) {
        return new DefaultCatalogConnection(options, comment);
    }
}
