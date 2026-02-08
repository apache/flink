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
 * Interface for a connection with resolved secrets.
 *
 * <p>A {@link SensitiveConnection} represents a complete connection configuration with all secrets
 * resolved and available as plain values. This interface is used temporarily during connection
 * resolution, merging catalog metadata with retrieved credential material.
 *
 * <p>This structure exists only at runtime and should never be persisted to prevent credential
 * exposure in storage systems.
 */
@PublicEvolving
public interface SensitiveConnection {

    /**
     * Returns a map of string-based connection options.
     *
     * <p>These options contain the complete connection configuration including plain secrets.
     * Secret references from the {@link CatalogConnection} are replaced with their actual values
     * retrieved from external secret management systems.
     *
     * @return connection options with resolved plain secrets
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
     * Creates a basic implementation of this interface.
     *
     * @param options connection options with resolved plain secrets
     * @param comment optional comment
     */
    static SensitiveConnection of(Map<String, String> options, @Nullable String comment) {
        return new DefaultSensitiveConnection(options, comment);
    }
}
