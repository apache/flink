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

package org.apache.flink.table.gateway.api.session;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.module.Module;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Environment to initialize the {@code Session}. */
@PublicEvolving
public class SessionEnvironment {
    private final @Nullable String sessionName;
    private final EndpointVersion version;
    private final Map<String, Catalog> registeredCatalogs;
    private final Map<String, Module> registeredModules;
    private final @Nullable String defaultCatalog;
    private final @Nullable String defaultDatabase;
    private final Map<String, String> sessionConfig;

    @VisibleForTesting
    SessionEnvironment(
            @Nullable String sessionName,
            EndpointVersion version,
            Map<String, Catalog> registeredCatalogs,
            Map<String, Module> registeredModules,
            @Nullable String defaultCatalog,
            @Nullable String defaultDatabase,
            Map<String, String> sessionConfig) {
        this.sessionName = sessionName;
        this.version = version;
        this.registeredCatalogs = registeredCatalogs;
        this.registeredModules = registeredModules;
        this.defaultCatalog = defaultCatalog;
        this.defaultDatabase = defaultDatabase;
        this.sessionConfig = sessionConfig;
    }

    // -------------------------------------------------------------------------------------------
    // Getter
    // -------------------------------------------------------------------------------------------

    public Optional<String> getSessionName() {
        return Optional.ofNullable(sessionName);
    }

    public EndpointVersion getSessionEndpointVersion() {
        return version;
    }

    public Map<String, String> getSessionConfig() {
        return Collections.unmodifiableMap(sessionConfig);
    }

    public Map<String, Catalog> getRegisteredCatalogs() {
        return Collections.unmodifiableMap(registeredCatalogs);
    }

    public Map<String, Module> getRegisteredModules() {
        return Collections.unmodifiableMap(registeredModules);
    }

    public Optional<String> getDefaultCatalog() {
        return Optional.ofNullable(defaultCatalog);
    }

    public Optional<String> getDefaultDatabase() {
        return Optional.ofNullable(defaultDatabase);
    }

    // -------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SessionEnvironment)) {
            return false;
        }
        SessionEnvironment that = (SessionEnvironment) o;
        return Objects.equals(sessionName, that.sessionName)
                && Objects.equals(version, that.version)
                && Objects.equals(registeredCatalogs, that.registeredCatalogs)
                && Objects.equals(registeredModules, that.registeredModules)
                && Objects.equals(defaultCatalog, that.defaultCatalog)
                && Objects.equals(defaultDatabase, that.defaultDatabase)
                && Objects.equals(sessionConfig, that.sessionConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sessionName,
                version,
                registeredCatalogs,
                registeredModules,
                defaultCatalog,
                defaultDatabase,
                sessionConfig);
    }

    // -------------------------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------------------------

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder to build the {@link SessionEnvironment}. */
    @PublicEvolving
    public static class Builder {
        private @Nullable String sessionName;
        private EndpointVersion version;
        private final Map<String, String> sessionConfig = new HashMap<>();
        private final Map<String, Catalog> registeredCatalogs = new HashMap<>();
        private final Map<String, Module> registeredModules = new HashMap<>();
        private @Nullable String defaultCatalog;
        private @Nullable String defaultDatabase;

        public Builder setSessionName(String sessionName) {
            this.sessionName = sessionName;
            return this;
        }

        public Builder setSessionEndpointVersion(EndpointVersion version) {
            this.version = version;
            return this;
        }

        public Builder addSessionConfig(Map<String, String> sessionConfig) {
            this.sessionConfig.putAll(sessionConfig);
            return this;
        }

        public Builder setDefaultCatalog(@Nullable String defaultCatalog) {
            this.defaultCatalog = defaultCatalog;
            return this;
        }

        public Builder setDefaultDatabase(@Nullable String defaultDatabase) {
            this.defaultDatabase = defaultDatabase;
            return this;
        }

        public Builder registerCatalog(String catalogName, Catalog catalog) {
            if (registeredCatalogs.containsKey(catalogName)) {
                throw new ValidationException(
                        String.format("A catalog with name '%s' already exists.", catalogName));
            }
            this.registeredCatalogs.put(catalogName, catalog);
            return this;
        }

        public Builder registerModuleAtHead(String moduleName, Module module) {
            if (registeredModules.containsKey(moduleName)) {
                throw new ValidationException(
                        String.format("A module with name '%s' already exists", moduleName));
            }

            this.registeredModules.put(moduleName, module);
            return this;
        }

        public SessionEnvironment build() {
            return new SessionEnvironment(
                    sessionName,
                    checkNotNull(version),
                    registeredCatalogs,
                    registeredModules,
                    defaultCatalog,
                    defaultDatabase,
                    sessionConfig);
        }
    }
}
