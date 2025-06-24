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
import org.apache.flink.configuration.ReadableConfig;
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
    private final Map<String, CatalogCreator> registeredCatalogCreators;
    private final Map<String, ModuleCreator> registeredModuleCreators;
    private final @Nullable String defaultCatalog;
    private final Map<String, String> sessionConfig;

    @VisibleForTesting
    SessionEnvironment(
            @Nullable String sessionName,
            EndpointVersion version,
            Map<String, CatalogCreator> registeredCatalogCreators,
            Map<String, ModuleCreator> registeredModuleCreators,
            @Nullable String defaultCatalog,
            Map<String, String> sessionConfig) {
        this.sessionName = sessionName;
        this.version = version;
        this.registeredCatalogCreators = registeredCatalogCreators;
        this.registeredModuleCreators = registeredModuleCreators;
        this.defaultCatalog = defaultCatalog;
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

    public Map<String, CatalogCreator> getRegisteredCatalogCreators() {
        return Collections.unmodifiableMap(registeredCatalogCreators);
    }

    public Map<String, ModuleCreator> getRegisteredModuleCreators() {
        return Collections.unmodifiableMap(registeredModuleCreators);
    }

    public Optional<String> getDefaultCatalog() {
        return Optional.ofNullable(defaultCatalog);
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
                && Objects.equals(registeredCatalogCreators, that.registeredCatalogCreators)
                && Objects.equals(registeredModuleCreators, that.registeredModuleCreators)
                && Objects.equals(defaultCatalog, that.defaultCatalog)
                && Objects.equals(sessionConfig, that.sessionConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sessionName,
                version,
                registeredCatalogCreators,
                registeredModuleCreators,
                defaultCatalog,
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
        private final Map<String, CatalogCreator> registeredCatalogCreators = new HashMap<>();
        private final Map<String, ModuleCreator> registeredModuleCreators = new HashMap<>();
        private @Nullable String defaultCatalog;

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

        public Builder registerCatalog(String catalogName, Catalog catalog) {
            if (registeredCatalogCreators.containsKey(catalogName)) {
                throw new ValidationException(
                        String.format("A catalog with name '%s' already exists.", catalogName));
            }
            this.registeredCatalogCreators.put(
                    catalogName, (configuration, classLoader) -> catalog);
            return this;
        }

        public Builder registerCatalogCreator(String catalogName, CatalogCreator catalogCreator) {
            if (registeredCatalogCreators.containsKey(catalogName)) {
                throw new ValidationException(
                        String.format("A catalog with name '%s' already exists.", catalogName));
            }
            this.registeredCatalogCreators.put(catalogName, catalogCreator);
            return this;
        }

        public Builder registerModuleAtHead(String moduleName, Module module) {
            if (registeredModuleCreators.containsKey(moduleName)) {
                throw new ValidationException(
                        String.format("A module with name '%s' already exists", moduleName));
            }

            this.registeredModuleCreators.put(moduleName, (configuration, classLoader) -> module);
            return this;
        }

        public Builder registerModuleCreatorAtHead(String moduleName, ModuleCreator moduleCreator) {
            if (registeredModuleCreators.containsKey(moduleName)) {
                throw new ValidationException(
                        String.format("A module with name '%s' already exists", moduleName));
            }

            this.registeredModuleCreators.put(moduleName, moduleCreator);
            return this;
        }

        public SessionEnvironment build() {
            return new SessionEnvironment(
                    sessionName,
                    checkNotNull(version),
                    registeredCatalogCreators,
                    registeredModuleCreators,
                    defaultCatalog,
                    sessionConfig);
        }
    }

    /** An interface used to create {@link Module}. */
    @PublicEvolving
    public interface ModuleCreator {

        /**
         * @param configuration The read-only configuration with which the module is created.
         * @param classLoader The class loader with which the module is created.
         * @return The created module object.
         */
        Module create(ReadableConfig configuration, ClassLoader classLoader);
    }

    /** An interface used to create {@link Catalog}. */
    @PublicEvolving
    public interface CatalogCreator {

        /**
         * @param configuration The read-only configuration with which the catalog is created.
         * @param classLoader The class loader with which the catalog is created.
         * @return The created catalog object.
         */
        Catalog create(ReadableConfig configuration, ClassLoader classLoader);
    }
}
