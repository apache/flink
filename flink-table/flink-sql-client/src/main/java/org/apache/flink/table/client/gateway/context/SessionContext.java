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

package org.apache.flink.table.client.gateway.context;

import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Collections;

/**
 * Context describing a session, it's mainly used for user to open a new session in the backend. If
 * client request to open a new session, the backend {@link Executor} will maintain the session
 * context map util users close it.
 */
public class SessionContext {

    private static final Logger LOG = LoggerFactory.getLogger(SessionContext.class);

    private final String sessionId;
    private final DefaultContext defaultContext;

    // store the value from the YAML and used to build TableEnvironment.
    private Environment sessionEnv;
    // store all options and use Configuration to build SessionState and TableConfig.
    private final Configuration sessionConfiguration;

    private final SessionState sessionState;
    private final URLClassLoader classLoader;
    private ExecutionContext executionContext;

    private SessionContext(
            DefaultContext defaultContext,
            String sessionId,
            Environment sessionEnv,
            Configuration sessionConfiguration,
            URLClassLoader classLoader,
            SessionState sessionState,
            ExecutionContext executionContext) {
        this.defaultContext = defaultContext;
        this.sessionId = sessionId;
        this.sessionEnv = sessionEnv;
        this.sessionConfiguration = sessionConfiguration;
        this.classLoader = classLoader;
        this.sessionState = sessionState;
        this.executionContext = executionContext;
    }

    // --------------------------------------------------------------------------------------------
    // Getter method
    // --------------------------------------------------------------------------------------------

    public String getSessionId() {
        return this.sessionId;
    }

    public Environment getSessionEnvironment() {
        return this.sessionEnv;
    }

    public ExecutionContext getExecutionContext() {
        return this.executionContext;
    }

    // --------------------------------------------------------------------------------------------
    // Method to execute commands
    // --------------------------------------------------------------------------------------------

    /**
     * Reset properties to default. It will rebuild a new {@link ExecutionContext}.
     *
     * <p>Reset runtime configurations specific to the current session which were set via the SET
     * command to their default values.
     */
    public void reset() {
        sessionEnv = defaultContext.getDefaultEnv().clone();
        // SessionState is built from the sessionConfiguration.
        // If rebuild a new Configuration, it loses control of the SessionState if users wants to
        // modify the configuration
        for (String key : sessionConfiguration.toMap().keySet()) {
            // Don't care the type of the option
            ConfigOption<String> keyToDelete = ConfigOptions.key(key).stringType().noDefaultValue();
            sessionConfiguration.removeConfig(keyToDelete);
        }
        sessionConfiguration.addAll(defaultContext.getFlinkConfig());
        executionContext = new ExecutionContext(sessionEnv, executionContext);
    }

    /** Set properties. It will rebuild a new {@link ExecutionContext} */
    public void set(String key, String value) {
        // put key-value into the Environment
        try {
            this.sessionEnv = Environment.enrich(sessionEnv, Collections.singletonMap(key, value));
            sessionConfiguration.setString(key, value);
        } catch (Throwable t) {
            throw new SqlExecutionException("Could not set session property.", t);
        }

        // Renew the ExecutionContext by new environment.
        // Book keep all the session states of current ExecutionContext then
        // re-register them into the new one.
        this.executionContext = new ExecutionContext(sessionEnv, executionContext);
    }

    /** Close resources, e.g. catalogs. */
    public void close() {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            for (String name : sessionState.catalogManager.listCatalogs()) {
                sessionState.catalogManager.getCatalog(name).ifPresent(Catalog::close);
            }
        }
        try {
            classLoader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing class loader.", e);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper method to create
    // --------------------------------------------------------------------------------------------

    public static SessionContext create(DefaultContext defaultContext, String sessionId) {
        // --------------------------------------------------------------------------------------------------------------
        // Init config
        // --------------------------------------------------------------------------------------------------------------

        Environment sessionEnv = defaultContext.getDefaultEnv().clone();
        Configuration configuration = defaultContext.getFlinkConfig().clone();

        // --------------------------------------------------------------------------------------------------------------
        // Init classloader
        // --------------------------------------------------------------------------------------------------------------

        URLClassLoader classLoader =
                ClientUtils.buildUserCodeClassLoader(
                        defaultContext.getDependencies(),
                        Collections.emptyList(),
                        SessionContext.class.getClassLoader(),
                        configuration);

        // --------------------------------------------------------------------------------------------------------------
        // Init session state
        // --------------------------------------------------------------------------------------------------------------

        ModuleManager moduleManager = new ModuleManager();

        final EnvironmentSettings settings = sessionEnv.getExecution().getEnvironmentSettings();

        CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(classLoader)
                        .config(configuration)
                        .defaultCatalog(
                                settings.getBuiltInCatalogName(),
                                new GenericInMemoryCatalog(
                                        settings.getBuiltInCatalogName(),
                                        settings.getBuiltInDatabaseName()))
                        .build();

        FunctionCatalog functionCatalog =
                new FunctionCatalog(configuration, catalogManager, moduleManager);
        SessionState sessionState =
                new SessionState(catalogManager, moduleManager, functionCatalog);

        // --------------------------------------------------------------------------------------------------------------
        // Init ExecutionContext
        // --------------------------------------------------------------------------------------------------------------

        ExecutionContext executionContext =
                new ExecutionContext(sessionEnv, configuration, classLoader, sessionState);
        LegacyTableEnvironmentInitializer.initializeSessionState(
                executionContext.getTableEnvironment(), sessionEnv, classLoader);

        return new SessionContext(
                defaultContext,
                sessionId,
                sessionEnv,
                configuration,
                classLoader,
                sessionState,
                executionContext);
    }

    // --------------------------------------------------------------------------------------------
    // Inner class
    // --------------------------------------------------------------------------------------------

    /** session state. */
    public static class SessionState {

        public final CatalogManager catalogManager;
        public final FunctionCatalog functionCatalog;
        public final ModuleManager moduleManager;

        public SessionState(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                FunctionCatalog functionCatalog) {
            this.catalogManager = catalogManager;
            this.moduleManager = moduleManager;
            this.functionCatalog = functionCatalog;
        }
    }
}
