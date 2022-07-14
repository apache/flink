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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.resource.ClientResourceManager;
import org.apache.flink.table.client.util.ClientClassloaderUtil;
import org.apache.flink.table.client.util.ClientWrapperClassLoader;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;
import java.util.Set;

/**
 * Context describing a session, it's mainly used for user to open a new session in the backend. If
 * client request to open a new session, the backend {@link Executor} will maintain the session
 * context map util users close it.
 */
public class SessionContext {

    private static final Logger LOG = LoggerFactory.getLogger(SessionContext.class);

    private final String sessionId;
    private final DefaultContext defaultContext;

    // store all options and use Configuration to build SessionState and TableConfig.
    private final Configuration sessionConfiguration;

    private final SessionState sessionState;
    private final ClientWrapperClassLoader classLoader;
    private ExecutionContext executionContext;

    private SessionContext(
            DefaultContext defaultContext,
            String sessionId,
            Configuration sessionConfiguration,
            ClientWrapperClassLoader classLoader,
            SessionState sessionState,
            ExecutionContext executionContext) {
        this.defaultContext = defaultContext;
        this.sessionId = sessionId;
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

    public ExecutionContext getExecutionContext() {
        return this.executionContext;
    }

    public ReadableConfig getReadableConfig() {
        return sessionConfiguration;
    }

    public Map<String, String> getConfigMap() {
        return sessionConfiguration.toMap();
    }

    @VisibleForTesting
    Set<URL> getDependencies() {
        return sessionState.resourceManager.getLocalJarResources();
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
        // SessionState is built from the sessionConfiguration.
        // If rebuild a new Configuration, it loses control of the SessionState if users wants to
        // modify the configuration
        resetSessionConfigurationToDefault(defaultContext.getFlinkConfig());
        executionContext = new ExecutionContext(sessionConfiguration, classLoader, sessionState);
    }

    /**
     * Reset key's property to default. It will rebuild a new {@link ExecutionContext}.
     *
     * <p>If key is not defined in default config file, remove it from session configuration.
     */
    public void reset(String key) {
        Configuration configuration = defaultContext.getFlinkConfig();
        // If the key exist in default yaml , reset to default
        if (configuration.containsKey(key)) {
            String defaultValue =
                    configuration.get(ConfigOptions.key(key).stringType().noDefaultValue());
            this.set(key, defaultValue);
        } else {
            ConfigOption<String> keyToDelete = ConfigOptions.key(key).stringType().noDefaultValue();
            sessionConfiguration.removeConfig(keyToDelete);
            // It's safe to build ExecutionContext directly because origin configuration is legal.
            this.executionContext = new ExecutionContext(executionContext);
        }
    }

    /** Set properties. It will rebuild a new {@link ExecutionContext} */
    public void set(String key, String value) {
        Configuration originConfiguration = sessionConfiguration.clone();

        sessionConfiguration.setString(key, value);
        try {
            // Renew the ExecutionContext.
            // Book keep all the session states of current ExecutionContext then
            // re-register them into the new one.
            ExecutionContext newContext = new ExecutionContext(executionContext);
            // update the reference
            this.executionContext = newContext;
        } catch (Exception e) {
            // get error and reset the key with old value
            resetSessionConfigurationToDefault(originConfiguration);
            throw new SqlExecutionException(
                    String.format("Failed to set key %s with value %s.", key, value), e);
        }
    }

    /** Close resources, e.g. catalogs. */
    public void close() {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            for (String name : sessionState.catalogManager.listCatalogs()) {
                sessionState.catalogManager.getCatalog(name).ifPresent(Catalog::close);
            }
        }
        classLoader.close();
    }

    // --------------------------------------------------------------------------------------------
    // Helper method to create
    // --------------------------------------------------------------------------------------------

    public static SessionContext create(DefaultContext defaultContext, String sessionId) {
        // --------------------------------------------------------------------------------------------------------------
        // Init config
        // --------------------------------------------------------------------------------------------------------------

        Configuration configuration = defaultContext.getFlinkConfig().clone();

        // --------------------------------------------------------------------------------------------------------------
        // Init classloader
        // --------------------------------------------------------------------------------------------------------------

        // here use ClientMutableURLClassLoader to support remove jar
        final ClientWrapperClassLoader userClassLoader =
                new ClientWrapperClassLoader(
                        ClientClassloaderUtil.buildUserClassLoader(
                                defaultContext.getDependencies(),
                                SessionContext.class.getClassLoader(),
                                new Configuration(configuration)),
                        configuration);

        // --------------------------------------------------------------------------------------------------------------
        // Init session state
        // --------------------------------------------------------------------------------------------------------------

        final ClientResourceManager resourceManager =
                new ClientResourceManager(configuration, userClassLoader);

        final ModuleManager moduleManager = new ModuleManager();

        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(configuration).build();

        final CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(userClassLoader)
                        .config(configuration)
                        .defaultCatalog(
                                settings.getBuiltInCatalogName(),
                                new GenericInMemoryCatalog(
                                        settings.getBuiltInCatalogName(),
                                        settings.getBuiltInDatabaseName()))
                        .build();

        final FunctionCatalog functionCatalog =
                new FunctionCatalog(configuration, resourceManager, catalogManager, moduleManager);
        final SessionState sessionState =
                new SessionState(catalogManager, moduleManager, resourceManager, functionCatalog);

        // --------------------------------------------------------------------------------------------------------------
        // Init ExecutionContext
        // --------------------------------------------------------------------------------------------------------------

        ExecutionContext executionContext =
                new ExecutionContext(configuration, userClassLoader, sessionState);

        return new SessionContext(
                defaultContext,
                sessionId,
                configuration,
                userClassLoader,
                sessionState,
                executionContext);
    }

    public void removeJar(String jarPath) {
        URL jarURL = sessionState.resourceManager.unregisterJarResource(jarPath);
        if (jarURL == null) {
            LOG.warn(
                    String.format(
                            "Could not remove the specified jar because the jar path [%s] hadn't registered to classloader.",
                            jarPath));
            return;
        }
        // remove jar from classloader
        classLoader.removeURL(jarURL);
    }

    // --------------------------------------------------------------------------------------------
    // Inner class
    // --------------------------------------------------------------------------------------------

    /** session state. */
    public static class SessionState {

        public final CatalogManager catalogManager;
        public final ModuleManager moduleManager;
        public final ClientResourceManager resourceManager;
        public final FunctionCatalog functionCatalog;

        public SessionState(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                ClientResourceManager resourceManager,
                FunctionCatalog functionCatalog) {
            this.catalogManager = catalogManager;
            this.moduleManager = moduleManager;
            this.resourceManager = resourceManager;
            this.functionCatalog = functionCatalog;
        }
    }

    // --------------------------------------------------------------------------------------------

    private void resetSessionConfigurationToDefault(Configuration defaultConf) {
        for (String key : sessionConfiguration.toMap().keySet()) {
            // Don't care the type of the option
            ConfigOption<String> keyToDelete = ConfigOptions.key(key).stringType().noDefaultValue();
            sessionConfiguration.removeConfig(keyToDelete);
        }
        sessionConfiguration.addAll(defaultConf);
    }
}
