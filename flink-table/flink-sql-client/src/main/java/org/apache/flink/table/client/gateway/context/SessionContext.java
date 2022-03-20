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
import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.util.JarUtils;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    // SafetyNetWrapperClassLoader doesn't override the getURL therefore we need to maintain the
    // dependencies by ourselves.
    private Set<URL> dependencies;
    private URLClassLoader classLoader;
    private ExecutionContext executionContext;

    private SessionContext(
            DefaultContext defaultContext,
            String sessionId,
            Configuration sessionConfiguration,
            URLClassLoader classLoader,
            SessionState sessionState,
            ExecutionContext executionContext) {
        this.defaultContext = defaultContext;
        this.sessionId = sessionId;
        this.sessionConfiguration = sessionConfiguration;
        this.classLoader = classLoader;
        this.sessionState = sessionState;
        this.executionContext = executionContext;
        this.dependencies = new HashSet<>(defaultContext.getDependencies());
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
        return dependencies;
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
        // Reset configuration will revert the `pipeline.jars`. To make the current classloader
        // still work, add the maintained dependencies into the configuration.
        updateClassLoaderAndDependencies(dependencies);
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

        final EnvironmentSettings settings = EnvironmentSettings.fromConfiguration(configuration);

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
                new ExecutionContext(configuration, classLoader, sessionState);

        return new SessionContext(
                defaultContext,
                sessionId,
                configuration,
                classLoader,
                sessionState,
                executionContext);
    }

    public void addJar(String jarPath) {
        URL jarURL = getURLFromPath(jarPath, "SQL Client only supports to add local jars.");
        if (dependencies.contains(jarURL)) {
            return;
        }

        Set<URL> newDependencies = new HashSet<>(dependencies);
        // merge the jars in config with the jars maintained in session
        Set<URL> jarsInConfig = getJarsInConfig();
        newDependencies.addAll(jarsInConfig);
        newDependencies.add(jarURL);
        updateClassLoaderAndDependencies(newDependencies);

        // renew the execution context
        executionContext = new ExecutionContext(sessionConfiguration, classLoader, sessionState);
    }

    public void removeJar(String jarPath) {
        URL jarURL = getURLFromPath(jarPath, "SQL Client only supports to remove local jars.");
        if (!dependencies.contains(jarURL)) {
            LOG.warn(
                    String.format(
                            "Could not remove the specified jar because the jar path(%s) is not found in session classloader.",
                            jarPath));
            return;
        }

        Set<URL> newDependencies = new HashSet<>(dependencies);
        // merge the jars in config with the jars maintained in session
        Set<URL> jarsInConfig = getJarsInConfig();
        newDependencies.addAll(jarsInConfig);
        newDependencies.remove(jarURL);
        updateClassLoaderAndDependencies(newDependencies);

        // renew the execution context
        executionContext = new ExecutionContext(sessionConfiguration, classLoader, sessionState);
    }

    public List<String> listJars() {
        return dependencies.stream().map(URL::getPath).collect(Collectors.toList());
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

    // --------------------------------------------------------------------------------------------

    private void resetSessionConfigurationToDefault(Configuration defaultConf) {
        for (String key : sessionConfiguration.toMap().keySet()) {
            // Don't care the type of the option
            ConfigOption<String> keyToDelete = ConfigOptions.key(key).stringType().noDefaultValue();
            sessionConfiguration.removeConfig(keyToDelete);
        }
        sessionConfiguration.addAll(defaultConf);
    }

    private void updateClassLoaderAndDependencies(Collection<URL> newDependencies) {
        ConfigUtils.encodeCollectionToConfig(
                sessionConfiguration,
                PipelineOptions.JARS,
                new ArrayList<>(newDependencies),
                URL::toString);

        // TODO: update the the classloader in CatalogManager.
        classLoader =
                ClientUtils.buildUserCodeClassLoader(
                        new ArrayList<>(newDependencies),
                        Collections.emptyList(),
                        SessionContext.class.getClassLoader(),
                        sessionConfiguration);
        dependencies = new HashSet<>(newDependencies);
    }

    private URL getURLFromPath(String jarPath, String message) {
        Path path = new Path(jarPath);
        String scheme = path.toUri().getScheme();
        if (scheme != null && !scheme.equals("file")) {
            throw new SqlExecutionException(message);
        }

        Path qualifiedPath = path.makeQualified(FileSystem.getLocalFileSystem());

        try {
            URL jarURL = qualifiedPath.toUri().toURL();
            JarUtils.checkJarFile(jarURL);
            return jarURL;
        } catch (MalformedURLException e) {
            throw new SqlExecutionException(
                    String.format("Failed to parse the input jar path: %s", jarPath), e);
        } catch (IOException e) {
            throw new SqlExecutionException(
                    String.format("Failed to get the jar file with specified path: %s", jarPath),
                    e);
        }
    }

    private Set<URL> getJarsInConfig() {
        Set<URL> jarsInConfig;
        try {
            jarsInConfig =
                    new HashSet<>(
                            ConfigUtils.decodeListFromConfig(
                                    sessionConfiguration, PipelineOptions.JARS, URL::new));
        } catch (MalformedURLException e) {
            throw new SqlExecutionException(
                    "Failed to parse the option `pipeline.jars` in configuration.", e);
        }
        return jarsInConfig;
    }
}
