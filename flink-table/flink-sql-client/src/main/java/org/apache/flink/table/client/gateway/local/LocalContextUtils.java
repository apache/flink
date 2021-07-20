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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.context.SessionContext;
import org.apache.flink.util.JarUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.client.config.entries.ConfigurationEntry.create;
import static org.apache.flink.table.client.config.entries.ConfigurationEntry.merge;

/** Utils to build a {@link DefaultContext} and {@link SessionContext}. */
public class LocalContextUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LocalContextUtils.class);

    private static final String DEFAULT_ENV_FILE = "sql-client-defaults.yaml";

    private static final String DEFAULT_SESSION_ID = "default";

    public static DefaultContext buildDefaultContext(CliOptions options) {

        final List<URL> jars;
        if (options.getJars() != null) {
            jars = options.getJars();
        } else {
            jars = Collections.emptyList();
        }
        final List<URL> libDirs;
        if (options.getLibraryDirs() != null) {
            libDirs = options.getLibraryDirs();
        } else {
            libDirs = Collections.emptyList();
        }

        // 1. find the configuration directory
        String flinkConfigDir = CliFrontend.getConfigurationDirectoryFromEnv();

        // 2. load the global configuration
        Configuration configuration = GlobalConfiguration.loadConfiguration(flinkConfigDir);

        // 3. load the custom command lines
        List<CustomCommandLine> commandLines =
                CliFrontend.loadCustomCommandLines(configuration, flinkConfigDir);

        // try to find a environment if not found use the default env;
        Environment defaultEnvironment =
                getLocalDefaultEnvironment(flinkConfigDir, options.getDefaults());
        Environment sessionEnvironment =
                getSessionEnvironment(defaultEnvironment, options.getEnvironment());

        appendPythonConfig(sessionEnvironment, options.getPythonConfiguration());
        final List<URL> dependencies = discoverDependencies(jars, libDirs);

        return new DefaultContext(sessionEnvironment, dependencies, configuration, commandLines);
    }

    public static SessionContext buildSessionContext(
            @Nullable String sessionId, DefaultContext defaultContext) {
        final SessionContext context;
        if (sessionId == null) {
            context = SessionContext.create(defaultContext, DEFAULT_SESSION_ID);
        } else {
            context = SessionContext.create(defaultContext, sessionId);
        }
        return context;
    }

    // --------------------------------------------------------------------------------------------

    private static Environment getLocalDefaultEnvironment(
            String flinkConfigDir, @Nullable URL defaultUrl) {
        // try to find a default environment
        URL defaultEnv = null;
        if (defaultUrl == null) {
            final String defaultFilePath = flinkConfigDir + "/" + DEFAULT_ENV_FILE;
            System.out.println("No default environment specified.");
            System.out.print("Searching for '" + defaultFilePath + "'...");
            final File file = new File(defaultFilePath);
            if (file.exists()) {
                System.out.println("found.");
                try {
                    defaultEnv = org.apache.flink.core.fs.Path.fromLocalFile(file).toUri().toURL();
                } catch (MalformedURLException e) {
                    throw new SqlClientException(e);
                }
                LOG.info("Using default environment file: {}", defaultEnv);
            } else {
                System.out.println("not found.");
            }
        }

        // inform user
        Environment defaultEnvironment;
        if (defaultEnv != null) {
            System.out.println("Reading default environment from: " + defaultEnv);
            try {
                defaultEnvironment = Environment.parse(defaultEnv);
            } catch (IOException e) {
                throw new SqlClientException(
                        "Could not read default environment file at: " + defaultEnv, e);
            }
        } else {
            defaultEnvironment = new Environment();
        }
        return defaultEnvironment;
    }

    private static Environment getSessionEnvironment(
            Environment defaultEnvironment, @Nullable URL sessionEnvironmentURL) {
        if (sessionEnvironmentURL == null) {
            return defaultEnvironment;
        }

        Environment sessionEnvironment = readSessionEnvironment(sessionEnvironmentURL);
        return Environment.merge(defaultEnvironment, sessionEnvironment);
    }

    // --------------------------------------------------------------------------------------------

    private static List<URL> discoverDependencies(List<URL> jars, List<URL> libraries) {
        final List<URL> dependencies = new ArrayList<>();
        try {
            // find jar files
            for (URL url : jars) {
                JarUtils.checkJarFile(url);
                dependencies.add(url);
            }

            // find jar files in library directories
            for (URL libUrl : libraries) {
                final File dir = new File(libUrl.toURI());
                if (!dir.isDirectory()) {
                    throw new SqlClientException("Directory expected: " + dir);
                } else if (!dir.canRead()) {
                    throw new SqlClientException("Directory cannot be read: " + dir);
                }
                final File[] files = dir.listFiles();
                if (files == null) {
                    throw new SqlClientException("Directory cannot be read: " + dir);
                }
                for (File f : files) {
                    // only consider jars
                    if (f.isFile() && f.getAbsolutePath().toLowerCase().endsWith(".jar")) {
                        final URL url = f.toURI().toURL();
                        JarUtils.checkJarFile(url);
                        dependencies.add(url);
                    }
                }
            }
        } catch (Exception e) {
            throw new SqlClientException("Could not load all required JAR files.", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Using the following dependencies: {}", dependencies);
        }

        return dependencies;
    }

    private static Environment readSessionEnvironment(@Nullable URL envUrl) {
        // use an empty environment by default
        if (envUrl == null) {
            System.out.println("No session environment specified.");
            return new Environment();
        }

        System.out.println("Reading session environment from: " + envUrl);
        LOG.info("Using session environment file: {}", envUrl);
        try {
            return Environment.parse(envUrl);
        } catch (IOException e) {
            throw new SqlClientException(
                    "Could not read session environment file at: " + envUrl, e);
        }
    }

    private static void appendPythonConfig(Environment env, Configuration pythonConfiguration) {
        Map<String, Object> pythonConfig = new HashMap<>(pythonConfiguration.toMap());
        Map<String, Object> combinedConfig =
                new HashMap<>(merge(env.getConfiguration(), create(pythonConfig)).asMap());
        env.setConfiguration(combinedConfig);
    }
}
