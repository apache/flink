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
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.context.SessionContext;
import org.apache.flink.util.JarUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Utils to build a {@link DefaultContext} and {@link SessionContext}. */
public class LocalContextUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LocalContextUtils.class);

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

        configuration.addAll(options.getPythonConfiguration());
        final List<URL> dependencies = discoverDependencies(jars, libDirs);

        return new DefaultContext(dependencies, configuration, commandLines);
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

        // add python dependencies by default
        try {
            URL location =
                    Class.forName(
                                    "org.apache.flink.python.PythonFunctionRunner",
                                    false,
                                    Thread.currentThread().getContextClassLoader())
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation();
            if (Paths.get(location.toURI()).toFile().isFile()) {
                dependencies.add(location);
            }
        } catch (URISyntaxException | ClassNotFoundException e) {
            // TODO: Introduce user jar analyzer to determine user jar whether contains the python
            // function or not. If user jar contains python function, manually add the python
            // dependencies
            throw new SqlExecutionException(
                    "Don't find python dependencies. Please add the flink-python jar via `--jar` command option manually.",
                    e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Using the following dependencies: {}", dependencies);
        }

        return dependencies;
    }
}
