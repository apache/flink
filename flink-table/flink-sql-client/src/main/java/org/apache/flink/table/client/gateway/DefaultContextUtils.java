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

package org.apache.flink.table.client.gateway;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.util.JarUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Utils to build a {@link DefaultContext}. */
public class DefaultContextUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultContextUtils.class);

    public static DefaultContext buildDefaultContext(CliOptions.EmbeddedCliOptions options) {
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
        Configuration sessionConfig = options.getPythonConfiguration();
        sessionConfig.addAll(ConfigurationUtils.createConfiguration(options.getSessionConfig()));
        return DefaultContext.load(sessionConfig, discoverDependencies(jars, libDirs), true, true);
    }

    public static DefaultContext buildDefaultContext(CliOptions.GatewayCliOptions options) {
        return DefaultContext.load(
                ConfigurationUtils.createConfiguration(options.getSessionConfig()),
                Collections.emptyList(),
                false,
                false);
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
}
