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

package org.apache.flink.api.java;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.JarUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A set of tools used by batch and streaming remote environments when preparing their
 * configurations.
 */
@Internal
public class RemoteEnvironmentConfigUtils {

    public static void validate(final String host, final int port) {
        if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
            throw new InvalidProgramException(
                    "The RemoteEnvironment cannot be instantiated when running in a pre-defined context "
                            + "(such as Command Line Client, Scala Shell, or TestEnvironment)");
        }

        checkNotNull(host);
        checkArgument(port > 0 && port < 0xffff);
    }

    public static void setJobManagerAddressToConfig(
            final String host, final int port, final Configuration configuration) {
        final InetSocketAddress address = new InetSocketAddress(host, port);
        configuration.setString(JobManagerOptions.ADDRESS, address.getHostString());
        configuration.setInteger(JobManagerOptions.PORT, address.getPort());
        configuration.setString(RestOptions.ADDRESS, address.getHostString());
        configuration.setInteger(RestOptions.PORT, address.getPort());
    }

    public static void setJarURLsToConfig(final String[] jars, final Configuration configuration) {
        final List<URL> jarURLs = getJarFiles(jars);
        ConfigUtils.encodeCollectionToConfig(
                configuration, PipelineOptions.JARS, jarURLs, URL::toString);
    }

    private static List<URL> getJarFiles(final String[] jars) {
        return jars == null
                ? Collections.emptyList()
                : Arrays.stream(jars)
                        .map(
                                jarPath -> {
                                    try {
                                        final URL fileURL =
                                                new File(jarPath).getAbsoluteFile().toURI().toURL();
                                        JarUtils.checkJarFile(fileURL);
                                        return fileURL;
                                    } catch (MalformedURLException e) {
                                        throw new IllegalArgumentException(
                                                "JAR file path invalid", e);
                                    } catch (IOException e) {
                                        throw new RuntimeException(
                                                "Problem with jar file " + jarPath, e);
                                    }
                                })
                        .collect(Collectors.toList());
    }
}
