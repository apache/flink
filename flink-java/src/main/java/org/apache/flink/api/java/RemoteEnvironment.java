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

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An {@link ExecutionEnvironment} that sends programs to a cluster for execution. The environment
 * needs to be created with the address and port of the JobManager of the Flink cluster that should
 * execute the programs.
 *
 * <p>Many programs executed via the remote environment depend on additional classes. Such classes
 * may be the classes of functions (transformation, aggregation, ...) or libraries. Those classes
 * must be attached to the remote environment as JAR files, to allow the environment to ship the
 * classes into the cluster for the distributed execution.
 *
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@Public
public class RemoteEnvironment extends ExecutionEnvironment {

    /**
     * Creates a new RemoteEnvironment that points to the master (JobManager) described by the given
     * host name and port.
     *
     * <p>Each program execution will have all the given JAR files in its classpath.
     *
     * @param host The host name or address of the master (JobManager), where the program should be
     *     executed.
     * @param port The port of the master (JobManager), where the program should be executed.
     * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the
     *     program uses user-defined functions, user-defined input formats, or any libraries, those
     *     must be provided in the JAR files.
     */
    public RemoteEnvironment(String host, int port, String... jarFiles) {
        this(host, port, new Configuration(), jarFiles, null);
    }

    /**
     * Creates a new RemoteEnvironment that points to the master (JobManager) described by the given
     * host name and port.
     *
     * <p>Each program execution will have all the given JAR files in its classpath.
     *
     * @param host The host name or address of the master (JobManager), where the program should be
     *     executed.
     * @param port The port of the master (JobManager), where the program should be executed.
     * @param clientConfig The configuration used by the client that connects to the cluster.
     * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the
     *     program uses user-defined functions, user-defined input formats, or any libraries, those
     *     must be provided in the JAR files.
     */
    public RemoteEnvironment(String host, int port, Configuration clientConfig, String[] jarFiles) {
        this(host, port, clientConfig, jarFiles, null);
    }

    /**
     * Creates a new RemoteEnvironment that points to the master (JobManager) described by the given
     * host name and port.
     *
     * <p>Each program execution will have all the given JAR files in its classpath.
     *
     * @param host The host name or address of the master (JobManager), where the program should be
     *     executed.
     * @param port The port of the master (JobManager), where the program should be executed.
     * @param clientConfig The configuration used by the client that connects to the cluster.
     * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the
     *     program uses user-defined functions, user-defined input formats, or any libraries, those
     *     must be provided in the JAR files.
     * @param globalClasspaths The paths of directories and JAR files that are added to each user
     *     code classloader on all nodes in the cluster. Note that the paths must specify a protocol
     *     (e.g. file://) and be accessible on all nodes (e.g. by means of a NFS share). The
     *     protocol must be supported by the {@link java.net.URLClassLoader}.
     */
    public RemoteEnvironment(
            String host,
            int port,
            Configuration clientConfig,
            String[] jarFiles,
            URL[] globalClasspaths) {
        super(
                validateAndGetEffectiveConfiguration(
                        clientConfig, host, port, jarFiles, globalClasspaths));
    }

    private static Configuration validateAndGetEffectiveConfiguration(
            final Configuration configuration,
            final String host,
            final int port,
            final String[] jarFiles,
            final URL[] globalClasspaths) {
        RemoteEnvironmentConfigUtils.validate(host, port);
        return getEffectiveConfiguration(
                getClientConfiguration(configuration),
                host,
                port,
                jarFiles,
                getClasspathURLs(globalClasspaths));
    }

    private static Configuration getClientConfiguration(final Configuration configuration) {
        return configuration == null ? new Configuration() : configuration;
    }

    private static List<URL> getClasspathURLs(final URL[] classpaths) {
        return classpaths == null ? Collections.emptyList() : Arrays.asList(classpaths);
    }

    private static Configuration getEffectiveConfiguration(
            final Configuration baseConfiguration,
            final String host,
            final int port,
            final String[] jars,
            final List<URL> classpaths) {

        final Configuration effectiveConfiguration = new Configuration(baseConfiguration);

        RemoteEnvironmentConfigUtils.setJobManagerAddressToConfig(
                host, port, effectiveConfiguration);
        RemoteEnvironmentConfigUtils.setJarURLsToConfig(jars, effectiveConfiguration);
        ConfigUtils.encodeCollectionToConfig(
                effectiveConfiguration, PipelineOptions.CLASSPATHS, classpaths, URL::toString);

        // these should be set in the end to overwrite any values from the client config provided in
        // the constructor.
        effectiveConfiguration.setString(DeploymentOptions.TARGET, "remote");
        effectiveConfiguration.setBoolean(DeploymentOptions.ATTACHED, true);

        return effectiveConfiguration;
    }

    @Override
    public String toString() {
        final String host = getConfiguration().getString(JobManagerOptions.ADDRESS);
        final int port = getConfiguration().getInteger(JobManagerOptions.PORT);
        final String parallelism = (getParallelism() == -1 ? "default" : "" + getParallelism());

        return "Remote Environment ("
                + host
                + ":"
                + port
                + " - parallelism = "
                + parallelism
                + ").";
    }
}
