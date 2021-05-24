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

package org.apache.flink.mesos.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.util.MesosUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entry point for running a TaskManager in a Mesos container.
 *
 * @deprecated Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the
 *     future (see FLINK-22352 for further details).
 */
@Deprecated
public class MesosTaskExecutorRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MesosTaskExecutorRunner.class);

    private static final int INIT_ERROR_EXIT_CODE = 31;

    private static final Options ALL_OPTIONS;

    static {
        ALL_OPTIONS = new Options().addOption(BootstrapTools.newDynamicPropertiesOption());
    }

    public static void main(String[] args) throws Exception {
        EnvironmentInformation.logEnvironmentInfo(
                LOG, MesosTaskExecutorRunner.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        // try to parse the command line arguments
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(ALL_OPTIONS, args);

        final Configuration configuration;
        try {
            Configuration dynamicProperties = BootstrapTools.parseDynamicProperties(cmd);
            LOG.info("Mesos dynamic properties: {}", dynamicProperties);

            configuration = MesosUtils.loadConfiguration(dynamicProperties, LOG);
        } catch (Throwable t) {
            LOG.error("Failed to load the TaskManager configuration and dynamic properties.", t);
            System.exit(INIT_ERROR_EXIT_CODE);
            return;
        }

        TaskManagerRunner.runTaskManagerProcessSecurely(configuration);
    }
}
