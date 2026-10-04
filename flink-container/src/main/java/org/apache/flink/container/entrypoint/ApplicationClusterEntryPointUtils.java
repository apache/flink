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

package org.apache.flink.container.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.artifact.ArtifactFetchManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.util.Collection;

@Internal
final class ApplicationClusterEntryPointUtils {

    static Configuration loadConfiguration(
            StandaloneApplicationClusterConfiguration clusterConfiguration) {
        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());
        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(
                        clusterConfiguration.getConfigDir(), dynamicProperties);

        setStaticJobId(clusterConfiguration, configuration);

        SavepointRestoreSettings.toConfiguration(
                clusterConfiguration.getSavepointRestoreSettings(), configuration);
        return configuration;
    }

    static PackagedProgram getPackagedProgram(
            final StandaloneApplicationClusterConfiguration clusterConfiguration,
            Configuration flinkConfiguration)
            throws Exception {
        final File userLibDir = ClusterEntrypointUtils.tryFindUserLibDirectory().orElse(null);

        File jobJar = null;
        Collection<File> artifacts = null;
        if (clusterConfiguration.hasJars()) {
            ArtifactFetchManager fetchMgr = new ArtifactFetchManager(flinkConfiguration);
            ArtifactFetchManager.Result res =
                    fetchMgr.fetchArtifacts(clusterConfiguration.getJars());

            jobJar = res.getJobJar();
            artifacts = res.getArtifacts();
        }

        final PackagedProgramRetriever programRetriever =
                DefaultPackagedProgramRetriever.create(
                        userLibDir,
                        jobJar,
                        artifacts,
                        clusterConfiguration.getJobClassName(),
                        clusterConfiguration.getArgs(),
                        flinkConfiguration);
        return programRetriever.getPackagedProgram();
    }

    private static void setStaticJobId(
            StandaloneApplicationClusterConfiguration clusterConfiguration,
            Configuration configuration) {
        final JobID jobId = clusterConfiguration.getJobId();
        if (jobId != null) {
            configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        }
    }
}
