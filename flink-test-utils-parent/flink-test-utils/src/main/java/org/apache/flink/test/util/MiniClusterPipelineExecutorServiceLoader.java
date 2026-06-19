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

package org.apache.flink.test.util;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.CacheSupportedPipelineExecutor;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerUtils;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * A {@link PipelineExecutorServiceLoader} that is hardwired to return {@link PipelineExecutor
 * PipelineExecutors} that use a given {@link MiniCluster}.
 */
public class MiniClusterPipelineExecutorServiceLoader implements PipelineExecutorServiceLoader {

    private static final Logger LOG =
            LoggerFactory.getLogger(MiniClusterPipelineExecutorServiceLoader.class);

    public static final String NAME = "minicluster";

    private final MiniCluster miniCluster;

    public MiniClusterPipelineExecutorServiceLoader(MiniCluster miniCluster) {
        this.miniCluster = miniCluster;
    }

    /**
     * Populates a {@link Configuration} that is compatible with this {@link
     * MiniClusterPipelineExecutorServiceLoader}.
     */
    public static Configuration updateConfigurationForMiniCluster(
            Configuration config, Collection<Path> jarFiles, Collection<URL> classPaths) {

        checkOverridesOption(config, PipelineOptions.JARS);
        checkOverridesOption(config, PipelineOptions.CLASSPATHS);
        checkOverridesOption(config, DeploymentOptions.TARGET);
        checkOverridesOption(config, DeploymentOptions.ATTACHED);

        ConfigUtils.encodeCollectionToConfig(
                config,
                PipelineOptions.JARS,
                jarFiles,
                MiniClusterPipelineExecutorServiceLoader::getAbsoluteURL);
        ConfigUtils.encodeCollectionToConfig(
                config, PipelineOptions.CLASSPATHS, classPaths, URL::toString);
        config.set(DeploymentOptions.TARGET, MiniClusterPipelineExecutorServiceLoader.NAME);
        config.set(DeploymentOptions.ATTACHED, true);
        return config;
    }

    private static void checkOverridesOption(Configuration config, ConfigOption<?> option) {
        if (config.contains(option)) {
            LOG.warn("Overriding config setting '{}' for MiniCluster.", option.key());
        }
    }

    private static String getAbsoluteURL(Path path) {
        FileSystem fs;
        try {
            fs = path.getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Could not get FileSystem from %s", path), e);
        }
        try {
            return path.makeQualified(fs).toUri().toURL().toString();
        } catch (MalformedURLException e) {
            throw new RuntimeException(String.format("Could not get URL from %s", path), e);
        }
    }

    @Override
    public PipelineExecutorFactory getExecutorFactory(Configuration configuration) {
        return new MiniClusterPipelineExecutorFactory(miniCluster);
    }

    @Override
    public Stream<String> getExecutorNames() {
        return Stream.of(MiniClusterPipelineExecutorServiceLoader.NAME);
    }

    private static class MiniClusterPipelineExecutorFactory implements PipelineExecutorFactory {
        private final MiniCluster miniCluster;

        public MiniClusterPipelineExecutorFactory(MiniCluster miniCluster) {
            this.miniCluster = miniCluster;
        }

        @Override
        public String getName() {
            return MiniClusterPipelineExecutorServiceLoader.NAME;
        }

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            return true;
        }

        @Override
        public PipelineExecutor getExecutor(Configuration configuration) {
            return new MiniClusterExecutor(miniCluster);
        }
    }

    private static class MiniClusterExecutor implements CacheSupportedPipelineExecutor {
        private final ExecutorService executorService =
                Executors.newFixedThreadPool(
                        1, new ExecutorThreadFactory("Flink-MiniClusterExecutor-IO"));
        private final MiniCluster miniCluster;
        private final List<JobStatusChangedListener> jobStatusChangedListeners;

        public MiniClusterExecutor(MiniCluster miniCluster) {
            this.miniCluster = miniCluster;
            this.jobStatusChangedListeners =
                    JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                            Thread.currentThread().getContextClassLoader(),
                            miniCluster.getConfiguration(),
                            executorService);
        }

        @Override
        public CompletableFuture<JobClient> execute(
                Pipeline pipeline, Configuration configuration, ClassLoader userCodeClassLoader)
                throws Exception {
            SavepointRestoreSettings savepointRestoreSettings =
                    ((StreamGraph) pipeline).getSavepointRestoreSettings();
            final StreamGraph streamGraph =
                    PipelineExecutorUtils.getStreamGraph(pipeline, configuration);
            if (streamGraph.getSavepointRestoreSettings() == SavepointRestoreSettings.none()) {
                streamGraph.setSavepointRestoreSettings(savepointRestoreSettings);
            }

            return miniCluster
                    .submitJob(streamGraph)
                    .whenComplete(
                            (ignored, throwable) -> {
                                if (throwable == null) {
                                    PipelineExecutorUtils.notifyJobStatusListeners(
                                            pipeline, streamGraph, jobStatusChangedListeners);
                                } else {
                                    LOG.error(
                                            "Failed to submit job graph to mini cluster.",
                                            throwable);
                                }
                            })
                    .thenApply(
                            result ->
                                    new MiniClusterJobClient(
                                            result.getJobID(),
                                            miniCluster,
                                            userCodeClassLoader,
                                            MiniClusterJobClient.JobFinalizationBehavior.NOTHING));
        }

        @Override
        public CompletableFuture<Set<AbstractID>> listCompletedClusterDatasetIds(
                Configuration configuration, ClassLoader userCodeClassloader) throws Exception {
            return miniCluster.listCompletedClusterDatasetIds();
        }

        @Override
        public CompletableFuture<Void> invalidateClusterDataset(
                AbstractID clusterDatasetId,
                Configuration configuration,
                ClassLoader userCodeClassloader)
                throws Exception {
            return miniCluster.invalidateClusterDataset(clusterDatasetId);
        }
    }
}
