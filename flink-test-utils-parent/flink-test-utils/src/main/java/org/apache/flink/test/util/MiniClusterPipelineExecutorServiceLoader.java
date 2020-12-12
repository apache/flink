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
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * A {@link PipelineExecutorServiceLoader} that is hardwired to return {@link PipelineExecutor
 * PipelineExecutors} that use a given {@link MiniCluster}.
 */
public class MiniClusterPipelineExecutorServiceLoader implements PipelineExecutorServiceLoader {
	public static final String NAME = "minicluster";

	private final MiniCluster miniCluster;

	public MiniClusterPipelineExecutorServiceLoader(MiniCluster miniCluster) {
		this.miniCluster = miniCluster;
	}

	/**
	 * Populates a {@link Configuration} that is compatible with this {@link
	 * MiniClusterPipelineExecutorServiceLoader}.
	 */
	public static Configuration createConfiguration(
			Collection<Path> jarFiles,
			Collection<URL> classPaths) {
		Configuration config = new Configuration();
		ConfigUtils.encodeCollectionToConfig(
				config,
				PipelineOptions.JARS,
				jarFiles,
				MiniClusterPipelineExecutorServiceLoader::getAbsoluteURL);
		ConfigUtils.encodeCollectionToConfig(
				config,
				PipelineOptions.CLASSPATHS,
				classPaths,
				URL::toString);
		config.set(DeploymentOptions.TARGET, MiniClusterPipelineExecutorServiceLoader.NAME);
		config.set(DeploymentOptions.ATTACHED, true);
		return config;
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

	private static class MiniClusterExecutor implements PipelineExecutor {

		private final MiniCluster miniCluster;

		public MiniClusterExecutor(MiniCluster miniCluster) {
			this.miniCluster = miniCluster;
		}

		@Override
		public CompletableFuture<JobClient> execute(
				Pipeline pipeline,
				Configuration configuration,
				ClassLoader userCodeClassLoader) throws Exception {
			final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);
			if (jobGraph.getSavepointRestoreSettings() == SavepointRestoreSettings.none() && pipeline instanceof StreamGraph) {
				jobGraph.setSavepointRestoreSettings(((StreamGraph) pipeline).getSavepointRestoreSettings());
			}
			return miniCluster.submitJob(jobGraph)
					.thenApply(result -> new MiniClusterJobClient(
							result.getJobID(),
							miniCluster,
							userCodeClassLoader,
							MiniClusterJobClient.JobFinalizationBehavior.NOTHING));
		}
	}
}
