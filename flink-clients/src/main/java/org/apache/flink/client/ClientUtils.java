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

package org.apache.flink.client;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.ContextEnvironmentFactory;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.ProgramMissingJobException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.ExecutorServiceLoader;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility functions for Flink client.
 */
public enum ClientUtils {
	;

	private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

	public static void checkJarFile(URL jar) throws IOException {
		File jarFile;
		try {
			jarFile = new File(jar.toURI());
		} catch (URISyntaxException e) {
			throw new IOException("JAR file path is invalid '" + jar + '\'');
		}
		if (!jarFile.exists()) {
			throw new IOException("JAR file does not exist '" + jarFile.getAbsolutePath() + '\'');
		}
		if (!jarFile.canRead()) {
			throw new IOException("JAR file can't be read '" + jarFile.getAbsolutePath() + '\'');
		}

		try (JarFile ignored = new JarFile(jarFile)) {
			// verify that we can open the Jar file
		} catch (IOException e) {
			throw new IOException("Error while opening jar file '" + jarFile.getAbsolutePath() + '\'', e);
		}
	}

	public static ClassLoader buildUserCodeClassLoader(
			List<URL> jars,
			List<URL> classpaths,
			ClassLoader parent,
			Configuration configuration) {
		URL[] urls = new URL[jars.size() + classpaths.size()];
		for (int i = 0; i < jars.size(); i++) {
			urls[i] = jars.get(i);
		}
		for (int i = 0; i < classpaths.size(); i++) {
			urls[i + jars.size()] = classpaths.get(i);
		}
		final String[] alwaysParentFirstLoaderPatterns = CoreOptions.getParentFirstLoaderPatterns(configuration);
		final String classLoaderResolveOrder =
			configuration.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);
		FlinkUserCodeClassLoaders.ResolveOrder resolveOrder =
			FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder);
		return FlinkUserCodeClassLoaders.create(resolveOrder, urls, parent, alwaysParentFirstLoaderPatterns);
	}

	public static JobExecutionResult submitJob(
			ClusterClient<?> client,
			JobGraph jobGraph) throws ProgramInvocationException {
		checkNotNull(client);
		checkNotNull(jobGraph);
		try {
			return client
				.submitJob(jobGraph)
				.thenApply(JobSubmissionResult::getJobID)
				.thenApply(DetachedJobExecutionResult::new)
				.get();
		} catch (InterruptedException | ExecutionException e) {
			ExceptionUtils.checkInterrupted(e);
			throw new ProgramInvocationException("Could not run job in detached mode.", jobGraph.getJobID(), e);
		}
	}

	public static JobExecutionResult submitJobAndWaitForResult(
			ClusterClient<?> client,
			JobGraph jobGraph,
			ClassLoader classLoader) throws ProgramInvocationException {
		checkNotNull(client);
		checkNotNull(jobGraph);
		checkNotNull(classLoader);

		JobResult jobResult;

		try {
			jobResult = client
				.submitJob(jobGraph)
				.thenApply(JobSubmissionResult::getJobID)
				.thenCompose(client::requestJobResult)
				.get();
		} catch (InterruptedException | ExecutionException e) {
			ExceptionUtils.checkInterrupted(e);
			throw new ProgramInvocationException("Could not run job", jobGraph.getJobID(), e);
		}

		try {
			return jobResult.toJobExecutionResult(classLoader);
		} catch (JobExecutionException | IOException | ClassNotFoundException e) {
			throw new ProgramInvocationException("Job failed", jobGraph.getJobID(), e);
		}
	}

	public static JobSubmissionResult executeProgram(
			ExecutorServiceLoader executorServiceLoader,
			Configuration configuration,
			PackagedProgram program) throws ProgramMissingJobException, ProgramInvocationException {

		checkNotNull(executorServiceLoader);
		final ClassLoader userCodeClassLoader = program.getUserCodeClassLoader();

		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(userCodeClassLoader);

			LOG.info("Starting program (detached: {})", !configuration.getBoolean(DeploymentOptions.ATTACHED));

			final AtomicReference<JobExecutionResult> jobExecutionResult = new AtomicReference<>();

			ContextEnvironmentFactory factory = new ContextEnvironmentFactory(
					executorServiceLoader,
					configuration,
					userCodeClassLoader,
					jobExecutionResult);
			ContextEnvironment.setAsContext(factory);

			try {
				program.invokeInteractiveModeForExecution();

				JobExecutionResult result = jobExecutionResult.get();
				if (result == null) {
					throw new ProgramMissingJobException("The program didn't contain a Flink job.");
				}
				return result;
			} finally {
				ContextEnvironment.unsetContext();
			}
		}
		finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}
}
