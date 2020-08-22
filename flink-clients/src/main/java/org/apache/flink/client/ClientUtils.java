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

import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility functions for Flink client.
 */
public enum ClientUtils {
	;

	private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

	public static URLClassLoader buildUserCodeClassLoader(
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
		final boolean checkClassloaderLeak = configuration.getBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER);
		return FlinkUserCodeClassLoaders.create(
			resolveOrder,
			urls,
			parent,
			alwaysParentFirstLoaderPatterns,
			NOOP_EXCEPTION_HANDLER,
			checkClassloaderLeak);
	}

	public static void executeProgram(
			PipelineExecutorServiceLoader executorServiceLoader,
			Configuration configuration,
			PackagedProgram program,
			boolean enforceSingleJobExecution,
			boolean suppressSysout) throws ProgramInvocationException {
		checkNotNull(executorServiceLoader);
		final ClassLoader userCodeClassLoader = program.getUserCodeClassLoader();
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(userCodeClassLoader);

			LOG.info("Starting program (detached: {})", !configuration.getBoolean(DeploymentOptions.ATTACHED));

			ContextEnvironment.setAsContext(
				executorServiceLoader,
				configuration,
				userCodeClassLoader,
				enforceSingleJobExecution,
				suppressSysout);

			StreamContextEnvironment.setAsContext(
				executorServiceLoader,
				configuration,
				userCodeClassLoader,
				enforceSingleJobExecution,
				suppressSysout);

			try {
				program.invokeInteractiveModeForExecution();
			} finally {
				ContextEnvironment.unsetAsContext();
				StreamContextEnvironment.unsetAsContext();
			}
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}
}
