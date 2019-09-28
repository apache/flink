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

import org.apache.flink.api.common.Plan;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.jar.JarFile;

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

	public static ClassLoader buildUserCodeClassLoader(List<URL> jars, List<URL> classpaths, ClassLoader parent) {
		URL[] urls = new URL[jars.size() + classpaths.size()];
		for (int i = 0; i < jars.size(); i++) {
			urls[i] = jars.get(i);
		}
		for (int i = 0; i < classpaths.size(); i++) {
			urls[i + jars.size()] = classpaths.get(i);
		}
		return FlinkUserCodeClassLoaders.parentFirst(urls, parent);
	}

	public static FlinkPlan getOptimizedPlan(
		Optimizer compiler,
		PackagedProgram program,
		int parallelism) throws CompilerException, ProgramInvocationException {
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(program.getUserCodeClassLoader());

			// TODO temporary hack to support the optimizer plan preview
			OptimizerPlanEnvironment env = new OptimizerPlanEnvironment(compiler);
			if (parallelism > 0) {
				env.setParallelism(parallelism);
			}
			return env.getOptimizedPlan(program);
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	public static OptimizedPlan getOptimizedPlan(
		Optimizer compiler,
		Plan p,
		int parallelism) throws CompilerException {
		if (parallelism > 0 && p.getDefaultParallelism() <= 0) {
			LOG.debug("Changing plan default parallelism from {} to {}", p.getDefaultParallelism(), parallelism);
			p.setDefaultParallelism(parallelism);
		}
		LOG.debug("Set parallelism {}, plan default parallelism {}", parallelism, p.getDefaultParallelism());

		return compiler.compile(p);
	}

	public static JobGraph getJobGraph(
		Configuration flinkConfig,
		FlinkPlan optPlan,
		List<URL> jarFiles,
		List<URL> classpaths,
		SavepointRestoreSettings savepointSettings) {
		JobGraph job;
		if (optPlan instanceof StreamingPlan) {
			job = ((StreamingPlan) optPlan).getJobGraph();
			job.setSavepointRestoreSettings(savepointSettings);
		} else {
			JobGraphGenerator gen = new JobGraphGenerator(flinkConfig);
			job = gen.compileJobGraph((OptimizedPlan) optPlan);
		}

		for (URL jar : jarFiles) {
			try {
				job.addJar(new Path(jar.toURI()));
			} catch (URISyntaxException e) {
				throw new RuntimeException("URL is invalid. This should not happen.", e);
			}
		}

		job.setClasspaths(classpaths);

		return job;
	}
}
