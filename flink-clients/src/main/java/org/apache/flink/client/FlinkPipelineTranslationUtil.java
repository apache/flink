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
 *
 */

package org.apache.flink.client;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * Utility for transforming {@link Pipeline FlinkPipelines} into a {@link JobGraph}. This uses
 * reflection or service discovery to find the right {@link FlinkPipelineTranslator} for a given
 * subclass of {@link Pipeline}.
 */
public final class FlinkPipelineTranslationUtil {

	/**
	 * Transmogrifies the given {@link Pipeline} to a {@link JobGraph}.
	 */
	public static JobGraph getJobGraph(
			Pipeline pipeline,
			Configuration optimizerConfiguration,
			int defaultParallelism) {

		FlinkPipelineTranslator pipelineTranslator = getPipelineTranslator(pipeline);

		return pipelineTranslator.translateToJobGraph(pipeline,
				optimizerConfiguration,
				defaultParallelism);
	}

	/**
	 * Extracts the execution plan (as JSON) from the given {@link Pipeline}.
	 */
	public static String translateToJSONExecutionPlan(Pipeline pipeline) {
		FlinkPipelineTranslator pipelineTranslator = getPipelineTranslator(pipeline);
		return pipelineTranslator.translateToJSONExecutionPlan(pipeline);
	}

	private static FlinkPipelineTranslator getPipelineTranslator(Pipeline pipeline) {
		PlanTranslator planToJobGraphTransmogrifier = new PlanTranslator();

		if (planToJobGraphTransmogrifier.canTranslate(pipeline)) {
			return planToJobGraphTransmogrifier;
		}

		FlinkPipelineTranslator streamGraphTranslator = reflectStreamGraphTranslator();

		if (!streamGraphTranslator.canTranslate(pipeline)) {
			throw new RuntimeException("Translator " + streamGraphTranslator + " cannot translate "
					+ "the given pipeline " + pipeline + ".");
		}
		return streamGraphTranslator;
	}

	private static FlinkPipelineTranslator reflectStreamGraphTranslator() {
		// Try our luck with StreamGraph translation. We have to load a StreamGraphTranslator
		// via reflection because the dependencies of flink-streaming-java are inverted compared
		// to flink-java. For flink-java does not depend on runtime, clients or optimizer and
		// we have the translation code in clients/optimizer. On the other hand,
		// flink-streaming-java depends on runtime and clients.

		Class<?> streamGraphTranslatorClass;
		try {
			streamGraphTranslatorClass = Class.forName(
					"org.apache.flink.streaming.api.graph.StreamGraphTranslator",
					true,
					FlinkPipelineTranslationUtil.class.getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load StreamGraphTranslator.", e);
		}

		FlinkPipelineTranslator streamGraphTranslator;
		try {
			streamGraphTranslator =
					(FlinkPipelineTranslator) streamGraphTranslatorClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("Could not instantiate StreamGraphTranslator.", e);
		}
		return streamGraphTranslator;
	}
}
