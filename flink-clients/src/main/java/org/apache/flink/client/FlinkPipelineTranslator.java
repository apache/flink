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
 * This can be used to turn a {@link Pipeline} into a {@link JobGraph}. There will be
 * implementations for the different pipeline APIs that Flink supports.
 */
public interface FlinkPipelineTranslator {

	/**
	 * Creates a {@link JobGraph} from the given {@link Pipeline} and attaches the given jar
	 * files and classpaths to the {@link JobGraph}.
	 */
	JobGraph translateToJobGraph(
			Pipeline pipeline,
			Configuration optimizerConfiguration,
			int defaultParallelism);


	/**
	 * Extracts the execution plan (as JSON) from the given {@link Pipeline}.
	 */
	String translateToJSONExecutionPlan(Pipeline pipeline);

	boolean canTranslate(Pipeline pipeline);
}
