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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nonnull;

/**
 * The entity responsible for executing a {@link Pipeline}, i.e. a user job.
 */
@Internal
public interface Executor {

	/**
	 * Executes a {@link Pipeline} based on the provided configuration.
	 *
	 * @param pipeline the {@link Pipeline} to execute
	 * @param configuration the {@link Configuration} with the required execution parameters
	 * @return the {@link JobExecutionResult} corresponding to the pipeline execution.
	 */
	JobExecutionResult execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) throws Exception;
}
