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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.TableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A {@link FunctionContext} for constant expression reduction. It is used when a function is called
 * with constant expressions or constant expressions can be derived from the given statement.
 *
 * <p>Since constant expression reduction happens during planning, methods that reference Flink's runtime
 * context are not available.
 *
 * @see FunctionDefinition#isDeterministic()
 */
@Internal
public final class ConstantFunctionContext extends FunctionContext {

	private static final Logger LOG = LoggerFactory.getLogger(ConstantFunctionContext.class);

	private static final UnregisteredMetricsGroup metricsGroup = new UnregisteredMetricsGroup();

	private final Map<String, String> jobParameters;

	public ConstantFunctionContext(Configuration configuration) {
		super(null);
		this.jobParameters = configuration.getOptional(PipelineOptions.GLOBAL_JOB_PARAMETERS)
			.map(HashMap::new)
			.orElseGet(HashMap::new);
	}

	@Override
	public MetricGroup getMetricGroup() {
		LOG.warn(
			"Calls to FunctionContext.getMetricGroup will have no effect during constant expression reduction.");
		return metricsGroup;
	}

	@Override
	public File getCachedFile(String name) {
		throw new TableException(
			"Calls to FunctionContext.getCachedFile are not available during constant expression reduction.");
	}

	@Override
	public String getJobParameter(String key, String defaultValue) {
		return jobParameters.getOrDefault(key, defaultValue);
	}

	@Override
	public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
		throw new TableException(
			"Calls to FunctionContext.getExternalResourceInfos are not available during constant expression reduction.");
	}
}
