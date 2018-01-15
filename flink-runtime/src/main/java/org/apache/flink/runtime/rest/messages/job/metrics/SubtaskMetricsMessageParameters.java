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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.handler.job.metrics.SubtaskMetricsHandler;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskMessageParameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * {@link MessageParameters} for {@link SubtaskMetricsHandler}.
 */
public class SubtaskMetricsMessageParameters extends SubtaskMessageParameters {

	private final MetricsFilterParameter metricsFilterParameter = new MetricsFilterParameter();

	@Override
	public Collection<MessagePathParameter<?>> getPathParameters() {
		return Collections.unmodifiableCollection(Arrays.asList(
			jobPathParameter,
			jobVertexIdPathParameter,
			subtaskIndexPathParameter
		));
	}

	@Override
	public Collection<MessageQueryParameter<?>> getQueryParameters() {
		return Collections.singletonList(metricsFilterParameter);
	}

}
