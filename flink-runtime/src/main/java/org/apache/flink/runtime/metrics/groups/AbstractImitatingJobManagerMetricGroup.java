/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.Map;

/**
 * Base class which imitates the reporting of the existing {@link JobManagerMetricGroup} in order
 * to guarantee backwards compatibility of metrics which are factored out of the {@link JobManagerMetricGroup}.
 */
class AbstractImitatingJobManagerMetricGroup extends AbstractMetricGroup<AbstractMetricGroup<?>> {
	protected final String hostname;

	AbstractImitatingJobManagerMetricGroup(MetricRegistry registry, String hostname) {
		super(registry, getScope(registry, hostname), null);
		this.hostname = hostname;
	}

	private static String[] getScope(MetricRegistry registry, String hostname) {
		// returning jobmanager scope in order to guarantee backwards compatibility
		// this can be changed once we introduce a proper scope for the process metric group
		return registry.getScopeFormats().getJobManagerFormat().formatScope(hostname);
	}

	@Override
	protected final String getGroupName(CharacterFilter filter) {
		// returning jobmanager in order to guarantee backwards compatibility
		// this can be changed once we introduce a proper group name for the process metric group
		return "jobmanager";
	}

	@Override
	protected final void putVariables(Map<String, String> variables) {
		variables.put(ScopeFormat.SCOPE_HOST, hostname);
	}

	@Override
	protected final QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
		return new QueryScopeInfo.JobManagerQueryScopeInfo();
	}
}
