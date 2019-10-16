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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricScope;
import org.apache.flink.runtime.metrics.DelimiterProvider;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Default scope implementation. Contains additional methods assembling identifiers based on reporter-specific delimiters.
 */
@Internal
public class InternalMetricScope implements MetricScope {

	private final DelimiterProvider delimiterProvider;
	private final Supplier<Map<String, String>> variablesProvider;

	/**
	 * The map containing all variables and their associated values, lazily computed.
	 */
	protected volatile Map<String, String> variables;

	/**
	 * The metrics scope represented by this group.
	 * For example ["host-7", "taskmanager-2", "window_word_count", "my-mapper" ].
	 */
	private final String[] scopeComponents;

	/**
	 * Array containing the metrics scope represented by this group for each reporter, as a concatenated string, lazily computed.
	 * For example: "host-7.taskmanager-2.window_word_count.my-mapper"
	 */
	private final String[] cachedIdentifierScopes;

	public InternalMetricScope(DelimiterProvider delimiterProvider, String[] scopeComponents, Supplier<Map<String, String>> variablesProvider) {
		this.delimiterProvider = delimiterProvider;
		this.variablesProvider = variablesProvider;
		this.scopeComponents = scopeComponents;
		this.cachedIdentifierScopes = new String[delimiterProvider.getNumberReporters()];
	}

	@Override
	public Map<String, String> getAllVariables() {
		if (variables == null) { // avoid synchronization for common case
			synchronized (this) {
				if (variables == null) {
					variables = variablesProvider.get();
				}
			}
		}
		return variables;
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return getMetricIdentifier(metricName, s -> s, delimiterProvider.getDelimiter(), -1);
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return getMetricIdentifier(metricName, filter, delimiterProvider.getDelimiter(), -1);
	}

	@Internal
	public String getMetricIdentifier(String metricName, CharacterFilter filter, int reporterIndex) {
		return getMetricIdentifier(metricName, filter, delimiterProvider.getDelimiter(reporterIndex), reporterIndex);
	}

	private String getMetricIdentifier(String metricName, CharacterFilter filter, char delimiter, int reporterIndex) {
		return getIdentifierScope(filter, delimiter, reporterIndex) + delimiter + filter.filterCharacters(metricName);
	}

	private String getIdentifierScope(CharacterFilter filter, char delimiter, int reporterIndex) {
		if (cachedIdentifierScopes.length == 0 || (reporterIndex < 0 || reporterIndex >= cachedIdentifierScopes.length)) {
			return ScopeFormat.concat(filter, delimiter, scopeComponents);
		} else {
			if (cachedIdentifierScopes[reporterIndex] == null) {
				cachedIdentifierScopes[reporterIndex] = ScopeFormat.concat(filter, delimiter, scopeComponents);
			}
			return cachedIdentifierScopes[reporterIndex];
		}
	}

	public String[] geScopeComponents() {
		return scopeComponents;
	}
}
