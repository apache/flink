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

package org.apache.flink.metrics;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * A metric definition class used by {@link AbstractMetrics}.
 */
public class MetricDef {
	private final Map<String, MetricInfo> definitions = new HashMap<>();
	private final Map<String, Set<String>> dependencies = new HashMap<>();

	// ---------------- public methods ----------------

	/**
	 * Define a metric using {@link MetricSpec}.
	 *
	 * @param name the name of the metric.
	 * @param doc the description of the metric.
	 * @param spec the {@link MetricSpec} of the metric.
	 * @return this MetricDef for chained invocation.
	 */
	public MetricDef define(String name, String doc, MetricSpec spec) {
		try {
			spec.validateMetricDef(this);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to define metric " + name);
		}

		spec.dependencies.forEach(dependency -> {
			dependencies.computeIfAbsent(dependency, dName -> new HashSet<>()).add(name);
			ensureNoCyclicDependency(name);
		});

		definitions.compute(name, (n, m) -> {
			if (m != null) {
				throw new IllegalStateException("Metric " + name + " is already defined for metric " + m);
			}
			return new MetricInfo(name, doc, spec);
		});

		return this;
	}

	/**
	 * Make a copy of this metric def.
	 *
	 * @return a copy of this metric def.
	 */
	public MetricDef copy() {
		MetricDef metricDef = new MetricDef();
		metricDef.definitions.putAll(this.definitions);
		metricDef.dependencies.putAll(this.dependencies);
		return metricDef;
	}

	/**
	 * Get a new metric def combining this metric def and the given metric def.
	 *
	 * @return a new metric def combining this metric def and the given metric def.
	 */
	public MetricDef combine(MetricDef other) {
		MetricDef combined = copy();
		other.definitions().values().forEach(info -> combined.define(info.name, info.doc, info.spec));
		return combined;
	}

	/**
	 * Get a new metric def excluding the metric.
	 *
	 * @param excludedMetrics the metrics to exclude.
	 * @return A new metric def that has excluded the given set of metrics.
	 */
	public MetricDef exclude(Set<String> excludedMetrics) {
		MetricDef afterExclusion = new MetricDef();
		for (MetricInfo info : definitions.values()) {
			if (!excludedMetrics.contains(info.name)) {
				afterExclusion.define(info.name, info.doc, info.spec);
			}
		}
		return afterExclusion;
	}

	// --------------- package private methods --------------------------

	/**
	 * @return the definitions of all the defined metrics.
	 */
	Map<String, MetricInfo> definitions() {
		return Collections.unmodifiableMap(definitions);
	}

	Map<String, Set<String>> dependencies() {
		return Collections.unmodifiableMap(dependencies);
	}

	// ----------------------------- package private class --------------------

	/**
	 * A container class hosting the definition of a metric.
	 */
	static class MetricInfo {
		final String name;
		final String doc;
		final MetricSpec spec;

		private MetricInfo(String name, String doc, MetricSpec spec) {
			this.name = name;
			this.doc = doc;
			this.spec = spec;
		}

		@Override
		public String toString() {
			return String.format("{name=%s,doc=\"%s\", spec=%s}", name, doc, spec);
		}
	}

	// ----------- private helpers --------------

	private void ensureNoCyclicDependency(String metricName) {
		Deque<String> dependencyChain = new ArrayDeque<>();
		dependencyChain.add(metricName);
		ensureNoCyclicDependency(dependencyChain, metricName);
	}

	private void ensureNoCyclicDependency(Deque<String> dependencyChain, String toFind) {
		String dependant = dependencyChain.peekLast();
		Set<String> dependencies = dependencies().getOrDefault(dependant, Collections.emptySet());
		// Cyclic dependency found.
		if (dependencies.contains(toFind)) {
			StringJoiner sj = new StringJoiner(" <- ", "[", "]");
			dependencyChain.forEach(sj::add);
			sj.add(toFind);
			throw new IllegalStateException("Cyclic metric dependency detected: " + sj.toString());
		}
		// Check next tier.
		dependencies.forEach(dependency -> {
			dependencyChain.addLast(dependency);
			ensureNoCyclicDependency(dependencyChain, toFind);
			dependencyChain.removeLast();
		});
	}
}
