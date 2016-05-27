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

package org.apache.flink.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.metrics.groups.Scope.SCOPE_WILDCARD;

/**
 * Abstract {@link org.apache.flink.metrics.MetricGroup} for system components (e.g., 
 * TaskManager, Job, Task, Operator).
 * 
 * <p>The components metric groups contain functionality to define alias names for
 * the component identifier. For example, while Tasks are registered under a Task Attempt ID,
 * the metrics name scope may use the task name instead. Using these aliases makes
 * Metric scope names stable across jobs, recovery attempts, etc.
 */
@Internal
public abstract class ComponentMetricGroup extends AbstractMetricGroup {

	private final ComponentMetricGroup parent;

	private final String format;
	
	// Map: scope variable -> specific value
	protected final Map<String, String> formats;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new ComponentMetricGroup.
	 *
	 * @param registry    registry to register new metrics with
	 * @param parentGroup parent group, may be null
	 * @param scopeFormat default format string
	 */
	public ComponentMetricGroup(MetricRegistry registry, ComponentMetricGroup parentGroup, String scopeFormat) {
		super(registry);
		this.formats = new HashMap<>();
		this.parent = parentGroup;
		this.format = scopeFormat;
	}

	/**
	 * Closes the component group by removing and closing all metrics and subgroups
	 * (inherited from {@link AbstractMetricGroup}), plus closing and removing all dedicated
	 * component subgroups.
	 */
	@Override
	public void close() {
		synchronized (this) {
			if (!isClosed()) {
				// remove all metrics and generic subgroups
				super.close();

				// remove and close all subcomponent metrics
				for (ComponentMetricGroup group : subComponents()) {
					group.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  sub components
	// ------------------------------------------------------------------------

	protected ComponentMetricGroup parent() {
		return parent;
	}

	protected abstract Iterable<? extends ComponentMetricGroup> subComponents();

	// ------------------------------------------------------------------------
	//  scope format
	// ------------------------------------------------------------------------

	protected abstract String getScopeFormat(Scope.ScopeFormat format);

	@Override
	public List<String> generateScope() {
		return generateScope(format);
	}

	@Override
	public List<String> generateScope(Scope.ScopeFormat format) {
		return generateScope(getScopeFormat(format));
	}

	private List<String> generateScope(String format) {
		String[] components = Scope.split(format);

		List<String> scope = new ArrayList<>();
		if (components[0].equals(SCOPE_WILDCARD)) {
			if (this.parent != null) {
				scope = this.parent.generateScope();
			}
			this.replaceFormats(components);
			addToList(scope, components, 1);
		} else {
			if (this.parent != null) {
				this.parent.replaceFormats(components);
			}
			this.replaceFormats(components);
			addToList(scope, components, 0);
		}
		return scope;
	}

	private void replaceFormats(String[] components) {
		if (this.parent != null) {
			this.parent.replaceFormats(components);
		}
		for (int x = 0; x < components.length; x++) {
			if (components[x].startsWith("<")) {
				if (this.formats.containsKey(components[x])) {
					components[x] = this.formats.get(components[x]);
				}
			}
		}
	}

	/**
	 * Adds all elements from the given array, starting from the given index, to the given list.
	 *
	 * @param list       destination
	 * @param array      source
	 * @param startIndex array index to start from
	 */
	private static void addToList(List<String> list, String[] array, int startIndex) {
		for (int x = startIndex; x < array.length; x++) {
			list.add(array[x]);
		}
	}
}
