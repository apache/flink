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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.metrics.MetricRegistry;

/**
 * Abstract {@link org.apache.flink.metrics.MetricGroup} for system components (e.g.,
 * TaskManager, Job, Task, Operator).
 *
 * <p>Usually, the scope of metrics is simply the hierarchy of the containing groups. For example
 * the Metric {@code "MyMetric"} in group {@code "B"} nested in group {@code "A"} would have a
 * fully scoped name of {@code "A.B.MyMetric"}, with {@code "A.B"} being the Metric's scope.
 *
 * <p>Component groups, however, have configurable scopes. This allow users to include or exclude
 * certain identifiers from the scope. The scope for metrics belonging to the "Task"
 * group could for example include the task attempt number (more fine grained identification), or
 * exclude it (for continuity of the namespace across failure and recovery).
 *
 * @param <P> The type of the parent MetricGroup.
 */
@Internal
public abstract class ComponentMetricGroup<P extends AbstractMetricGroup<?>> extends AbstractMetricGroup<P> {

	/**
	 * Creates a new ComponentMetricGroup.
	 *
	 * @param registry     registry to register new metrics with
	 * @param scope        the scope of the group
	 */
	public ComponentMetricGroup(MetricRegistry registry, String[] scope, P parent) {
		super(registry, scope, parent);
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
	//  Component Metric Group Specifics
	// ------------------------------------------------------------------------

	/**
	 * Gets all component metric groups that are contained in this component metric group.
	 *
	 * @return All component metric groups that are contained in this component metric group.
	 */
	protected abstract Iterable<? extends ComponentMetricGroup> subComponents();
}
