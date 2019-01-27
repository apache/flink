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

package org.apache.flink.runtime.schedule;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.ScheduleMode;

import java.io.Serializable;

/**
 * This factory is for creating {@link GraphManagerPlugin} instances.
 */
public class GraphManagerPluginFactory implements Serializable {
	private static final long serialVersionUID = 7967476136812239100L;

	/**
	 * Creates a {@link GraphManagerPlugin} instance from the given configuration.
	 *
	 * @param configuration job configurations
	 * @param classLoader user class loader which may be used to create custom plugin
	 * @return GraphManagerPlugin instance
	 */
	public static GraphManagerPlugin createGraphManagerPlugin(
		final Configuration configuration,
		final ClassLoader classLoader) {

		GraphManagerPlugin graphManagerPlugin;

		String graphManagerPluginClassName = configuration.getString(JobManagerOptions.GRAPH_MANAGER_PLUGIN);
		if (graphManagerPluginClassName == null) {
			// if no given plugin, use default plugin regarding to schedule mode
			ScheduleMode scheduleMode = ScheduleMode.valueOf(
				configuration.getString(ScheduleMode.class.getName(), ScheduleMode.LAZY_FROM_SOURCES.toString()));
			switch (scheduleMode) {
				case EAGER:
					graphManagerPlugin = new EagerSchedulingPlugin();
					break;
				case LAZY_FROM_SOURCES:
					graphManagerPlugin = new StepwiseSchedulingPlugin();
					break;
				default:
					throw new IllegalArgumentException("Unknown schedule mode: " + scheduleMode);
			}
		} else {
			try {
				@SuppressWarnings("rawtypes")
				Class<? extends GraphManagerPlugin> clazz =
					Class.forName(graphManagerPluginClassName, false, classLoader)
						.asSubclass(GraphManagerPlugin.class);

				graphManagerPlugin = clazz.newInstance();
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(
					"Cannot find configured graph manager plugin class: " + graphManagerPluginClassName, e);
			} catch (ClassCastException | InstantiationException | IllegalAccessException e) {
				throw new IllegalArgumentException("The class configured under '" +
					JobManagerOptions.GRAPH_MANAGER_PLUGIN.key() + "' is not a valid graph manager plugin(" +
					graphManagerPluginClassName + ')', e);
			}
		}

		return graphManagerPlugin;
	}
}
