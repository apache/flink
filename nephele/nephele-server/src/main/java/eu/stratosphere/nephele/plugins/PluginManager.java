/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.plugins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class PluginManager {

	/**
	 * The log object used to report errors and information in general.
	 */
	private static final Log LOG = LogFactory.getLog(PluginManager.class);

	/**
	 * The name of the file containing the plugin configuration.
	 */
	private static final String PLUGIN_CONFIG_FILE = "nephele-plugins.xml";

	/**
	 * The singleton instance of this class.
	 */
	private static PluginManager INSTANCE = null;

	private final Map<String, AbstractPluginLoader> plugins;

	private PluginManager(final String configDir) {

		this.plugins = loadPlugins();
	}

	private Map<String, AbstractPluginLoader> loadPlugins() {

		final Map<String, AbstractPluginLoader> tmpPluginList = new LinkedHashMap<String, AbstractPluginLoader>();

		return Collections.unmodifiableMap(tmpPluginList);
	}

	private static synchronized PluginManager getInstance(final String configDir) {

		if (INSTANCE == null) {
			INSTANCE = new PluginManager(configDir);
		}

		return INSTANCE;
	}

	private List<JobManagerPlugin> getJobManagerPluginsInternal() {

		final List<JobManagerPlugin> jobManagerPluginList = new ArrayList<JobManagerPlugin>();

		final Iterator<AbstractPluginLoader> it = this.plugins.values().iterator();
		while (it.hasNext()) {

			final JobManagerPlugin jmp = it.next().getJobManagerPlugin();
			if (jmp != null) {
				jobManagerPluginList.add(jmp);
			}
		}

		return Collections.unmodifiableList(jobManagerPluginList);
	}

	private List<TaskManagerPlugin> getTaskManagerPluginsInternal() {

		final List<TaskManagerPlugin> taskManagerPluginList = new ArrayList<TaskManagerPlugin>();

		final Iterator<AbstractPluginLoader> it = this.plugins.values().iterator();
		while (it.hasNext()) {

			final TaskManagerPlugin jmp = it.next().getTaskManagerPlugin();
			if (jmp != null) {
				taskManagerPluginList.add(jmp);
			}
		}

		return Collections.unmodifiableList(taskManagerPluginList);
	}

	public static List<JobManagerPlugin> getJobManagerPlugins(final String configDir) {

		return getInstance(configDir).getJobManagerPluginsInternal();
	}

	public static List<TaskManagerPlugin> getTaskManagerPlugins(final String configDir) {

		return getInstance(configDir).getTaskManagerPluginsInternal();
	}
}
