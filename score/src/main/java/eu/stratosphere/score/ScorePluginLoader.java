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

package eu.stratosphere.score;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.plugins.AbstractPluginLoader;
import eu.stratosphere.nephele.plugins.JobManagerPlugin;
import eu.stratosphere.nephele.plugins.PluginID;
import eu.stratosphere.nephele.plugins.PluginLookupService;
import eu.stratosphere.nephele.plugins.TaskManagerPlugin;

/**
 * A plugin loader for the SCORE (Stratosphere Continuous Re-optimization) module.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ScorePluginLoader extends AbstractPluginLoader {

	private ScoreJobManagerPlugin jobManagerPlugin = null;

	private ScoreTaskManagerPlugin taskManagerPlugin = null;

	private final PluginID pluginID;

	public ScorePluginLoader(final String pluginName, final Configuration pluginConfiguration,
			final PluginLookupService pluginLookupService) {

		super(pluginName, pluginConfiguration, pluginLookupService);

		this.pluginID = PluginID.fromByteArray(new byte[] { 0x44, -0x6f, 0x18, -0x1d, 0x00, 0x4a, 0x60, -0x61, -0x25,
			0x00, 0x00, 0x16, 0x00, 0x18, -0x01, 0x62 });
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized JobManagerPlugin getJobManagerPlugin() {

		if (this.jobManagerPlugin == null) {
			this.jobManagerPlugin = new ScoreJobManagerPlugin();
		}

		return this.jobManagerPlugin;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized TaskManagerPlugin getTaskManagerPlugin() {

		if (this.taskManagerPlugin == null) {
			this.taskManagerPlugin = new ScoreTaskManagerPlugin(getPluginConfiguration());
		}

		return this.taskManagerPlugin;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public PluginID getPluginID() {

		return this.pluginID;
	}
}
