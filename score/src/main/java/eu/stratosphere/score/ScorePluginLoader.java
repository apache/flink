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

	public ScorePluginLoader(final Configuration pluginConfiguration) {
		super(pluginConfiguration);
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
}
