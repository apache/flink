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
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.plugins.TaskManagerPlugin;

public final class ScoreTaskManagerPlugin implements TaskManagerPlugin {

	ScoreTaskManagerPlugin(final Configuration pluginConfiguration) {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerTask(final ExecutionVertexID id, final Configuration jobConfiguration,
			final RuntimeEnvironment environment) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterTask(final ExecutionVertexID id, final RuntimeEnvironment environment) {
		// TODO Auto-generated method stub

	}
}
