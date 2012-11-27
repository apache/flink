/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.checkpointing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

public final class CheckpointDecision {

	private static final Log LOG = LogFactory.getLog(CheckpointDecision.class);

	public static boolean getDecision(final RuntimeTask task) {

		switch (CheckpointUtils.getCheckpointMode()) {
		case NEVER:
			return false;
		case ALWAYS:
			return true;
		case NETWORK:
			return isNetworkTask(task);
		}

		return false;
	}

	private static boolean isNetworkTask(final RuntimeTask task) {

		final RuntimeEnvironment environment = task.getRuntimeEnvironment();

		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {

			if (environment.getOutputGate(i).getChannelType() == ChannelType.NETWORK) {
				LOG.info(environment.getTaskNameWithIndex() + " is a network task");
				return true;
			}
		}

		return false;
	}
}
