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

package eu.stratosphere.nephele.checkpointing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

public final class CheckpointDecision {

	private static final Log LOG = LogFactory.getLog(CheckpointDecision.class);

	public static boolean getDecision(final RuntimeTask task, final ResourceUtilizationSnapshot rus) {

		switch (CheckpointUtils.getCheckpointMode()) {
		case NEVER:
			return false;
		case ALWAYS:
			return true;
		case NETWORK:
			return isNetworkTask(task);
		}

		if (rus.getForced() != null) {
			LOG.info("Checkpoint decision was forced to " + rus.getForced());
			// checkpoint decision was forced by the user
			return rus.getForced();
		}

		final double CPlower = CheckpointUtils.getCPLower();

		final double CPupper = CheckpointUtils.getCPUpper();

		if (rus.getPactRatio() >= 0.0 && CheckpointUtils.usePACT()) {
			LOG.info("Ratio = " + rus.getPactRatio());
			if (rus.getPactRatio() >= CPlower) {
				// amount of data is small so we checkpoint
				return true;
			}
			if (rus.getPactRatio() <= CPupper) {
				// amount of data is too big
				return false;
			}
		} else {
			// no info from upper layer so use average sizes
			if (rus.isDam()) {
				LOG.info("is Dam");

				if (rus.getAverageOutputRecordSize() != 0) {
					LOG.info("avg ratio " + rus.getAverageInputRecordSize()
						/ rus.getAverageOutputRecordSize());
				}

				if (rus.getAverageOutputRecordSize() != 0 &&
						rus.getAverageInputRecordSize() / rus.getAverageOutputRecordSize() >= CPlower) {
					return true;
				}

				if (rus.getAverageOutputRecordSize() != 0 &&
						rus.getAverageInputRecordSize() / rus.getAverageOutputRecordSize() <= CPupper) {
					return false;
				}
			} else {

				// we have no data dam so we can estimate the input/output-ratio
				LOG.info("out " + rus.getTotalOutputAmount() + " in " + rus.getTotalInputAmount());
				if (rus.getTotalInputAmount() != 0) {
					LOG.info("Selectivity is " + (double) rus.getTotalOutputAmount()
						/ rus.getTotalInputAmount());

				}
				if (rus.getTotalOutputAmount() != 0
					&& ((double) rus.getTotalInputAmount() / rus.getTotalOutputAmount() >= CPlower)) {
					// size off checkpoint would be to large: do not checkpoint
					// TODO progress estimation would make sense here
					LOG.info(task.getEnvironment().getTaskName() + " Checkpoint too large selectivity "
						+ ((double) rus.getTotalInputAmount() / rus.getTotalOutputAmount()));
					return false;

				}
				if (rus.getTotalOutputAmount() != 0
					&& ((double) rus.getTotalInputAmount() / rus.getTotalOutputAmount() <= CPupper)) {
					// size of checkpoint will be small enough: checkpoint
					// TODO progress estimation would make sense here
					LOG.info(task.getEnvironment().getTaskName() + " Checkpoint small selectivity "
						+ ((double) rus.getTotalInputAmount() / rus.getTotalOutputAmount()));
					return true;

				}

			}
		}
		// between thresholds check CPU Usage.
		if (rus.getUserCPU() >= 90) {
			LOG.info(task.getEnvironment().getTaskName() + "CPU-Bottleneck");
			// CPU bottleneck
			return true;
		}

		LOG.info("Checkpoint decision false by default");
		// in case of doubt do not checkpoint
		return false;

	}

	private static boolean isNetworkTask(final RuntimeTask task) {

		final RuntimeEnvironment environment = task.getRuntimeEnvironment();

		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {

			if (environment.getOutputGate(i).getChannelType() == ChannelType.NETWORK) {
				return true;
			}
		}

		return false;
	}
}
