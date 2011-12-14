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

package eu.stratosphere.nephele.visualization.swt;

import org.eclipse.swt.widgets.Display;

import eu.stratosphere.nephele.jobgraph.JobID;

public final class JobFailurePatternExecutor implements Runnable {

	private final Display timer;

	private final JobID jobID;

	private final String jobName;

	private long offset = 0L;

	private boolean stopRequested = false;

	private boolean executorStarted = false;

	JobFailurePatternExecutor(final Display timer, final JobID jobID, final String jobName,
			final JobFailurePattern failurePattern) {

		this.timer = timer;
		this.jobID = jobID;
		this.jobName = jobName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		if (this.stopRequested) {
			this.stopRequested = false;
			return;
		}

		scheduleNextEvent();
	}

	public void start(final long referenceTime) {

		if (this.executorStarted) {
			throw new IllegalStateException("The executor has already been started");
		}

		final long now = System.currentTimeMillis();

		this.executorStarted = true;
		this.offset = now - referenceTime;

		scheduleNextEvent();
	}

	public void stop() {

		this.stopRequested = true;
		this.executorStarted = false;
	}

	private void scheduleNextEvent() {

		// TODO: Implement me
	}
}
