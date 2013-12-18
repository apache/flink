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

package eu.stratosphere.addons.visualization.swt;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.widgets.Display;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.util.StringUtils;

public final class JobFailurePatternExecutor implements Runnable {

	private static final Log LOG = LogFactory.getLog(JobFailurePatternExecutor.class);

	private final Display timer;

	private final JobID jobID;

	private final ExtendedManagementProtocol jobManager;

	private final Map<String, ManagementVertexID> nameToIDMap;

	private final Iterator<AbstractFailureEvent> eventIterator;

	private final int offset;

	private AbstractFailureEvent nextEvent = null;

	private boolean stopRequested = false;

	JobFailurePatternExecutor(final Display timer, final ExtendedManagementProtocol jobManager,
			final ManagementGraph managementGraph, final JobFailurePattern failurePattern, final long referenceTime) {

		this.timer = timer;
		this.jobManager = jobManager;
		this.jobID = managementGraph.getJobID();

		final long now = System.currentTimeMillis();

		this.offset = (int) (now - referenceTime);

		final Map<String, ManagementVertexID> tmpMap = new HashMap<String, ManagementVertexID>();
		final Iterator<ManagementVertex> it = new ManagementGraphIterator(managementGraph, true);
		while (it.hasNext()) {

			final ManagementVertex vertex = it.next();
			tmpMap.put(SWTFailurePatternsManager.getSuggestedName(vertex), vertex.getID());
		}

		this.nameToIDMap = Collections.unmodifiableMap(tmpMap);

		this.eventIterator = failurePattern.iterator();

		scheduleNextEvent();
	}


	@Override
	public void run() {

		if (this.stopRequested) {
			LOG.info("Stopping job failure pattern executor for job " + this.jobID);
			this.stopRequested = false;
			return;
		}

		LOG.info("Triggering event " + this.nextEvent.getName() + " for job " + this.jobID);
		if (this.nextEvent instanceof VertexFailureEvent) {

			// Find out the ID of the vertex to be killed
			final ManagementVertexID vertexID = this.nameToIDMap.get(this.nextEvent.getName());
			if (vertexID == null) {
				LOG.error("Cannot determine ID for vertex " + this.nextEvent.getName());
			} else {
				try {
					this.jobManager.killTask(this.jobID, vertexID);
				} catch (IOException ioe) {
					LOG.error(StringUtils.stringifyException(ioe));
				}
			}

		} else {
			try {
				this.jobManager.killInstance(new StringRecord(this.nextEvent.getName()));
			} catch (IOException ioe) {
				LOG.error(StringUtils.stringifyException(ioe));
			}
		}

		// Schedule next event
		scheduleNextEvent();
	}

	public void stop() {

		this.stopRequested = true;
	}

	private void scheduleNextEvent() {

		if (this.eventIterator.hasNext()) {
			this.nextEvent = this.eventIterator.next();
		} else {
			this.nextEvent = null;
			return;
		}

		final int interval = this.nextEvent.getInterval() - this.offset;

		this.timer.timerExec(interval, this);
	}
}
