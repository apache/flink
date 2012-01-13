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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;

public final class SWTFailurePatternsManager {

	private static final Log LOG = LogFactory.getLog(SWTFailurePatternsManager.class);

	private final Display display;

	private final ExtendedManagementProtocol jobManager;

	private Map<String, JobFailurePattern> failurePatterns = new HashMap<String, JobFailurePattern>();

	public SWTFailurePatternsManager(final Display display, final ExtendedManagementProtocol jobManager) {

		this.display = display;
		this.jobManager = jobManager;
	}

	public void startFailurePattern(final String jobName, final ManagementGraph managementGraph,
			final long referenceTime) {

		final JobFailurePattern failurePattern = this.failurePatterns.get(jobName);
		if (failurePattern == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No failure pattern for job " + jobName);
			}
			return;
		}

		LOG.info("Starting failure pattern for job " + jobName);

		new JobFailurePatternExecutor(this.display, this.jobManager, managementGraph, failurePattern, referenceTime);
	}

	public void openEditor(final Shell parent, final Set<String> jobSuggestions, final Set<String> nameSuggestions) {

		final SWTFailurePatternsEditor editor = new SWTFailurePatternsEditor(parent, jobSuggestions, nameSuggestions,
			this.failurePatterns);

		editor.show();
	}

	public static String getSuggestedName(final ManagementVertex vertex) {

		final String vertexName = (vertex.getName() != null) ? vertex.getName() : "null";

		return vertexName + " " + (vertex.getIndexInGroup() + 1);
	}
}
