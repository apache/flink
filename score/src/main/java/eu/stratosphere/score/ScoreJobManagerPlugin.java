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

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.JobManagerPlugin;
import eu.stratosphere.nephele.profiling.ProfilingListener;

public final class ScoreJobManagerPlugin implements JobManagerPlugin {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobGraph rewriteJobGraph(final JobGraph jobGraph) {

		// Nothing to do here

		return jobGraph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionGraph rewriteExecutionGraph(final ExecutionGraph executionGraph) {

		synchronized (executionGraph) {

			// Register for events
			final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(executionGraph, true);

			while (it.hasNext()) {
				final ExecutionVertex vertex = it.next();
				vertex.registerExecutionListener(new ScoreExecutionListener(vertex));
			}
		}

		return executionGraph;
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
	public void sendData(final IOReadableWritable data) throws IOException {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOReadableWritable requestData(final IOReadableWritable data) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean requiresProfiling() {

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ProfilingListener getProfilingListener(final JobID jobID) {

		return null;
	}
}
