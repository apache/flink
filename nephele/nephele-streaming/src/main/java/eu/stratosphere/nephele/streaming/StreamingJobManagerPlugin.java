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

package eu.stratosphere.nephele.streaming;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.plugins.JobManagerPlugin;
import eu.stratosphere.nephele.streaming.wrapper.StreamingTask;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;

public class StreamingJobManagerPlugin implements JobManagerPlugin {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamingJobManagerPlugin.class);

	StreamingJobManagerPlugin(final Configuration pluginConfiguration) {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobGraph rewriteJobGraph(final JobGraph jobGraph) {

		// Rewrite input vertices
		/*final Iterator<AbstractJobInputVertex> inputIt = jobGraph.getInputVertices();

		final Iterator<JobTaskVertex> taskIt = jobGraph.getTaskVertices();
		while (taskIt.hasNext()) {

			final JobTaskVertex taskVertex = taskIt.next();

			final Class<? extends AbstractInvokable> originalClass = taskVertex.getInvokableClass();

			taskVertex.setTaskClass(StreamingTask.class);
			taskVertex.getConfiguration().setString("origClass", originalClass.getName());
		}*/

		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionGraph rewriteExecutionGraph(final ExecutionGraph executionGraph) {
		// TODO Auto-generated method stub
		return null;
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

		if (!(data instanceof StreamingData)) {
			LOG.error("Received unexpected data of type " + data);
			return;
		}

		System.out.println(data);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOReadableWritable requestData(final IOReadableWritable data) throws IOException {

		if (!(data instanceof StreamingData)) {
			LOG.error("Received unexpected data of type " + data);
			return null;
		}

		return null;
	}
}
