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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.PluginCommunication;
import eu.stratosphere.nephele.plugins.TaskManagerPlugin;
import eu.stratosphere.nephele.types.Record;

public class StreamingTaskManagerPlugin implements TaskManagerPlugin {

	/**
	 * Provides access to the configuration entry which defines the interval in which records shall be tagged.
	 */
	private static final String TAGGING_INTERVAL_KEY = "streaming.tagging.interval";

	/**
	 * The default tagging interval.
	 */
	private static final int DEFAULT_TAGGING_INTERVAL = 10;

	/**
	 * Provides access to the configuration entry which defines the interval in which received tags shall be aggregated
	 * and sent to the job manager plugin component.
	 */
	private static final String AGGREGATION_INTERVAL_KEY = "streaming.aggregation.interval";

	/**
	 * The default aggregation interval.
	 */
	private static final int DEFAULT_AGGREGATION_INTERVAL = 10;

	/**
	 * The tagging interval as specified in the plugin configuration.
	 */
	private final int taggingInterval;

	/**
	 * The aggregation interval as specified in the plugin configuration.
	 */
	private final int aggregationInterval;

	/**
	 * A special thread to asynchronously send data to the job manager component without suffering from the RPC latency.
	 */
	private final StreamingCommunicationThread communicationThread;

	StreamingTaskManagerPlugin(final Configuration pluginConfiguration, final PluginCommunication jobManagerComponent) {

		this.taggingInterval = pluginConfiguration.getInteger(TAGGING_INTERVAL_KEY, DEFAULT_TAGGING_INTERVAL);
		this.aggregationInterval = pluginConfiguration.getInteger(AGGREGATION_INTERVAL_KEY,
			DEFAULT_AGGREGATION_INTERVAL);

		this.communicationThread = new StreamingCommunicationThread(jobManagerComponent);
		this.communicationThread.start();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {

		this.communicationThread.stopCommunicationThread();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerTask(final ExecutionVertexID id, final Configuration jobConfiguration,
			final RuntimeEnvironment environment) {

		// Check if user has provided a job-specific aggregation interval
		final int aggregationInterval = jobConfiguration.getInteger(AGGREGATION_INTERVAL_KEY,
			this.aggregationInterval);

		StreamingTaskListener listener = null;
		final JobID jobID = environment.getJobID();

		if (environment.getNumberOfInputGates() == 0) {
			// Check if user has provided a job-specific tagging interval
			final int taggingInterval = jobConfiguration.getInteger(TAGGING_INTERVAL_KEY, this.taggingInterval);

			listener = StreamingTaskListener.createForInputTask(this.communicationThread, jobID, id, taggingInterval,
				aggregationInterval);
		} else if (environment.getNumberOfOutputGates() == 0) {
			listener = StreamingTaskListener.createForOutputTask(this.communicationThread, jobID, id,
				aggregationInterval);
		} else {
			listener = StreamingTaskListener.createForRegularTask(this.communicationThread, jobID, id,
				aggregationInterval);
		}

		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			final OutputGate<? extends Record> outputGate = environment.getOutputGate(i);
			outputGate.registerOutputGateListener(listener);
		}

		for (int i = 0; i < environment.getNumberOfInputGates(); ++i) {
			final InputGate<? extends Record> inputGate = environment.getInputGate(i);
			inputGate.registerInputGateListener(listener);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterTask(final ExecutionVertexID id, final RuntimeEnvironment environment) {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendData(final IOReadableWritable data) throws IOException {

		// TODO Implement me
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOReadableWritable requestData(final IOReadableWritable data) throws IOException {

		// TODO Implement me

		return null;
	}

}
