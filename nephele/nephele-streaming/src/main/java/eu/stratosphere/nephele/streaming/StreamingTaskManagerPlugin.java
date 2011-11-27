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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.PluginCommunication;
import eu.stratosphere.nephele.plugins.TaskManagerPlugin;
import eu.stratosphere.nephele.streaming.actions.AbstractAction;
import eu.stratosphere.nephele.streaming.listeners.StreamListenerContext;

public class StreamingTaskManagerPlugin implements TaskManagerPlugin {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamingTaskManagerPlugin.class);

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
	 * Stores the instance of the streaming task manager plugin.
	 */
	private static volatile StreamingTaskManagerPlugin INSTANCE = null;

	/**
	 * Map storing the listener context objects for the individual stream listners.
	 */
	private final ConcurrentMap<String, StreamListenerContext> listenerContexts = new ConcurrentHashMap<String, StreamListenerContext>();

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

		LOG.info("Configured tagging interval is " + this.taggingInterval);

		INSTANCE = this;
	}

	public static StreamListenerContext getStreamingListenerContext(final String listenerKey) {

		if (INSTANCE == null) {
			throw new IllegalStateException("StreamingTaskManagerPlugin has not been initialized");
		}

		return INSTANCE.listenerContexts.get(listenerKey);
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

		final int taggingInterval = jobConfiguration.getInteger(TAGGING_INTERVAL_KEY, this.taggingInterval);

		final String idAsString = id.toString();

		environment.getTaskConfiguration().setString(StreamListenerContext.CONTEXT_CONFIGURATION_KEY, idAsString);

		final JobID jobID = environment.getJobID();
		StreamListenerContext listenerContext = null;
		if (environment.getNumberOfInputGates() == 0) {
			listenerContext = StreamListenerContext.createForInputTask(jobID, id, this.communicationThread,
				aggregationInterval, taggingInterval);
		} else if (environment.getNumberOfOutputGates() == 0) {
			listenerContext = StreamListenerContext.createForOutputTask(jobID, id, this.communicationThread,
				aggregationInterval);
		} else {
			listenerContext = StreamListenerContext.createForRegularTask(jobID, id, this.communicationThread,
				aggregationInterval);
		}

		this.listenerContexts.putIfAbsent(idAsString, listenerContext);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterTask(final ExecutionVertexID id, final RuntimeEnvironment environment) {

		this.listenerContexts.remove(id.toString());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendData(final IOReadableWritable data) throws IOException {

		if (!(data instanceof AbstractAction)) {
			LOG.error("Received data is of unknown type " + data.getClass());
			return;
		}

		final AbstractAction action = (AbstractAction) data;
		final StreamListenerContext listenerContext = this.listenerContexts.get(action.getVertexID().toString());

		if (listenerContext == null) {
			LOG.error("Cannot find listener context for vertex with ID " + action.getVertexID());
			return;
		}

		// Queue the action and return
		listenerContext.queuePendingAction(action);
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
