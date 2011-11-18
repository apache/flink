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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.InputGateListener;
import eu.stratosphere.nephele.io.OutputGateListener;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class StreamingTaskListener implements InputGateListener, OutputGateListener {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamingTaskListener.class);

	private static enum TaskType {
		INPUT, REGULAR, OUTPUT
	};

	private final static double ALPHA = 0.5;

	private final StreamingCommunicationThread communicationThread;

	private final JobID jobID;

	private final ExecutionVertexID vertexID;

	private final TaskType taskType;

	private final int taggingInterval;

	private final int aggregationInterval;

	private StreamingTag tag = null;

	private int tagCounter = 0;

	private Map<ExecutionVertexID, Integer> aggregationCounter = new HashMap<ExecutionVertexID, Integer>();

	private Map<ExecutionVertexID, Double> aggregatedValue = new HashMap<ExecutionVertexID, Double>();

	static StreamingTaskListener createForInputTask(final StreamingCommunicationThread communicationThread,
			final JobID jobID, final ExecutionVertexID vertexID, final int taggingInterval,
			final int aggregationInterval) {

		return new StreamingTaskListener(communicationThread, jobID, vertexID, TaskType.INPUT, taggingInterval,
			aggregationInterval);
	}

	static StreamingTaskListener createForRegularTask(final StreamingCommunicationThread communicationThread,
			final JobID jobID, final ExecutionVertexID vertexID, final int aggregationInterval) {

		return new StreamingTaskListener(communicationThread, jobID, vertexID, TaskType.REGULAR, 0, aggregationInterval);
	}

	static StreamingTaskListener createForOutputTask(final StreamingCommunicationThread communicationThread,
			final JobID jobID, final ExecutionVertexID vertexID, final int aggregationInterval) {

		return new StreamingTaskListener(communicationThread, jobID, vertexID, TaskType.OUTPUT, 0, aggregationInterval);
	}

	private StreamingTaskListener(final StreamingCommunicationThread communicationThread, final JobID jobID,
			final ExecutionVertexID vertexID, final TaskType taskType, final int taggingInterval,
			final int aggregationInterval) {

		this.communicationThread = communicationThread;
		this.jobID = jobID;
		this.vertexID = vertexID;
		this.taskType = taskType;
		this.taggingInterval = taggingInterval;
		this.aggregationInterval = aggregationInterval;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void channelCapacityExhausted(final int channelIndex) {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void recordEmitted(final Record record) {

		switch (this.taskType) {
		case INPUT:
			if (this.tagCounter++ == this.taggingInterval) {
				final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
				taggableRecord.setTag(createTag());
				this.tagCounter = 0;
			}
			break;
		case REGULAR:
			final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			taggableRecord.setTag(this.tag);
			break;
		case OUTPUT:
			throw new IllegalStateException("Output task emitted record");
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void waitingForAnyChannel() {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void recordReceived(final Record record) {

		if (this.taskType == TaskType.INPUT) {
			throw new IllegalStateException("Input task received record");
		}

		final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
		this.tag = (StreamingTag) taggableRecord.getTag();
		if (this.tag != null) {

			final long pathLatency = System.currentTimeMillis() - this.tag.getTimestamp();

			final ExecutionVertexID sourceID = this.tag.getSourceID();

			// Calculate moving average
			Double aggregatedLatency = this.aggregatedValue.get(sourceID);
			if (aggregatedLatency == null) {
				aggregatedLatency = Double.valueOf(pathLatency);
			} else {
				aggregatedLatency = Double.valueOf((ALPHA * pathLatency)
					+ ((1 - ALPHA) * aggregatedLatency.doubleValue()));
			}
			this.aggregatedValue.put(sourceID, aggregatedLatency);

			// Check if we need to compute an event and send it to the job manager component
			Integer counter = this.aggregationCounter.get(sourceID);
			if (counter == null) {
				counter = Integer.valueOf(0);
			}

			counter = Integer.valueOf(counter.intValue() + 1);
			if (counter.intValue() == this.aggregationInterval) {

				final PathLatency pl = new PathLatency(this.jobID, sourceID, this.vertexID,
					aggregatedLatency.doubleValue());

				try {
					this.communicationThread.sendDataAsynchronously(pl);
				} catch (InterruptedException e) {
					LOG.warn(StringUtils.stringifyException(e));
				}

				counter = Integer.valueOf(0);
			}
			this.aggregationCounter.put(sourceID, counter);
		}

	}

	private StreamingTag createTag() {

		if (this.tag == null) {
			this.tag = new StreamingTag(this.vertexID);
		}

		this.tag.setTimestamp(System.currentTimeMillis());

		return this.tag;
	}

}
