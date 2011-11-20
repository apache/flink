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

	private long lastTimestamp = -1L;

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
	public void recordEmitted(final Record record) {

		switch (this.taskType) {
		case INPUT:
			if (this.tagCounter++ == this.taggingInterval) {
				final long timestamp = System.currentTimeMillis();
				final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
				taggableRecord.setTag(createTag(timestamp));
				if (this.lastTimestamp > 0) {
					final long taskLatency = (timestamp - this.lastTimestamp) / this.taggingInterval;
					try {
						this.communicationThread.sendDataAsynchronously(new TaskLatency(this.jobID, this.vertexID,
							taskLatency));
					} catch (InterruptedException e) {
						LOG.error(StringUtils.stringifyException(e));
					}
				}
				this.lastTimestamp = timestamp;
				this.tagCounter = 0;
			}
			break;
		case REGULAR:
			final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			if (this.tag == null) {
				taggableRecord.setTag(null);
			} else {
				final long timestamp = System.currentTimeMillis();
				this.tag = createTag(timestamp);
				taggableRecord.setTag(this.tag);
				this.lastTimestamp = timestamp;
			}
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
	public void recordReceived(final Record record) {

		if (this.taskType == TaskType.INPUT) {
			throw new IllegalStateException("Input task received record");
		}

		final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
		this.tag = (StreamingTag) taggableRecord.getTag();
		if (this.tag != null) {

			final long timestamp = System.currentTimeMillis();
			if (this.lastTimestamp > 0) {
				try {
					this.communicationThread.sendDataAsynchronously(new TaskLatency(jobID, vertexID, timestamp
						- this.lastTimestamp));
				} catch (InterruptedException e) {
					LOG.error(StringUtils.stringifyException(e));
				}
				if (this.taskType == TaskType.REGULAR) {
					this.lastTimestamp = -1L;
				} else {
					this.lastTimestamp = timestamp;
				}
			}

			final long pathLatency = timestamp - this.tag.getTimestamp();

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

				final ChannelLatency pl = new ChannelLatency(this.jobID, sourceID, this.vertexID,
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

	private StreamingTag createTag(final long timestamp) {

		this.tag = new StreamingTag(this.vertexID);
		this.tag.setTimestamp(timestamp);

		return this.tag;
	}

}
