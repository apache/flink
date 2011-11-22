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

package eu.stratosphere.nephele.streaming.listeners;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.StreamingTag;
import eu.stratosphere.nephele.streaming.StreamingTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.types.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.TaskLatency;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class StreamListener {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamListener.class);

	private final static double ALPHA = 0.5;

	private final Configuration taskConfiguration;

	private StreamListenerContext listenerContext = null;

	private StreamingTag tag = null;

	private int tagCounter = 0;

	private long lastTimestamp = -1L;

	private Map<ExecutionVertexID, Integer> aggregationCounter = new HashMap<ExecutionVertexID, Integer>();

	private Map<ExecutionVertexID, Double> aggregatedValue = new HashMap<ExecutionVertexID, Double>();

	public StreamListener(final Configuration taskConfiguration) {

		if (taskConfiguration == null) {
			throw new IllegalArgumentException("Argument taskConfiguration must not be null");
		}

		this.taskConfiguration = taskConfiguration;
	}

	/**
	 * Initializes the stream listener by retrieving the listener context from the task manager plugin.
	 */
	public void init() {

		final String listenerKey = this.taskConfiguration.getString(StreamListenerContext.CONTEXT_CONFIGURATION_KEY,
			null);

		if (listenerKey == null) {
			throw new RuntimeException("Stream listener is unable to retrieve context key");
		}

		this.listenerContext = StreamingTaskManagerPlugin.getStreamingListenerContext(listenerKey);
	}

	public long recordEmitted(final Record record) {

		long timestamp = -1L;

		// Input vertex
		if (this.listenerContext.isInputVertex()) {
			final int taggingInterval = this.listenerContext.getTaggingInterval();
			if (this.tagCounter++ == taggingInterval) {
				timestamp = System.currentTimeMillis();
				final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
				taggableRecord.setTag(createTag(timestamp));
				if (this.lastTimestamp > 0L) {
					final long taskLatency = (timestamp - this.lastTimestamp) / taggingInterval;
					try {
						this.listenerContext.sendDataAsynchronously(new TaskLatency(this.listenerContext.getJobID(),
							this.listenerContext.getVertexID(), taskLatency));
					} catch (InterruptedException e) {
						LOG.error(StringUtils.stringifyException(e));
					}
				}
				this.lastTimestamp = timestamp;
				this.tagCounter = 0;
			}
		} else {
			final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			if (this.tag == null) {
				taggableRecord.setTag(null);
			} else {
				timestamp = System.currentTimeMillis();
				this.tag = createTag(timestamp);
				taggableRecord.setTag(this.tag);
				this.lastTimestamp = timestamp;
			}
		}

		return timestamp;
	}

	/**
	 * {@inheritDoc}
	 */
	public void recordReceived(final Record record) {

		/*
		 * if (this.taskType == TaskType.INPUT) {
		 * throw new IllegalStateException("Input task received record");
		 * }
		 * final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
		 * this.tag = (StreamingTag) taggableRecord.getTag();
		 * if (this.tag != null) {
		 * final long timestamp = System.currentTimeMillis();
		 * if (this.lastTimestamp > 0) {
		 * try {
		 * this.communicationThread.sendDataAsynchronously(new TaskLatency(jobID, vertexID, timestamp
		 * - this.lastTimestamp));
		 * } catch (InterruptedException e) {
		 * LOG.error(StringUtils.stringifyException(e));
		 * }
		 * if (this.taskType == TaskType.REGULAR) {
		 * this.lastTimestamp = -1L;
		 * } else {
		 * this.lastTimestamp = timestamp;
		 * }
		 * }
		 * final long pathLatency = timestamp - this.tag.getTimestamp();
		 * final ExecutionVertexID sourceID = this.tag.getSourceID();
		 * // Calculate moving average
		 * Double aggregatedLatency = this.aggregatedValue.get(sourceID);
		 * if (aggregatedLatency == null) {
		 * aggregatedLatency = Double.valueOf(pathLatency);
		 * } else {
		 * aggregatedLatency = Double.valueOf((ALPHA * pathLatency)
		 * + ((1 - ALPHA) * aggregatedLatency.doubleValue()));
		 * }
		 * this.aggregatedValue.put(sourceID, aggregatedLatency);
		 * // Check if we need to compute an event and send it to the job manager component
		 * Integer counter = this.aggregationCounter.get(sourceID);
		 * if (counter == null) {
		 * counter = Integer.valueOf(0);
		 * }
		 * counter = Integer.valueOf(counter.intValue() + 1);
		 * if (counter.intValue() == this.aggregationInterval) {
		 * final ChannelLatency pl = new ChannelLatency(this.jobID, sourceID, this.vertexID,
		 * aggregatedLatency.doubleValue());
		 * try {
		 * this.communicationThread.sendDataAsynchronously(pl);
		 * } catch (InterruptedException e) {
		 * LOG.warn(StringUtils.stringifyException(e));
		 * }
		 * counter = Integer.valueOf(0);
		 * }
		 * this.aggregationCounter.put(sourceID, counter);
		 * }
		 */

	}

	public void reportChannelThroughput(final ChannelID sourceChannelID, final double throughput) {

		try {
			this.listenerContext.sendDataAsynchronously(new ChannelThroughput(this.listenerContext.getJobID(),
				this.listenerContext.getVertexID(), sourceChannelID, throughput));
		} catch (InterruptedException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

	private StreamingTag createTag(final long timestamp) {
		this.tag = new StreamingTag(this.listenerContext.getVertexID());
		this.tag.setTimestamp(timestamp);
		return this.tag;
	}
}
