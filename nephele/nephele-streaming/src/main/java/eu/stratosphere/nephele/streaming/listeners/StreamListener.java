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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.StreamingTag;
import eu.stratosphere.nephele.streaming.StreamingTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.actions.AbstractAction;
import eu.stratosphere.nephele.streaming.actions.BufferSizeLimitAction;
import eu.stratosphere.nephele.streaming.types.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.TaskLatency;
import eu.stratosphere.nephele.streaming.wrappers.StreamingOutputGate;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class StreamListener {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamListener.class);

	private final Configuration taskConfiguration;

	private StreamListenerContext listenerContext = null;

	private int tagCounter = 0;

	private Map<GateID, StreamingOutputGate<? extends Record>> outputGateMap = new HashMap<GateID, StreamingOutputGate<? extends Record>>();

	private Map<ChannelID, AbstractOutputChannel<? extends Record>> outputChannelMap;

	/**
	 * Indicates the time of the last received tagged incoming record
	 */
	private long lastTimestamp = -1L;

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

		final Map<ChannelID, AbstractOutputChannel<? extends Record>> tmpMap = new HashMap<ChannelID, AbstractOutputChannel<? extends Record>>();

		final Iterator<StreamingOutputGate<? extends Record>> it = this.outputGateMap.values().iterator();
		while (it.hasNext()) {
			final StreamingOutputGate<? extends Record> outputGate = it.next();
			final int numberOfOutputChannels = outputGate.getNumberOfOutputChannels();
			for (int i = 0; i < numberOfOutputChannels; ++i) {
				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(i);
				tmpMap.put(outputChannel.getID(), outputChannel);
			}
		}

		this.outputChannelMap = Collections.unmodifiableMap(tmpMap);
	}

	public long recordEmitted(final Record record) {

		long timestamp = -1L;

		// Input vertex
		if (this.listenerContext.isInputVertex()) {

			final int taggingInterval = this.listenerContext.getTaggingInterval();
			final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;

			// Tag every <taggingInterval> record and calculate task latency
			if (this.tagCounter++ == taggingInterval) {
				timestamp = System.currentTimeMillis();
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

				// Finally, check for pending actions
				checkForPendingActions();
			} else {
				taggableRecord.setTag(null);
			}

		} else {

			final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;

			if (this.lastTimestamp >= 0L) {

				timestamp = System.currentTimeMillis();
				taggableRecord.setTag(createTag(timestamp));
				final JobID jobID = this.listenerContext.getJobID();
				final ExecutionVertexID vertexID = this.listenerContext.getVertexID();

				// Calculate task latency
				final TaskLatency tl = new TaskLatency(jobID, vertexID, timestamp - this.lastTimestamp);
				try {
					this.listenerContext.sendDataAsynchronously(tl);
				} catch (InterruptedException e) {
					LOG.error(StringUtils.stringifyException(e));
				}

				this.lastTimestamp = -1L;

				// Finally, check for pending actions
				checkForPendingActions();
			} else {
				taggableRecord.setTag(null);
			}
		}

		return timestamp;
	}

	/**
	 * {@inheritDoc}
	 */
	public void recordReceived(final Record record) {

		final AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
		final StreamingTag tag = (StreamingTag) taggableRecord.getTag();
		if (tag != null) {

			final long timestamp = System.currentTimeMillis();
			final JobID jobID = this.listenerContext.getJobID();

			final ExecutionVertexID vertexID = this.listenerContext.getVertexID();

			// Calculate channel latency
			final ChannelLatency cl = new ChannelLatency(jobID, tag.getSourceID(), vertexID, timestamp
				- tag.getTimestamp());
			try {
				this.listenerContext.sendDataAsynchronously(cl);
			} catch (InterruptedException e) {
				LOG.warn(StringUtils.stringifyException(e));
			}

			this.lastTimestamp = timestamp;
		}
	}

	public void reportChannelThroughput(final ChannelID sourceChannelID, final double throughput) {

		try {
			this.listenerContext.sendDataAsynchronously(new ChannelThroughput(this.listenerContext.getJobID(),
				this.listenerContext.getVertexID(), sourceChannelID, throughput));
		} catch (InterruptedException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

	public void reportBufferLatency(final ChannelID sourceChannelID, final int bufferLatency) {

		try {
			this.listenerContext.sendDataAsynchronously(new OutputBufferLatency(this.listenerContext.getJobID(),
				this.listenerContext.getVertexID(), sourceChannelID, bufferLatency));
		} catch (InterruptedException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

	private StreamingTag createTag(final long timestamp) {
		StreamingTag tag = new StreamingTag(this.listenerContext.getVertexID());
		tag.setTimestamp(timestamp);
		return tag;
	}

	private void checkForPendingActions() {

		final Queue<AbstractAction> pendingActions = this.listenerContext.getPendingActionsQueue();

		synchronized (pendingActions) {

			while (!pendingActions.isEmpty()) {

				final AbstractAction action = pendingActions.poll();

				if (action instanceof BufferSizeLimitAction) {
					limitBufferSize((BufferSizeLimitAction) action);
				} else {
					LOG.error("Ignoring unknown action of type " + action.getClass());
				}
			}
		}
	}

	private void limitBufferSize(final BufferSizeLimitAction bsla) {

		final ChannelID sourceChannelID = bsla.getSourceChannelID();
		final int bufferSize = bsla.getBufferSize();

		final AbstractOutputChannel<? extends Record> outputChannel = this.outputChannelMap.get(sourceChannelID);
		if (outputChannel == null) {
			LOG.error("Cannot find output channel with ID " + sourceChannelID);
			return;
		}

		if (!(outputChannel instanceof AbstractByteBufferedOutputChannel)) {
			LOG.error("Output channel with ID " + sourceChannelID + " is not a byte-buffered channel");
			return;
		}

		final AbstractByteBufferedOutputChannel<? extends Record> byteBufferedOutputChannel =
			(AbstractByteBufferedOutputChannel<? extends Record>) outputChannel;

		LOG.info("Setting buffer size limit of output channel " + sourceChannelID + " to " + bufferSize + " bytes");
		byteBufferedOutputChannel.limitBufferSize(bufferSize);
	}

	public void registerOutputGate(final StreamingOutputGate<? extends Record> outputGate) {

		this.outputGateMap.put(outputGate.getGateID(), outputGate);
	}
}
