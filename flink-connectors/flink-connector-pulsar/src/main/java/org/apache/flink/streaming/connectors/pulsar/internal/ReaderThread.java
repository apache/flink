/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Actual working thread that read a specific Pulsar topic.
 *
 * @param <T> the record type that read from each Pulsar message.
 */
@Internal
public class ReaderThread<T> extends Thread {
	private static final Logger log = LoggerFactory.getLogger(ReaderThread.class);
	protected final PulsarFetcher<T> owner;
	protected final PulsarTopicState<T> state;
	protected final ClientConfigurationData clientConf;
	protected final Map<String, Object> readerConf;
	protected final int pollTimeoutMs;
	protected final ExceptionProxy exceptionProxy;
	protected final TopicRange topicRange;
	protected final MessageId startMessageId;
	private boolean failOnDataLoss = true;

	protected volatile boolean running = true;

	protected final PulsarDeserializationSchema<T> deserializer;

	protected volatile Reader<T> reader = null;

	public ReaderThread(
		PulsarFetcher<T> owner,
		PulsarTopicState state,
		ClientConfigurationData clientConf,
		Map<String, Object> readerConf,
		PulsarDeserializationSchema<T> deserializer,
		int pollTimeoutMs,
		ExceptionProxy exceptionProxy) {
		this.owner = owner;
		this.state = state;
		this.clientConf = clientConf;
		this.readerConf = readerConf;
		this.deserializer = deserializer;
		this.pollTimeoutMs = pollTimeoutMs;
		this.exceptionProxy = exceptionProxy;

		this.topicRange = state.getTopicRange();
		this.startMessageId = state.getOffset();
	}

	public ReaderThread(
		PulsarFetcher<T> owner,
		PulsarTopicState state,
		ClientConfigurationData clientConf,
		Map<String, Object> readerConf,
		PulsarDeserializationSchema<T> deserializer,
		int pollTimeoutMs,
		ExceptionProxy exceptionProxy,
		boolean failOnDataLoss) {
		this(owner, state, clientConf, readerConf, deserializer, pollTimeoutMs, exceptionProxy);
		this.failOnDataLoss = failOnDataLoss;
	}

	@Override
	public void run() {
		log.info(
			"Starting to fetch from {} at {}, failOnDataLoss {}",
			topicRange,
			startMessageId,
			failOnDataLoss);

		try {
			createActualReader();

			skipFirstMessageIfNeeded();

			log.info("Starting to read {} with reader thread {}", topicRange, getName());

			while (running) {
				Message<T> message = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS);
				if (message != null) {
					emitRecord(message);
				}
			}
		} catch (Throwable e) {
			exceptionProxy.reportError(e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Throwable e) {
					log.error("Error while closing Pulsar reader " + e.toString());
				}
			}
		}
	}

	protected void createActualReader() throws org.apache.pulsar.client.api.PulsarClientException, ExecutionException {
		ReaderBuilder<T> readerBuilder = CachedPulsarClient
			.getOrCreate(clientConf)
			.newReader(deserializer.getSchema())
			.topic(topicRange.getTopic())
			.startMessageId(startMessageId)
			.startMessageIdInclusive()
			.loadConf(readerConf);
		log.info("Create a reader at topic {} starting from message {} (inclusive) : config = {}",
			topicRange, startMessageId, readerConf);
		if (!topicRange.isFullRange()) {
			readerBuilder.keyHashRange(topicRange.getPulsarRange());
		}

		reader = readerBuilder.create();
	}

	protected void skipFirstMessageIfNeeded() throws org.apache.pulsar.client.api.PulsarClientException {
		Message<?> currentMessage = null;
		MessageId currentId;
		boolean failOnDataLoss = this.failOnDataLoss;
		if (!startMessageId.equals(MessageId.earliest)
			&& !startMessageId.equals(MessageId.latest)
			&& ((MessageIdImpl) startMessageId).getEntryId() != -1) {
			MessageIdImpl lastMessageId = (MessageIdImpl) this.owner
				.getMetaDataReader()
				.getLastMessageId(reader.getTopic());
			if (!messageIdRoughEquals(startMessageId, lastMessageId)
				&& !reader.hasMessageAvailable()) {
				MessageIdImpl startMsgIdImpl = (MessageIdImpl) startMessageId;
				long startMsgLedgerId = startMsgIdImpl.getLedgerId();
				long startMsgEntryId = startMsgIdImpl.getEntryId();

				// startMessageId is bigger than lastMessageId
				if (startMsgLedgerId > lastMessageId.getLedgerId()
					|| (startMsgLedgerId == lastMessageId.getLedgerId()
					&& lastMessageId.getEntryId() != -1
					&& startMsgEntryId > lastMessageId.getEntryId())) {
					log.error(
						"the start message id is beyond the last commit message id, with topic:{}, "
							+
							"start msgId:{}, last msgId:{}",
						reader.getTopic(),
						startMessageId,
						lastMessageId);
				}
				log.warn("reset message to valid offset");
				this.owner.getMetaDataReader().resetCursor(reader.getTopic(), startMessageId);
			} else {
				failOnDataLoss = false;
			}
			while (currentMessage == null && running) {
				currentMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS);
				if (failOnDataLoss) {
					break;
				}
			}
			if (currentMessage == null) {
				reportDataLoss(String.format(
					"Cannot read data at offset %s from topic: %s",
					startMessageId.toString(),
					topicRange));
			} else {
				currentId = currentMessage.getMessageId();
				state.setOffset(currentId);
				if (!messageIdRoughEquals(currentId, startMessageId) && failOnDataLoss) {
					reportDataLoss(
						String.format(
							"Potential Data Loss in reading %s: intended to start at %s, actually we get %s",
							topicRange,
							startMessageId.toString(),
							currentId.toString()));
				}

				if (startMessageId instanceof BatchMessageIdImpl
					&& currentId instanceof BatchMessageIdImpl) {
					// we seek using a batch message id, we can read next directly later
				} else if (startMessageId instanceof MessageIdImpl
					&& currentId instanceof BatchMessageIdImpl) {
					// we seek using a message id, this is supposed to be read by previous task since it's
					// inclusive for the checkpoint, so we skip this batch
					BatchMessageIdImpl cbmid = (BatchMessageIdImpl) currentId;

					MessageIdImpl newStart =
						new MessageIdImpl(
							cbmid.getLedgerId(),
							cbmid.getEntryId() + 1,
							cbmid.getPartitionIndex());
					reader.seek(newStart);
				} else if (startMessageId instanceof MessageIdImpl
					&& currentId instanceof MessageIdImpl) {
					// current entry is a non-batch entry, we can read next directly later
				}
			}
		}
	}

	protected void emitRecord(Message<T> message) throws IOException {
		MessageId messageId = message.getMessageId();
		final T record = deserializer.deserialize(message);
		if (deserializer.isEndOfStream(record)) {
			running = false;
			return;
		}
		owner.emitRecordsWithTimestamps(record, state, messageId, message.getEventTime());
	}

	public void cancel() throws IOException {
		this.running = false;

		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				log.error("failed to close reader. ", e);
			}
		}

		this.interrupt();
	}

	public boolean isRunning() {
		return running;
	}

	private void reportDataLoss(String message) {
		running = false;
		exceptionProxy.reportError(
			new IllegalStateException(
				message + PulsarOptions.INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE));
	}

	/**
	 * used to check whether starting position and current message we got actually are equal
	 * we neglect the potential batchIdx deliberately while seeking to MessageIdImpl for batch entry.
	 */
	public static boolean messageIdRoughEquals(MessageId l, MessageId r) {
		if (l == null || r == null) {
			return false;
		}

		if (l instanceof BatchMessageIdImpl && r instanceof BatchMessageIdImpl) {
			return l.equals(r);
		} else if (l instanceof MessageIdImpl && r instanceof BatchMessageIdImpl) {
			BatchMessageIdImpl rb = (BatchMessageIdImpl) r;
			return l.equals(new MessageIdImpl(
				rb.getLedgerId(),
				rb.getEntryId(),
				rb.getPartitionIndex()));
		} else if (r instanceof MessageIdImpl && l instanceof BatchMessageIdImpl) {
			BatchMessageIdImpl lb = (BatchMessageIdImpl) l;
			return r.equals(new MessageIdImpl(
				lb.getLedgerId(),
				lb.getEntryId(),
				lb.getPartitionIndex()));
		} else if (l instanceof MessageIdImpl && r instanceof MessageIdImpl) {
			return l.equals(r);
		} else {
			throw new IllegalStateException(
				String.format(
					"comparing messageIds of type %s, %s",
					l.getClass().toString(),
					r.getClass().toString()));
		}
	}
}
