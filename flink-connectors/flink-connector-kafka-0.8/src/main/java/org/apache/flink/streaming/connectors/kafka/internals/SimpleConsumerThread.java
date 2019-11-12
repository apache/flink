/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.ExceptionUtils;

import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.PropertiesUtil.getInt;

/**
 * This class implements a thread with a connection to a single Kafka broker. The thread
 * pulls records for a set of topic partitions for which the connected broker is currently
 * the leader. The thread deserializes these records and emits them.
 *
 * @param <T> The type of elements that this consumer thread creates from Kafka's byte messages
 *            and emits into the Flink DataStream.
 */
@Internal
class SimpleConsumerThread<T> extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumerThread.class);

	private static final KafkaTopicPartitionState<TopicAndPartition> MARKER = Kafka08Fetcher.MARKER;

	// ------------------------------------------------------------------------

	private final Kafka08Fetcher<T> owner;

	private final KafkaDeserializationSchema<T> deserializer;

	private final List<KafkaTopicPartitionState<TopicAndPartition>> partitions;

	private final Node broker;

	/** Queue containing new fetch partitions for the consumer thread. */
	private final ClosableBlockingQueue<KafkaTopicPartitionState<TopicAndPartition>> newPartitionsQueue;

	private final ClosableBlockingQueue<KafkaTopicPartitionState<TopicAndPartition>> unassignedPartitions;

	private final ExceptionProxy errorHandler;

	private final long invalidOffsetBehavior;

	private volatile boolean running = true;

	// ----------------- Simple Consumer ----------------------
	private volatile SimpleConsumer consumer;

	private final int soTimeout;
	private final int minBytes;
	private final int maxWait;
	private final int fetchSize;
	private final int bufferSize;
	private final int reconnectLimit;
	private final String clientId;

	// exceptions are thrown locally
	public SimpleConsumerThread(
			Kafka08Fetcher<T> owner,
			ExceptionProxy errorHandler,
			Properties config,
			Node broker,
			List<KafkaTopicPartitionState<TopicAndPartition>> seedPartitions,
			ClosableBlockingQueue<KafkaTopicPartitionState<TopicAndPartition>> unassignedPartitions,
			KafkaDeserializationSchema<T> deserializer,
			long invalidOffsetBehavior) {
		this.owner = owner;
		this.errorHandler = errorHandler;
		this.broker = broker;
		// all partitions should have been assigned a starting offset by the fetcher
		checkAllPartitionsHaveDefinedStartingOffsets(seedPartitions);
		this.partitions = seedPartitions;
		this.deserializer = requireNonNull(deserializer);
		this.unassignedPartitions = requireNonNull(unassignedPartitions);
		this.newPartitionsQueue = new ClosableBlockingQueue<>();
		this.invalidOffsetBehavior = invalidOffsetBehavior;

		// these are the actual configuration values of Kafka + their original default values.
		this.soTimeout = getInt(config, "socket.timeout.ms", 30000);
		this.minBytes = getInt(config, "fetch.min.bytes", 1);
		this.maxWait = getInt(config, "fetch.wait.max.ms", 100);
		this.fetchSize = getInt(config, "fetch.message.max.bytes", 1048576);
		this.bufferSize = getInt(config, "socket.receive.buffer.bytes", 65536);
		this.reconnectLimit = getInt(config, "flink.simple-consumer-reconnectLimit", 3);
		String groupId = config.getProperty("group.id", "flink-kafka-consumer-legacy-" + broker.id());
		this.clientId = config.getProperty("client.id", groupId);
	}

	public ClosableBlockingQueue<KafkaTopicPartitionState<TopicAndPartition>> getNewPartitionsQueue() {
		return newPartitionsQueue;
	}

	// ------------------------------------------------------------------------
	//  main work loop
	// ------------------------------------------------------------------------

	@Override
	public void run() {
		LOG.info("Starting to fetch from {}", this.partitions);

		// set up the config values
		try {
			// create the Kafka consumer that we actually use for fetching
			consumer = new SimpleConsumer(broker.host(), broker.port(), soTimeout, bufferSize, clientId);

			// replace earliest of latest starting offsets with actual offset values fetched from Kafka
			requestAndSetEarliestOrLatestOffsetsFromKafka(consumer, partitions);

			LOG.info("Starting to consume {} partitions with consumer thread {}", partitions.size(), getName());

			// Now, the actual work starts :-)
			int offsetOutOfRangeCount = 0;
			int reconnects = 0;
			while (running) {

				// ----------------------------------- partitions list maintenance ----------------------------

				// check queue for new partitions to read from:
				List<KafkaTopicPartitionState<TopicAndPartition>> newPartitions = newPartitionsQueue.pollBatch();
				if (newPartitions != null) {
					// found some new partitions for this thread's broker

					// the new partitions should already be assigned a starting offset
					checkAllPartitionsHaveDefinedStartingOffsets(newPartitions);
					// if the new partitions are to start from earliest or latest offsets,
					// we need to replace them with actual values from Kafka
					requestAndSetEarliestOrLatestOffsetsFromKafka(consumer, newPartitions);

					// add the new partitions (and check they are not already in there)
					for (KafkaTopicPartitionState<TopicAndPartition> newPartition: newPartitions) {
						if (partitions.contains(newPartition)) {
							throw new IllegalStateException("Adding partition " + newPartition +
									" to subscribed partitions even though it is already subscribed");
						}
						partitions.add(newPartition);
					}

					LOG.info("Adding {} new partitions to consumer thread {}", newPartitions.size(), getName());
					LOG.debug("Partitions list: {}", newPartitions);
				}

				if (partitions.size() == 0) {
					if (newPartitionsQueue.close()) {
						// close succeeded. Closing thread
						running = false;

						LOG.info("Consumer thread {} does not have any partitions assigned anymore. Stopping thread.",
								getName());

						// add the wake-up marker into the queue to make the main thread
						// immediately wake up and termination faster
						unassignedPartitions.add(MARKER);

						break;
					} else {
						// close failed: fetcher main thread concurrently added new partitions into the queue.
						// go to top of loop again and get the new partitions
						continue;
					}
				}

				// ----------------------------------- request / response with kafka ----------------------------

				FetchRequestBuilder frb = new FetchRequestBuilder();
				frb.clientId(clientId);
				frb.maxWait(maxWait);
				frb.minBytes(minBytes);

				for (KafkaTopicPartitionState<?> partition : partitions) {
					frb.addFetch(
							partition.getKafkaTopicPartition().getTopic(),
							partition.getKafkaTopicPartition().getPartition(),
							partition.getOffset() + 1, // request the next record
							fetchSize);
				}

				kafka.api.FetchRequest fetchRequest = frb.build();
				LOG.debug("Issuing fetch request {}", fetchRequest);

				FetchResponse fetchResponse;
				try {
					fetchResponse = consumer.fetch(fetchRequest);
				}
				catch (Throwable cce) {
					//noinspection ConstantConditions
					if (cce instanceof ClosedChannelException) {
						LOG.warn("Fetch failed because of ClosedChannelException.");
						LOG.debug("Full exception", cce);

						// we don't know if the broker is overloaded or unavailable.
						// retry a few times, then return ALL partitions for new leader lookup
						if (++reconnects >= reconnectLimit) {
							LOG.warn("Unable to reach broker after {} retries. Returning all current partitions", reconnectLimit);
							for (KafkaTopicPartitionState<TopicAndPartition> fp: this.partitions) {
								unassignedPartitions.add(fp);
							}
							this.partitions.clear();
							continue; // jump to top of loop: will close thread or subscribe to new partitions
						}
						try {
							consumer.close();
						} catch (Throwable t) {
							LOG.warn("Error while closing consumer connection", t);
						}
						// delay & retry
						Thread.sleep(100);
						consumer = new SimpleConsumer(broker.host(), broker.port(), soTimeout, bufferSize, clientId);
						continue; // retry
					} else {
						throw cce;
					}
				}
				reconnects = 0;

				// ---------------------------------------- error handling ----------------------------

				if (fetchResponse == null) {
					throw new IOException("Fetch from Kafka failed (request returned null)");
				}

				if (fetchResponse.hasError()) {
					String exception = "";
					List<KafkaTopicPartitionState<TopicAndPartition>> partitionsToGetOffsetsFor = new ArrayList<>();

					// iterate over partitions to get individual error codes
					Iterator<KafkaTopicPartitionState<TopicAndPartition>> partitionsIterator = partitions.iterator();
					boolean partitionsRemoved = false;

					while (partitionsIterator.hasNext()) {
						final KafkaTopicPartitionState<TopicAndPartition> fp = partitionsIterator.next();
						short code = fetchResponse.errorCode(fp.getTopic(), fp.getPartition());

						if (code == ErrorMapping.OffsetOutOfRangeCode()) {
							// we were asked to read from an out-of-range-offset (maybe set wrong in Zookeeper)
							// Kafka's high level consumer is resetting the offset according to 'auto.offset.reset'
							partitionsToGetOffsetsFor.add(fp);
						}
						else if (code == ErrorMapping.NotLeaderForPartitionCode() ||
								code == ErrorMapping.LeaderNotAvailableCode() ||
								code == ErrorMapping.BrokerNotAvailableCode() ||
								code == ErrorMapping.UnknownCode()) {
							// the broker we are connected to is not the leader for the partition.
							LOG.warn("{} is not the leader of {}. Reassigning leader for partition", broker, fp);
							LOG.debug("Error code = {}", code);

							unassignedPartitions.add(fp);

							partitionsIterator.remove(); // unsubscribe the partition ourselves
							partitionsRemoved = true;
						}
						else if (code != ErrorMapping.NoError()) {
							exception += "\nException for " + fp.getTopic() + ":" + fp.getPartition() + ": " +
									ExceptionUtils.stringifyException(ErrorMapping.exceptionFor(code));
						}
					}
					if (partitionsToGetOffsetsFor.size() > 0) {
						// safeguard against an infinite loop.
						if (offsetOutOfRangeCount++ > 3) {
							throw new RuntimeException("Found invalid offsets more than three times in partitions "
									+ partitionsToGetOffsetsFor + " Exceptions: " + exception);
						}
						// get valid offsets for these partitions and try again.
						LOG.warn("The following partitions had an invalid offset: {}", partitionsToGetOffsetsFor);
						requestAndSetSpecificTimeOffsetsFromKafka(consumer, partitionsToGetOffsetsFor, invalidOffsetBehavior);

						LOG.warn("The new partition offsets are {}", partitionsToGetOffsetsFor);
						continue; // jump back to create a new fetch request. The offset has not been touched.
					}
					else if (partitionsRemoved) {
						continue; // create new fetch request
					}
					else {
						// partitions failed on an error
						throw new IOException("Error while fetching from broker '" + broker + "': " + exception);
					}
				} else {
					// successful fetch, reset offsetOutOfRangeCount.
					offsetOutOfRangeCount = 0;
				}

				// ----------------------------------- process fetch response ----------------------------

				int messagesInFetch = 0;
				int deletedMessages = 0;
				Iterator<KafkaTopicPartitionState<TopicAndPartition>> partitionsIterator = partitions.iterator();

				partitionsLoop:
				while (partitionsIterator.hasNext()) {
					final KafkaTopicPartitionState<TopicAndPartition> currentPartition = partitionsIterator.next();

					final ByteBufferMessageSet messageSet = fetchResponse.messageSet(
							currentPartition.getTopic(), currentPartition.getPartition());

					for (MessageAndOffset msg : messageSet) {
						if (running) {
							messagesInFetch++;
							final ByteBuffer payload = msg.message().payload();
							final long offset = msg.offset();

							if (offset <= currentPartition.getOffset()) {
								// we have seen this message already
								LOG.info("Skipping message with offset " + msg.offset()
										+ " because we have seen messages until (including) "
										+ currentPartition.getOffset()
										+ " from topic/partition " + currentPartition.getTopic() + '/'
										+ currentPartition.getPartition() + " already");
								continue;
							}

							// If the message value is null, this represents a delete command for the message key.
							// Log this and pass it on to the client who might want to also receive delete messages.
							byte[] valueBytes;
							if (payload == null) {
								deletedMessages++;
								valueBytes = null;
							} else {
								valueBytes = new byte[payload.remaining()];
								payload.get(valueBytes);
							}

							// put key into byte array
							byte[] keyBytes = null;
							int keySize = msg.message().keySize();

							if (keySize >= 0) { // message().hasKey() is doing the same. We save one int deserialization
								ByteBuffer keyPayload = msg.message().key();
								keyBytes = new byte[keySize];
								keyPayload.get(keyBytes);
							}

							final T value = deserializer.deserialize(
								new ConsumerRecord<>(
									currentPartition.getTopic(),
									currentPartition.getPartition(), keyBytes, valueBytes, offset));

							if (deserializer.isEndOfStream(value)) {
								// remove partition from subscribed partitions.
								partitionsIterator.remove();
								continue partitionsLoop;
							}

							owner.emitRecord(value, currentPartition, offset);
						}
						else {
							// no longer running
							return;
						}
					}
				}
				LOG.debug("This fetch contained {} messages ({} deleted messages)", messagesInFetch, deletedMessages);
			} // end of fetch loop

			if (!newPartitionsQueue.close()) {
				throw new Exception("Bug: Cleanly leaving fetcher thread without having a closed queue.");
			}
		}
		catch (Throwable t) {
			// report to the fetcher's error handler
			errorHandler.reportError(t);
		}
		finally {
			if (consumer != null) {
				// closing the consumer should not fail the program
				try {
					consumer.close();
				}
				catch (Throwable t) {
					LOG.error("Error while closing the Kafka simple consumer", t);
				}
			}
		}
	}

	/**
	 * Cancels this fetch thread. The thread will release all resources and terminate.
	 */
	public void cancel() {
		this.running = false;

		// interrupt whatever the consumer is doing
		if (consumer != null) {
			consumer.close();
		}

		this.interrupt();
	}

	// ------------------------------------------------------------------------
	//  Kafka Request Utils
	// ------------------------------------------------------------------------

	/**
	 * Request offsets before a specific time for a set of partitions, via a Kafka consumer.
	 *
	 * @param consumer The consumer connected to lead broker
	 * @param partitions The list of partitions we need offsets for
	 * @param whichTime The type of time we are requesting. -1 and -2 are special constants (See OffsetRequest)
	 */
	private static void requestAndSetSpecificTimeOffsetsFromKafka(
			SimpleConsumer consumer,
			List<KafkaTopicPartitionState<TopicAndPartition>> partitions,
			long whichTime) throws IOException {
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
		for (KafkaTopicPartitionState<TopicAndPartition> part : partitions) {
			requestInfo.put(part.getKafkaPartitionHandle(), new PartitionOffsetRequestInfo(whichTime, 1));
		}

		requestAndSetOffsetsFromKafka(consumer, partitions, requestInfo);
	}

	/**
	 * For a set of partitions, if a partition is set with the special offsets {@link OffsetRequest#EarliestTime()}
	 * or {@link OffsetRequest#LatestTime()}, replace them with actual offsets requested via a Kafka consumer.
	 *
	 * @param consumer The consumer connected to lead broker
	 * @param partitions The list of partitions we need offsets for
	 */
	private static void requestAndSetEarliestOrLatestOffsetsFromKafka(
			SimpleConsumer consumer,
			List<KafkaTopicPartitionState<TopicAndPartition>> partitions) throws Exception {
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
		for (KafkaTopicPartitionState<TopicAndPartition> part : partitions) {
			if (part.getOffset() == OffsetRequest.EarliestTime() || part.getOffset() == OffsetRequest.LatestTime()) {
				requestInfo.put(part.getKafkaPartitionHandle(), new PartitionOffsetRequestInfo(part.getOffset(), 1));
			}
		}

		requestAndSetOffsetsFromKafka(consumer, partitions, requestInfo);
	}

	/**
	 * Request offsets from Kafka with a specified set of partition's offset request information.
	 * The returned offsets are used to set the internal partition states.
	 *
	 * <p>This method retries three times if the response has an error.
	 *
	 * @param consumer The consumer connected to lead broker
	 * @param partitionStates the partition states, will be set with offsets fetched from Kafka request
	 * @param partitionToRequestInfo map of each partition to its offset request info
	 */
	private static void requestAndSetOffsetsFromKafka(
			SimpleConsumer consumer,
			List<KafkaTopicPartitionState<TopicAndPartition>> partitionStates,
			Map<TopicAndPartition, PartitionOffsetRequestInfo> partitionToRequestInfo) throws IOException {
		int retries = 0;
		OffsetResponse response;
		while (true) {
			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				partitionToRequestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
			response = consumer.getOffsetsBefore(request);

			if (response.hasError()) {
				StringBuilder exception = new StringBuilder();
				for (KafkaTopicPartitionState<TopicAndPartition> part : partitionStates) {
					short code;
					if ((code = response.errorCode(part.getTopic(), part.getPartition())) != ErrorMapping.NoError()) {
						exception.append("\nException for topic=").append(part.getTopic())
							.append(" partition=").append(part.getPartition()).append(": ")
							.append(ExceptionUtils.stringifyException(ErrorMapping.exceptionFor(code)));
					}
				}
				if (++retries >= 3) {
					throw new IOException("Unable to get last offset for partitions " + partitionStates + ": "
						+ exception.toString());
				} else {
					LOG.warn("Unable to get last offset for partitions: Exception(s): {}", exception);
				}
			} else {
				break; // leave retry loop
			}
		}

		for (KafkaTopicPartitionState<TopicAndPartition> part: partitionStates) {
			// there will be offsets only for partitions that were requested for
			if (partitionToRequestInfo.containsKey(part.getKafkaPartitionHandle())) {
				final long offset = response.offsets(part.getTopic(), part.getPartition())[0];

				// the offset returned is that of the next record to fetch. because our state reflects the latest
				// successfully emitted record, we subtract one
				part.setOffset(offset - 1);
			}
		}
	}

	private static void checkAllPartitionsHaveDefinedStartingOffsets(
		List<KafkaTopicPartitionState<TopicAndPartition>> partitions) {
		for (KafkaTopicPartitionState<TopicAndPartition> part : partitions) {
			if (!part.isOffsetDefined()) {
				throw new IllegalArgumentException("SimpleConsumerThread received a partition with undefined starting offset");
			}
		}
	}
}
