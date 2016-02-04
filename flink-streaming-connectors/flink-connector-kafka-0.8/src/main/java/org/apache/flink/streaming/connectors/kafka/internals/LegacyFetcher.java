/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * This fetcher uses Kafka's low-level API to pull data from a specific
 * set of topics and partitions.
 * 
 * <p>This code is in parts based on the tutorial code for the low-level Kafka consumer.</p>
 */
public class LegacyFetcher implements Fetcher {
	
	private static final Logger LOG = LoggerFactory.getLogger(LegacyFetcher.class);

	
	/** The properties that configure the Kafka connection */
	private final Properties config;
	
	/** The task name, to give more readable names to the spawned threads */
	private final String taskName;
	
	/** The first error that occurred in a connection thread */
	private final AtomicReference<Throwable> error;
	
	/** The classloader for dynamically loaded classes */
	private final ClassLoader userCodeClassloader;

	/** The partitions that the fetcher should read, with their starting offsets */
	private Map<KafkaTopicPartitionLeader, Long> partitionsToRead;

	/** The seek() method might receive KafkaTopicPartition's without leader information
	 * (for example when restoring).
	 * If there are elements in this list, we'll fetch the leader from Kafka.
	 **/
	private Map<KafkaTopicPartition, Long> partitionsToReadWithoutLeader;
	
	/** Reference the the thread that executed the run() method. */
	private volatile Thread mainThread;
	
	/** Flag to shot the fetcher down */
	private volatile boolean running = true;

	public LegacyFetcher(
				List<KafkaTopicPartitionLeader> partitions, Properties props,
				String taskName, ClassLoader userCodeClassloader) {
		
		this.config = requireNonNull(props, "The config properties cannot be null");
		this.userCodeClassloader = requireNonNull(userCodeClassloader);
		
		//this.topic = checkNotNull(topic, "The topic cannot be null");
		this.partitionsToRead = new HashMap<>();
		for (KafkaTopicPartitionLeader p: partitions) {
			partitionsToRead.put(p, FlinkKafkaConsumer08.OFFSET_NOT_SET);
		}
		this.taskName = taskName;
		this.error = new AtomicReference<>();
	}

	// ------------------------------------------------------------------------
	//  Fetcher methods
	// ------------------------------------------------------------------------
	
	@Override
	public void seek(KafkaTopicPartition topicPartition, long offsetToRead) {
		if (partitionsToRead == null) {
			throw new IllegalArgumentException("No partitions to read set");
		}
		if (!topicPartition.isContained(partitionsToRead)) {
			throw new IllegalArgumentException("Can not set offset on a partition (" + topicPartition
					+ ") we are not going to read. Partitions to read " + partitionsToRead);
		}
		if (partitionsToReadWithoutLeader == null) {
			partitionsToReadWithoutLeader = new HashMap<>();
		}
		partitionsToReadWithoutLeader.put(topicPartition, offsetToRead);
	}
	
	@Override
	public void close() {
		// flag needs to be check by the run() method that creates the spawned threads
		this.running = false;
		
		// all other cleanup is made by the run method itself
	}

	@Override
	public <T> void run(SourceFunction.SourceContext<T> sourceContext,
						KeyedDeserializationSchema<T> deserializer,
						HashMap<KafkaTopicPartition, Long> lastOffsets) throws Exception {
		
		if (partitionsToRead == null || partitionsToRead.size() == 0) {
			throw new IllegalArgumentException("No partitions set");
		}
		
		// NOTE: This method is needs to always release all resources it acquires
		
		this.mainThread = Thread.currentThread();

		LOG.info("Reading from partitions " + partitionsToRead + " using the legacy fetcher");

		// get lead broker if necessary
		if (partitionsToReadWithoutLeader != null && partitionsToReadWithoutLeader.size() > 0) {
			LOG.info("Refreshing leader information for partitions {}", KafkaTopicPartition.toString(partitionsToReadWithoutLeader));
			// NOTE: The kafka client apparently locks itself in an infinite loop sometimes
			// when it is interrupted, so we run it only in a separate thread.
			// since it sometimes refuses to shut down, we resort to the admittedly harsh
			// means of killing the thread after a timeout.
			PartitionInfoFetcher infoFetcher = new PartitionInfoFetcher(KafkaTopicPartition.getTopics(partitionsToReadWithoutLeader), config);
			infoFetcher.start();

			KillerWatchDog watchDog = new KillerWatchDog(infoFetcher, 60000);
			watchDog.start();

			List<KafkaTopicPartitionLeader> topicPartitionWithLeaderList = infoFetcher.getPartitions();

			// replace potentially outdated leader information in partitionsToRead with fresh data from topicPartitionWithLeader
			for (Map.Entry<KafkaTopicPartition, Long> pt: partitionsToReadWithoutLeader.entrySet()) {
				KafkaTopicPartitionLeader topicPartitionWithLeader = null;
				// go through list
				for (KafkaTopicPartitionLeader withLeader: topicPartitionWithLeaderList) {
					if (withLeader.getTopicPartition().equals(pt.getKey())) {
						topicPartitionWithLeader = withLeader;
						break;
					}
				}
				if (topicPartitionWithLeader == null) {
					throw new IllegalStateException("Unable to find topic/partition leader information");
				}
				Long removed = KafkaTopicPartitionLeader.replaceIgnoringLeader(topicPartitionWithLeader, pt.getValue(), partitionsToRead);
				if (removed == null) {
					throw new IllegalStateException("Seek request on unknown topic partition");
				}
			}
		}


		// build a map for each broker with its partitions
		Map<Node, List<FetchPartition>> fetchBrokers = new HashMap<>();

		for (Map.Entry<KafkaTopicPartitionLeader, Long> entry : partitionsToRead.entrySet()) {
			final KafkaTopicPartitionLeader topicPartition = entry.getKey();
			final long offset = entry.getValue();

			List<FetchPartition> partitions = fetchBrokers.get(topicPartition.getLeader());
			if (partitions == null) {
				partitions = new ArrayList<>();
				fetchBrokers.put(topicPartition.getLeader(), partitions);
			}

			partitions.add(new FetchPartition(topicPartition.getTopicPartition().getTopic(), topicPartition.getTopicPartition().getPartition(), offset));
		}

		// create SimpleConsumers for each broker
		ArrayList<SimpleConsumerThread<?>> consumers = new ArrayList<>(fetchBrokers.size());
		
		for (Map.Entry<Node, List<FetchPartition>> brokerInfo : fetchBrokers.entrySet()) {
			final Node broker = brokerInfo.getKey();
			final List<FetchPartition> partitionsList = brokerInfo.getValue();
			
			FetchPartition[] partitions = partitionsList.toArray(new FetchPartition[partitionsList.size()]);

			final KeyedDeserializationSchema<T> clonedDeserializer =
					InstantiationUtil.clone(deserializer, userCodeClassloader);

			SimpleConsumerThread<T> thread = new SimpleConsumerThread<>(this, config,
					broker, partitions, sourceContext, clonedDeserializer, lastOffsets);

			thread.setName(String.format("SimpleConsumer - %s - broker-%s (%s:%d)",
					taskName, broker.id(), broker.host(), broker.port()));
			thread.setDaemon(true);
			consumers.add(thread);
		}
		
		// last check whether we should abort.
		if (!running) {
			return;
		}
		
		// start all consumer threads
		for (SimpleConsumerThread<?> t : consumers) {
			LOG.info("Starting thread {}", t.getName());
			t.start();
		}
		
		// wait until all consumer threads are done, or until we are aborted, or until
		// an error occurred in one of the fetcher threads
		try {
			boolean someConsumersRunning = true;
			while (running && error.get() == null && someConsumersRunning) {
				try {
					// wait for the consumer threads. if an error occurs, we are interrupted
					for (SimpleConsumerThread<?> t : consumers) {
						t.join();
					}
	
					// safety net
					someConsumersRunning = false;
					for (SimpleConsumerThread<?> t : consumers) {
						someConsumersRunning |= t.isAlive();
					}
				}
				catch (InterruptedException e) {
					// ignore. we should notice what happened in the next loop check
				}
			}
			
			// make sure any asynchronous error is noticed
			Throwable error = this.error.get();
			if (error != null) {
				throw new Exception(error.getMessage(), error);
			}
		}
		finally {
			// make sure that in any case (completion, abort, error), all spawned threads are stopped
			for (SimpleConsumerThread<?> t : consumers) {
				if (t.isAlive()) {
					t.cancel();
				}
			}
		}
	}
	
	/**
	 * Reports an error from a fetch thread. This will cause the main thread to see this error,
	 * abort, and cancel all other fetch threads.
	 * 
	 * @param error The error to report.
	 */
	@Override
	public void stopWithError(Throwable error) {
		if (this.error.compareAndSet(null, error)) {
			// we are the first to report an error
			if (mainThread != null) {
				mainThread.interrupt();
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Representation of a partition to fetch.
	 */
	private static class FetchPartition {

		final String topic;
		
		/** ID of the partition within the topic (0 indexed, as given by Kafka) */
		final int partition;
		
		/** Offset pointing at the next element to read from that partition. */
		long nextOffsetToRead;

		FetchPartition(String topic, int partition, long nextOffsetToRead) {
			this.topic = topic;
			this.partition = partition;
			this.nextOffsetToRead = nextOffsetToRead;
		}
		
		@Override
		public String toString() {
			return "FetchPartition {topic=" + topic +", partition=" + partition + ", offset=" + nextOffsetToRead + '}';
		}
	}

	// ------------------------------------------------------------------------
	//  Per broker fetcher
	// ------------------------------------------------------------------------
	
	/**
	 * Each broker needs its separate connection. This thread implements the connection to
	 * one broker. The connection can fetch multiple partitions from the broker.
	 * 
	 * @param <T> The data type fetched.
	 */
	private static class SimpleConsumerThread<T> extends Thread {
		
		private final SourceFunction.SourceContext<T> sourceContext;
		private final KeyedDeserializationSchema<T> deserializer;
		private final HashMap<KafkaTopicPartition, Long> offsetsState;
		
		private final FetchPartition[] partitions;
		
		private final Node broker;

		private final Properties config;

		private final LegacyFetcher owner;

		private SimpleConsumer consumer;
		
		private volatile boolean running = true;


		// exceptions are thrown locally
		public SimpleConsumerThread(LegacyFetcher owner,
									Properties config,
									Node broker,
									FetchPartition[] partitions,
									SourceFunction.SourceContext<T> sourceContext,
									KeyedDeserializationSchema<T> deserializer,
									HashMap<KafkaTopicPartition, Long> offsetsState) {
			this.owner = owner;
			this.config = config;
			this.broker = broker;
			this.partitions = partitions;
			this.sourceContext = requireNonNull(sourceContext);
			this.deserializer = requireNonNull(deserializer);
			this.offsetsState = requireNonNull(offsetsState);
		}

		@Override
		public void run() {
			LOG.info("Starting to fetch from {}", Arrays.toString(this.partitions));
			try {
				// set up the config values
				final String clientId = "flink-kafka-consumer-legacy-" + broker.id();

				// these are the actual configuration values of Kafka + their original default values.
				final int soTimeout = Integer.valueOf(config.getProperty("socket.timeout.ms", "30000"));
				final int bufferSize = Integer.valueOf(config.getProperty("socket.receive.buffer.bytes", "65536"));
				final int fetchSize = Integer.valueOf(config.getProperty("fetch.message.max.bytes", "1048576"));
				final int maxWait = Integer.valueOf(config.getProperty("fetch.wait.max.ms", "100"));
				final int minBytes = Integer.valueOf(config.getProperty("fetch.min.bytes", "1"));
				
				// create the Kafka consumer that we actually use for fetching
				consumer = new SimpleConsumer(broker.host(), broker.port(), soTimeout, bufferSize, clientId);

				// make sure that all partitions have some offsets to start with
				// those partitions that do not have an offset from a checkpoint need to get
				// their start offset from ZooKeeper
				{
					List<FetchPartition> partitionsToGetOffsetsFor = new ArrayList<>();

					for (FetchPartition fp : partitions) {
						if (fp.nextOffsetToRead == FlinkKafkaConsumer08.OFFSET_NOT_SET) {
							// retrieve the offset from the consumer
							partitionsToGetOffsetsFor.add(fp);
						}
					}
					if (partitionsToGetOffsetsFor.size() > 0) {
						getLastOffset(consumer, partitionsToGetOffsetsFor, getInvalidOffsetBehavior(config));
						LOG.info("No prior offsets found for some partitions. Fetched the following start offsets {}", partitionsToGetOffsetsFor);
					}
				}
				
				// Now, the actual work starts :-)
				int offsetOutOfRangeCount = 0;
				fetchLoop: while (running) {
					FetchRequestBuilder frb = new FetchRequestBuilder();
					frb.clientId(clientId);
					frb.maxWait(maxWait);
					frb.minBytes(minBytes);
					
					for (FetchPartition fp : partitions) {
						frb.addFetch(fp.topic, fp.partition, fp.nextOffsetToRead, fetchSize);
					}
					kafka.api.FetchRequest fetchRequest = frb.build();
					LOG.debug("Issuing fetch request {}", fetchRequest);

					FetchResponse fetchResponse = consumer.fetch(fetchRequest);

					if (fetchResponse.hasError()) {
						String exception = "";
						List<FetchPartition> partitionsToGetOffsetsFor = new ArrayList<>();
						for (FetchPartition fp : partitions) {
							short code = fetchResponse.errorCode(fp.topic, fp.partition);

							if (code == ErrorMapping.OffsetOutOfRangeCode()) {
								// we were asked to read from an out-of-range-offset (maybe set wrong in Zookeeper)
								// Kafka's high level consumer is resetting the offset according to 'auto.offset.reset'
								partitionsToGetOffsetsFor.add(fp);
							} else if (code != ErrorMapping.NoError()) {
								exception += "\nException for partition " + fp.partition + ": " +
										StringUtils.stringifyException(ErrorMapping.exceptionFor(code));
							}
						}
						if (partitionsToGetOffsetsFor.size() > 0) {
							// safeguard against an infinite loop.
							if (offsetOutOfRangeCount++ > 0) {
								throw new RuntimeException("Found invalid offsets more than once in partitions "+partitionsToGetOffsetsFor.toString()+" " +
										"Exceptions: "+exception);
							}
							// get valid offsets for these partitions and try again.
							LOG.warn("The following partitions had an invalid offset: {}", partitionsToGetOffsetsFor);
							getLastOffset(consumer, partitionsToGetOffsetsFor, getInvalidOffsetBehavior(config));
							LOG.warn("The new partition offsets are {}", partitionsToGetOffsetsFor);
							continue; // jump back to create a new fetch request. The offset has not been touched.
						} else {
							// all partitions failed on an error
							throw new IOException("Error while fetching from broker: " + exception);
						}
					}

					int messagesInFetch = 0;
					int deletedMessages = 0;
					for (FetchPartition fp : partitions) {
						final ByteBufferMessageSet messageSet = fetchResponse.messageSet(fp.topic, fp.partition);
						final KafkaTopicPartition topicPartition = new KafkaTopicPartition(fp.topic, fp.partition);
						
						for (MessageAndOffset msg : messageSet) {
							if (running) {
								messagesInFetch++;
								if (msg.offset() < fp.nextOffsetToRead) {
									// we have seen this message already
									LOG.info("Skipping message with offset " + msg.offset()
											+ " because we have seen messages until " + fp.nextOffsetToRead
											+ " from partition " + fp.partition + " already");
									continue;
								}

								final long offset = msg.offset();

								ByteBuffer payload = msg.message().payload();

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

								final T value = deserializer.deserialize(keyBytes, valueBytes, fp.topic, fp.partition, offset);
								if(deserializer.isEndOfStream(value)) {
									running = false;
									break fetchLoop; // leave running loop
								}
								synchronized (sourceContext.getCheckpointLock()) {
									sourceContext.collect(value);
									offsetsState.put(topicPartition, offset);
								}
								
								// advance offset for the next request
								fp.nextOffsetToRead = offset + 1;
							}
							else {
								// no longer running
								return;
							}
						}
					}
					LOG.debug("This fetch contained {} messages ({} deleted messages)", messagesInFetch, deletedMessages);
				}
			}
			catch (Throwable t) {
				// report to the main thread
				owner.stopWithError(t);
			}
			finally {
				// end of run loop. close connection to consumer
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

		/**
		 * Request latest offsets for a set of partitions, via a Kafka consumer.
		 *
		 * @param consumer The consumer connected to lead broker
		 * @param partitions The list of partitions we need offsets for
		 * @param whichTime The type of time we are requesting. -1 and -2 are special constants (See OffsetRequest)
		 */
		private static void getLastOffset(SimpleConsumer consumer, List<FetchPartition> partitions, long whichTime) {

			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
			for (FetchPartition fp: partitions) {
				TopicAndPartition topicAndPartition = new TopicAndPartition(fp.topic, fp.partition);
				requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
			}

			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
			OffsetResponse response = consumer.getOffsetsBefore(request);

			if (response.hasError()) {
				String exception = "";
				for (FetchPartition fp: partitions) {
					short code;
					if ( (code=response.errorCode(fp.topic, fp.partition)) != ErrorMapping.NoError()) {
						exception += "\nException for partition "+fp.partition+": "+ StringUtils.stringifyException(ErrorMapping.exceptionFor(code));
					}
				}
				throw new RuntimeException("Unable to get last offset for partitions " + partitions + ". " + exception);
			}

			for (FetchPartition fp: partitions) {
				// the resulting offset is the next offset we are going to read
				// for not-yet-consumed partitions, it is 0.
				fp.nextOffsetToRead = response.offsets(fp.topic, fp.partition)[0];
			}
		}

		private static long getInvalidOffsetBehavior(Properties config) {
			long timeType;
			if (config.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest").equals("latest")) {
				timeType = OffsetRequest.LatestTime();
			} else {
				timeType = OffsetRequest.EarliestTime();
			}
			return timeType;
		}
	}


	private static class PartitionInfoFetcher extends Thread {

		private final List<String> topics;
		private final Properties properties;

		private volatile List<KafkaTopicPartitionLeader> result;
		private volatile Throwable error;


		PartitionInfoFetcher(List<String> topics, Properties properties) {
			this.topics = topics;
			this.properties = properties;
		}

		@Override
		public void run() {
			try {
				result = FlinkKafkaConsumer08.getPartitionsForTopic(topics, properties);
			}
			catch (Throwable t) {
				this.error = t;
			}
		}

		public List<KafkaTopicPartitionLeader> getPartitions() throws Exception {
			try {
				this.join();
			}
			catch (InterruptedException e) {
				throw new Exception("Partition fetching was cancelled before completion");
			}

			if (error != null) {
				throw new Exception("Failed to fetch partitions for topics " + topics.toString(), error);
			}
			if (result != null) {
				return result;
			}
			throw new Exception("Partition fetching failed");
		}
	}

	private static class KillerWatchDog extends Thread {

		private final Thread toKill;
		private final long timeout;

		private KillerWatchDog(Thread toKill, long timeout) {
			super("KillerWatchDog");
			setDaemon(true);

			this.toKill = toKill;
			this.timeout = timeout;
		}

		@SuppressWarnings("deprecation")
		@Override
		public void run() {
			final long deadline = System.currentTimeMillis() + timeout;
			long now;

			while (toKill.isAlive() && (now = System.currentTimeMillis()) < deadline) {
				try {
					toKill.join(deadline - now);
				}
				catch (InterruptedException e) {
					// ignore here, our job is important!
				}
			}

			// this is harsh, but this watchdog is a last resort
			if (toKill.isAlive()) {
				toKill.stop();
			}
		}
	}
}
