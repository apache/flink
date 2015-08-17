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

package org.apache.flink.streaming.connectors.internals;

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
import org.apache.flink.streaming.connectors.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.StringUtils;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This fetcher uses Kafka's low-level API to pull data from a specific
 * set of partitions and offsets for a certain topic.
 * 
 * <p>This code is in parts based on the tutorial code for the low-level Kafka consumer.</p>
 */
public class LegacyFetcher implements Fetcher {
	
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumer.class);

	/** The topic from which this fetcher pulls data */
	private final String topic;
	
	/** The properties that configure the Kafka connection */
	private final Properties config;
	
	/** The task name, to give more readable names to the spawned threads */
	private final String taskName;
	
	/** The first error that occurred in a connection thread */
	private final AtomicReference<Throwable> error;

	/** The partitions that the fetcher should read, with their starting offsets */
	private Map<TopicPartition, Long> partitionsToRead;
	
	/** Reference the the thread that executed the run() method. */
	private volatile Thread mainThread;
	
	/** Flag to shot the fetcher down */
	private volatile boolean running = true;

	public LegacyFetcher(String topic, Properties props, String taskName) {
		this.config = checkNotNull(props, "The config properties cannot be null");
		this.topic = checkNotNull(topic, "The topic cannot be null");
		this.taskName = taskName;
		this.error = new AtomicReference<>();
	}

	// ------------------------------------------------------------------------
	//  Fetcher methods
	// ------------------------------------------------------------------------
	
	@Override
	public void setPartitionsToRead(List<TopicPartition> partitions) {
		partitionsToRead = new HashMap<>(partitions.size());
		for (TopicPartition tp: partitions) {
			partitionsToRead.put(tp, FlinkKafkaConsumer.OFFSET_NOT_SET);
		}
	}

	@Override
	public void seek(TopicPartition topicPartition, long offsetToRead) {
		if (partitionsToRead == null) {
			throw new IllegalArgumentException("No partitions to read set");
		}
		if (!partitionsToRead.containsKey(topicPartition)) {
			throw new IllegalArgumentException("Can not set offset on a partition (" + topicPartition
					+ ") we are not going to read. Partitions to read " + partitionsToRead);
		}
		partitionsToRead.put(topicPartition, offsetToRead);
	}
	
	@Override
	public void close() {
		// flag needs to be check by the run() method that creates the spawned threads
		this.running = false;
		
		// all other cleanup is made by the run method itself
	}

	@Override
	public <T> void run(SourceFunction.SourceContext<T> sourceContext, 
						DeserializationSchema<T> valueDeserializer,
						long[] lastOffsets) throws Exception {
		
		if (partitionsToRead == null || partitionsToRead.size() == 0) {
			throw new IllegalArgumentException("No partitions set");
		}
		
		// NOTE: This method is needs to always release all resources it acquires
		
		this.mainThread = Thread.currentThread();

		LOG.info("Reading from partitions " + partitionsToRead + " using the legacy fetcher");
		
		// get lead broker for each partition
		
		// NOTE: The kafka client apparently locks itself in an infinite loop sometimes
		// when it is interrupted, so we run it only in a separate thread.
		// since it sometimes refuses to shut down, we resort to the admittedly harsh
		// means of killing the thread after a timeout.
		PartitionInfoFetcher infoFetcher = new PartitionInfoFetcher(topic, config);
		infoFetcher.start();
		
		KillerWatchDog watchDog = new KillerWatchDog(infoFetcher, 60000);
		watchDog.start();
		
		final List<PartitionInfo> allPartitionsInTopic = infoFetcher.getPartitions();
		
		// brokers to fetch partitions from.
		int fetchPartitionsCount = 0;
		Map<Node, List<FetchPartition>> fetchBrokers = new HashMap<>();
		
		for (PartitionInfo partitionInfo : allPartitionsInTopic) {
			if (partitionInfo.leader() == null) {
				throw new RuntimeException("Unable to consume partition " + partitionInfo.partition()
						+ " from topic "+partitionInfo.topic()+" because it does not have a leader");
			}
			
			for (Map.Entry<TopicPartition, Long> entry : partitionsToRead.entrySet()) {
				final TopicPartition topicPartition = entry.getKey();
				final long offset = entry.getValue();
				
				// check if that partition is for us
				if (topicPartition.partition() == partitionInfo.partition()) {
					List<FetchPartition> partitions = fetchBrokers.get(partitionInfo.leader());
					if (partitions == null) {
						partitions = new ArrayList<>();
						fetchBrokers.put(partitionInfo.leader(), partitions);
					}
					
					partitions.add(new FetchPartition(topicPartition.partition(), offset));
					fetchPartitionsCount++;
					
				}
				// else this partition is not for us
			}
		}
		
		if (partitionsToRead.size() != fetchPartitionsCount) {
			throw new RuntimeException(partitionsToRead.size() + " partitions to read, but got only "
					+ fetchPartitionsCount + " partition infos with lead brokers.");
		}

		// create SimpleConsumers for each broker
		ArrayList<SimpleConsumerThread<?>> consumers = new ArrayList<>(fetchBrokers.size());
		
		for (Map.Entry<Node, List<FetchPartition>> brokerInfo : fetchBrokers.entrySet()) {
			final Node broker = brokerInfo.getKey();
			final List<FetchPartition> partitionsList = brokerInfo.getValue();
			
			FetchPartition[] partitions = partitionsList.toArray(new FetchPartition[partitionsList.size()]);

			SimpleConsumerThread<T> thread = new SimpleConsumerThread<>(this, config, topic,
					broker, partitions, sourceContext, valueDeserializer, lastOffsets);

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
	void onErrorInFetchThread(Throwable error) {
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
		
		/** ID of the partition within the topic (0 indexed, as given by Kafka) */
		int partition;
		
		/** Offset pointing at the next element to read from that partition. */
		long nextOffsetToRead;

		FetchPartition(int partition, long nextOffsetToRead) {
			this.partition = partition;
			this.nextOffsetToRead = nextOffsetToRead;
		}
		
		@Override
		public String toString() {
			return "FetchPartition {partition=" + partition + ", offset=" + nextOffsetToRead + '}';
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
		private final DeserializationSchema<T> valueDeserializer;
		private final long[] offsetsState;
		
		private final FetchPartition[] partitions;
		
		private final Node broker;
		private final String topic;
		private final Properties config;

		private final LegacyFetcher owner;

		private SimpleConsumer consumer;
		
		private volatile boolean running = true;


		// exceptions are thrown locally
		public SimpleConsumerThread(LegacyFetcher owner,
									Properties config, String topic,
									Node broker,
									FetchPartition[] partitions,
									SourceFunction.SourceContext<T> sourceContext,
									DeserializationSchema<T> valueDeserializer,
									long[] offsetsState) {
			this.owner = owner;
			this.config = config;
			this.topic = topic;
			this.broker = broker;
			this.partitions = partitions;
			this.sourceContext = checkNotNull(sourceContext);
			this.valueDeserializer = checkNotNull(valueDeserializer);
			this.offsetsState = checkNotNull(offsetsState);
		}

		@Override
		public void run() {
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
				
				List<FetchPartition> partitionsToGetOffsetsFor = new ArrayList<>();

				for (FetchPartition fp : partitions) {
					if (fp.nextOffsetToRead == FlinkKafkaConsumer.OFFSET_NOT_SET) {
						// retrieve the offset from the consumer
						partitionsToGetOffsetsFor.add(fp);
					}
				}
				if (partitionsToGetOffsetsFor.size() > 0) {
					long timeType;
					if (config.getProperty("auto.offset.reset", "latest").equals("latest")) {
						timeType = OffsetRequest.LatestTime();
					} else {
						timeType = OffsetRequest.EarliestTime();
					}
					getLastOffset(consumer, topic, partitionsToGetOffsetsFor, timeType);
					LOG.info("No prior offsets found for some partitions in topic {}. Fetched the following start offsets {}",
							topic, partitionsToGetOffsetsFor);
				}
				
				// Now, the actual work starts :-)
				
				while (running) {
					FetchRequestBuilder frb = new FetchRequestBuilder();
					frb.clientId(clientId);
					frb.maxWait(maxWait);
					frb.minBytes(minBytes);
					
					for (FetchPartition fp : partitions) {
						frb.addFetch(topic, fp.partition, fp.nextOffsetToRead, fetchSize);
					}
					kafka.api.FetchRequest fetchRequest = frb.build();
					LOG.debug("Issuing fetch request {}", fetchRequest);

					FetchResponse fetchResponse;
					fetchResponse = consumer.fetch(fetchRequest);

					if (fetchResponse.hasError()) {
						String exception = "";
						for (FetchPartition fp : partitions) {
							short code;
							if ((code = fetchResponse.errorCode(topic, fp.partition)) != ErrorMapping.NoError()) {
								exception += "\nException for partition " + fp.partition + ": " + 
										StringUtils.stringifyException(ErrorMapping.exceptionFor(code));
							}
						}
						throw new IOException("Error while fetching from broker: " + exception);
					}

					int messagesInFetch = 0;
					for (FetchPartition fp : partitions) {
						final ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, fp.partition);
						final int partition = fp.partition;
						
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
								
								ByteBuffer payload = msg.message().payload();
								byte[] valueByte = new byte[payload.remaining()];
								payload.get(valueByte);
								
								final T value = valueDeserializer.deserialize(valueByte);
								final long offset = msg.offset();
										
								synchronized (sourceContext.getCheckpointLock()) {
									sourceContext.collect(value);
									offsetsState[partition] = offset;
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
					LOG.debug("This fetch contained {} messages", messagesInFetch);
				}
			}
			catch (Throwable t) {
				// report to the main thread
				owner.onErrorInFetchThread(t);
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
		 * @param topic The topic name
		 * @param partitions The list of partitions we need offsets for
		 * @param whichTime The type of time we are requesting. -1 and -2 are special constants (See OffsetRequest)
		 */
		private static void getLastOffset(SimpleConsumer consumer, String topic, List<FetchPartition> partitions, long whichTime) {

			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
			for (FetchPartition fp: partitions) {
				TopicAndPartition topicAndPartition = new TopicAndPartition(topic, fp.partition);
				requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
			}

			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
			OffsetResponse response = consumer.getOffsetsBefore(request);

			if (response.hasError()) {
				String exception = "";
				for (FetchPartition fp: partitions) {
					short code;
					if ( (code=response.errorCode(topic, fp.partition)) != ErrorMapping.NoError()) {
						exception += "\nException for partition "+fp.partition+": "+ StringUtils.stringifyException(ErrorMapping.exceptionFor(code));
					}
				}
				throw new RuntimeException("Unable to get last offset for topic " + topic + " and partitions " + partitions
						+ ". " + exception);
			}

			for (FetchPartition fp: partitions) {
				// the resulting offset is the next offset we are going to read
				// for not-yet-consumed partitions, it is 0.
				fp.nextOffsetToRead = response.offsets(topic, fp.partition)[0];
			}
		}
	}
	
	private static class PartitionInfoFetcher extends Thread {

		private final String topic;
		private final Properties properties;
		
		private volatile List<PartitionInfo> result;
		private volatile Throwable error;

		
		PartitionInfoFetcher(String topic, Properties properties) {
			this.topic = topic;
			this.properties = properties;
		}

		@Override
		public void run() {
			try {
				result = FlinkKafkaConsumer.getPartitionsForTopic(topic, properties);
			}
			catch (Throwable t) {
				this.error = t;
			}
		}
		
		public List<PartitionInfo> getPartitions() throws Exception {
			try {
				this.join();
			}
			catch (InterruptedException e) {
				throw new Exception("Partition fetching was cancelled before completion");
			}
			
			if (error != null) {
				throw new Exception("Failed to fetch partitions for topic " + topic, error);
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
