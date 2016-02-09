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
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

	
	/** Reference the the thread that executed the run() method. */
	private volatile Thread mainThread;
	
	/** Flag to shot the fetcher down */
	private volatile boolean running = true;

	/**
	 * Queue of partitions which need to find a (new) leader. Elements are added to the queue
	 * from the consuming threads. The LegacyFetcher thread is finding new leaders and assigns the partitions
	 * to the respective threads.
	 */
	private final ClosableBlockingQueue<FetchPartition> unassignedPartitions = new ClosableBlockingQueue<>();

	/**
	 * Create a LegacyFetcher instance.
	 *
	 * @param initialPartitionsToRead Map of partitions to read. The offset passed is the last-fetched-offset (not the next-offset-to-fetch).
	 * @param props kafka properties
	 * @param taskName name of the parent task
	 * @param userCodeClassloader classloader for loading user code
	 */
	public LegacyFetcher(
				Map<KafkaTopicPartition, Long> initialPartitionsToRead, Properties props,
				String taskName, ClassLoader userCodeClassloader) {
		
		this.config = requireNonNull(props, "The config properties cannot be null");
		this.userCodeClassloader = requireNonNull(userCodeClassloader);
		if(initialPartitionsToRead.size() == 0) {
			throw new IllegalArgumentException("List of initial partitions is empty");
		}

		try {
			for(Map.Entry<KafkaTopicPartition, Long> partitionToRead: initialPartitionsToRead.entrySet()) {
				KafkaTopicPartition ktp = partitionToRead.getKey();
				// we increment the offset by one so that we fetch the next message in the partition.
				long offset = partitionToRead.getValue();
				if(offset >= 0 && offset != FlinkKafkaConsumer08.OFFSET_NOT_SET) {
					offset += 1L;
				}
				unassignedPartitions.addIfOpen(new FetchPartition(ktp.getTopic(), ktp.getPartition(), offset));
			}
		} catch (IllegalStateException e) {
			throw new RuntimeException("Fetcher initialization got interrupted", e);
		}
		this.taskName = taskName;
		this.error = new AtomicReference<>();
	}

	// ------------------------------------------------------------------------
	//  Fetcher methods
	// ------------------------------------------------------------------------


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

		// NOTE: This method needs to always release all resources it acquires

		this.mainThread = Thread.currentThread();

		// keep presumably dead threads in the list until we are sure the thread is not alive anymore.
		List<SimpleConsumerThread<T>> deadBrokerThreads = new ArrayList<>();
		Map<Node, SimpleConsumerThread<T>> brokerToThread = new HashMap<>();
		try {
			// Main loop polling elements from the unassignedPartitions queue to the threads
			while (running && error.get() == null) {
				try {
					// wait for 5 seconds trying to get partitions to assign
					List<FetchPartition> partitionsToAssign = unassignedPartitions.getBatchBlocking(5000);
					if(!partitionsToAssign.isEmpty()) {
						LOG.info("Assigning {} partitions to broker threads", partitionsToAssign.size());
						Map<Node, List<FetchPartition>> partitionsWithLeaders = findLeaderForPartitions(partitionsToAssign);

						// assign the partitions to the leaders (maybe start the threads)
						for (Map.Entry<Node, List<FetchPartition>> partitionsWithLeader : partitionsWithLeaders.entrySet()) {
							final Node leader = partitionsWithLeader.getKey();
							final List<FetchPartition> partitions = partitionsWithLeader.getValue();
							SimpleConsumerThread<T> brokerThread = brokerToThread.get(leader);
							// TODO: maybe add a check again here if we are still running.
							if (brokerThread == null) {
								// start new thread
								brokerThread = createAndStartSimpleConsumerThread(sourceContext, deserializer, lastOffsets, partitions, leader);
								brokerToThread.put(leader, brokerThread);
							} else {
								if(brokerThread.isAlive()) {
									// put elements into queue of thread
									ClosableBlockingQueue<FetchPartition> newPartitionsQueue = brokerThread.getNewPartitionsQueue();
									for (FetchPartition fp : partitions) {
										if (!newPartitionsQueue.addIfOpen(fp)) {
											// we were unable to add the partition to the broker's queue
											// the broker has closed in the meantime (the thread will shut down)
											// create a new thread for connecting to this broker
											List<FetchPartition> seedPartitions = new ArrayList<>();
											seedPartitions.add(fp);
											brokerThread = createAndStartSimpleConsumerThread(sourceContext, deserializer, lastOffsets, seedPartitions, leader);
											SimpleConsumerThread<T> oldThread = brokerToThread.put(leader, brokerThread);
											if(oldThread != null) {
												deadBrokerThreads.add(oldThread);
											}
											newPartitionsQueue = brokerThread.getNewPartitionsQueue(); // update queue for the subsequent partitions
										}
									}
								} else {
									// broker shut down in the meantime. Start it:
									brokerThread = createAndStartSimpleConsumerThread(sourceContext, deserializer, lastOffsets, partitions, leader);
									SimpleConsumerThread<T> oldThread = brokerToThread.put(leader, brokerThread);
									if(oldThread != null) {
										deadBrokerThreads.add(oldThread);
									}
								}
							}
						}
					} else {
						// there were no partitions to assign. Check if any broker threads shut down.
						Iterator<Map.Entry<Node, SimpleConsumerThread<T>>> bttIterator = brokerToThread.entrySet().iterator();
						while(bttIterator.hasNext()) {
							Map.Entry<Node, SimpleConsumerThread<T>> brokerAndThread = bttIterator.next();
							if(!brokerAndThread.getValue().isAlive()) {
								LOG.info("Removing stopped consumer thread {}", brokerAndThread.getValue().getName());
								bttIterator.remove();
							}
						}
					}

					if(deadBrokerThreads.size() > 0) {
						// see how the dead brokers are doing
						Iterator<SimpleConsumerThread<T>> deadBrokerThreadsIterator = deadBrokerThreads.iterator();
						while(deadBrokerThreadsIterator.hasNext()) {
							if(!deadBrokerThreadsIterator.next().isAlive()) {
								deadBrokerThreadsIterator.remove();
							}
						}
					}

					if(brokerToThread.size() == 0 && unassignedPartitions.peek() == null) {
						if(unassignedPartitions.close()) {
							LOG.info("All consumer threads are finished, there are no more unassigned partitions. Stopping fetcher");
							break;
						}
						// we end up here if somebody added something to the queue in the meantime --> continue to poll queue again
					}
				} catch (InterruptedException e) {
					// ignore. we should notice what happened in the next loop check
				}
			}

			// make sure any asynchronous error is noticed
			Throwable error = this.error.get();
			if (error != null) {
				throw new Exception(error.getMessage(), error);
			}
		} finally {
			// make sure that in any case (completion, abort, error), all spawned threads are stopped
			int runningThreads;
			do {
				runningThreads = 0;
				for (SimpleConsumerThread<?> t : brokerToThread.values()) {
					if (t.isAlive()) {
						t.cancel();
						runningThreads++;
					}
				}

				// also ensure shutdown of dead brokers:
				if (deadBrokerThreads.size() > 0) {
					// see how the dead brokers are doing
					for (SimpleConsumerThread<T> thread : deadBrokerThreads) {
						if (thread.isAlive()) {
							thread.cancel();
							runningThreads++;
						}
					}
				}
				if(runningThreads > 0) {
					Thread.sleep(500);
				}
			} while(runningThreads > 0);
		}
	}

	private <T> SimpleConsumerThread<T> createAndStartSimpleConsumerThread(SourceFunction.SourceContext<T> sourceContext,
																		KeyedDeserializationSchema<T> deserializer,
																		HashMap<KafkaTopicPartition, Long> lastOffsets,
																		List<FetchPartition> seedPartitions, Node leader) throws IOException, ClassNotFoundException {
		SimpleConsumerThread<T> brokerThread;
		final KeyedDeserializationSchema<T> clonedDeserializer =
				InstantiationUtil.clone(deserializer, userCodeClassloader);

		// seed thread with list of fetch partitions (otherwise it would shut down immediately again
		brokerThread = new SimpleConsumerThread<>(this, config,
				leader, seedPartitions, unassignedPartitions, sourceContext, clonedDeserializer, lastOffsets);

		brokerThread.setName(String.format("SimpleConsumer - %s - broker-%s (%s:%d)",
				taskName, leader.id(), leader.host(), leader.port()));
		brokerThread.setDaemon(true);
		brokerThread.start();
		LOG.info("Starting thread {}", brokerThread.getName());
		return brokerThread;
	}

	/**
	 * Find leaders for the partitions
	 *
	 * @param partitionsToAssign fetch partitions list
	 * @return leader to partitions map
	 */
	private Map<Node, List<FetchPartition>> findLeaderForPartitions(List<FetchPartition> partitionsToAssign) throws Exception {
		if(partitionsToAssign.size() == 0) {
			throw new IllegalArgumentException("Leader request for empty partitions list");
		}

		LOG.info("Refreshing leader information for partitions {}", partitionsToAssign);
		// NOTE: The kafka client apparently locks itself in an infinite loop sometimes
		// when it is interrupted, so we run it only in a separate thread.
		// since it sometimes refuses to shut down, we resort to the admittedly harsh
		// means of killing the thread after a timeout.
		PartitionInfoFetcher infoFetcher = new PartitionInfoFetcher(getTopics(partitionsToAssign), config);
		infoFetcher.start();

		KillerWatchDog watchDog = new KillerWatchDog(infoFetcher, 60000);
		watchDog.start();

		List<KafkaTopicPartitionLeader> topicPartitionWithLeaderList = infoFetcher.getPartitions();

		LOG.debug("topic partitions with leader list {}", topicPartitionWithLeaderList);
		// create new list to remove elements from
		List<FetchPartition> partitionsToAssignInternal = new ArrayList<>(partitionsToAssign);
		Map<Node, List<FetchPartition>> leaderToPartitions = new HashMap<>();
		for(KafkaTopicPartitionLeader partitionLeader: topicPartitionWithLeaderList) {
			if(partitionsToAssignInternal.size() == 0) {
				// we are done: all partitions are assigned
				break;
			}
			Iterator<FetchPartition> fpIter = partitionsToAssignInternal.iterator();
			while(fpIter.hasNext()) {
				FetchPartition fp = fpIter.next();
				if(fp.topic.equals(partitionLeader.getTopicPartition().getTopic())
						&& fp.partition == partitionLeader.getTopicPartition().getPartition()) {
					// we found the leader for one of the fetch partitions
					Node leader = partitionLeader.getLeader();
					List<FetchPartition> partitionsOfLeader = leaderToPartitions.get(leader);
					if(partitionsOfLeader == null) {
						partitionsOfLeader = new ArrayList<>();
						leaderToPartitions.put(leader, partitionsOfLeader);
					}
					partitionsOfLeader.add(fp);
					fpIter.remove();
					break;
				}
			}
		}
		if(partitionsToAssignInternal.size() > 0) {
			throw new RuntimeException("Unable to find a leader for partitions: " + partitionsToAssignInternal);
		}

		LOG.debug("Partitions with assigned leaders {}", leaderToPartitions);

		return leaderToPartitions;
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

		private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumerThread.class);
		
		private final SourceFunction.SourceContext<T> sourceContext;
		private final KeyedDeserializationSchema<T> deserializer;
		private final HashMap<KafkaTopicPartition, Long> offsetsState;
		
		private final List<FetchPartition> partitions;
		
		private final Node broker;

		private final Properties config;

		private final LegacyFetcher owner;

		private final ClosableBlockingQueue<FetchPartition> unassignedPartitions;



		private volatile boolean running = true;

		/** Queue containing new fetch partitions for the consumer thread */
		private final ClosableBlockingQueue<FetchPartition> newPartitionsQueue = new ClosableBlockingQueue<>();

		// ----------------- Simple Consumer ----------------------
		private SimpleConsumer consumer;

		private final int soTimeout;
		private final int minBytes;
		private final int maxWait;
		private final int fetchSize;
		private final int bufferSize;
		private final int reconnectLimit;


		// exceptions are thrown locally
		public SimpleConsumerThread(LegacyFetcher owner,
									Properties config,
									Node broker,
									List<FetchPartition> seedPartitions,
									ClosableBlockingQueue<FetchPartition> unassignedPartitions,
									SourceFunction.SourceContext<T> sourceContext,
									KeyedDeserializationSchema<T> deserializer,
									HashMap<KafkaTopicPartition, Long> offsetsState) {
			this.owner = owner;
			this.config = config;
			this.broker = broker;
			this.partitions = seedPartitions;
			this.sourceContext = requireNonNull(sourceContext);
			this.deserializer = requireNonNull(deserializer);
			this.offsetsState = requireNonNull(offsetsState);
			this.unassignedPartitions = requireNonNull(unassignedPartitions);

			this.soTimeout = Integer.valueOf(config.getProperty("socket.timeout.ms", "30000"));
			this.minBytes = Integer.valueOf(config.getProperty("fetch.min.bytes", "1"));
			this.maxWait = Integer.valueOf(config.getProperty("fetch.wait.max.ms", "100"));
			this.fetchSize = Integer.valueOf(config.getProperty("fetch.message.max.bytes", "1048576"));
			this.bufferSize = Integer.valueOf(config.getProperty("socket.receive.buffer.bytes", "65536"));
			this.reconnectLimit = Integer.valueOf(config.getProperty("flink.simple-consumer-reconnectLimit", "3"));
		}

		@Override
		public void run() {
			LOG.info("Starting to fetch from {}", this.partitions);

			// set up the config values
			final String clientId = "flink-kafka-consumer-legacy-" + broker.id();
			int reconnects = 0;
			// these are the actual configuration values of Kafka + their original default values.

			try {
				// create the Kafka consumer that we actually use for fetching
				consumer = new SimpleConsumer(broker.host(), broker.port(), soTimeout, bufferSize, clientId);

				// make sure that all partitions have some offsets to start with
				// those partitions that do not have an offset from a checkpoint need to get
				// their start offset from ZooKeeper
				getMissingOffsetsFromKafka(partitions);

				// Now, the actual work starts :-)
				int offsetOutOfRangeCount = 0;
				while (running) {

					// ----------------------------------- partitions list maintenance ----------------------------

					// check queue for new partitions to read from:
					List<FetchPartition> newPartitions = newPartitionsQueue.pollBatch();
					if(newPartitions != null) {
						// only this thread is taking elements from the queue, so the next call will never block

						// check if the new partitions need an offset lookup
						getMissingOffsetsFromKafka(newPartitions);
						// add the new partitions (and check they are not already in there)
						for(FetchPartition newPartition: newPartitions) {
							if(partitions.contains(newPartition)) {
								throw new IllegalStateException("Adding partition " + newPartition + " to subscribed partitions even though it is already subscribed");
							}
							partitions.add(newPartition);
						}
						LOG.info("Adding {} new partitions to consumer thread {}", newPartitions.size(), getName());
						if(LOG.isDebugEnabled()) {
							LOG.debug("Partitions list: {}", newPartitions);
						}
					}

					if(partitions.size() == 0) {
						if(newPartitionsQueue.close()) {
							// close succeeded. Closing thread
							LOG.info("Consumer thread {} does not have any partitions assigned anymore. Stopping thread.", getName());
							running = false;
							break;
						} else {
							// close failed: LegacyFetcher main thread added new partitions into the queue.
							continue; // go to top of loop again and get the new partitions
						}
					}

					// ----------------------------------- request / response with kafka ----------------------------

					FetchRequestBuilder frb = new FetchRequestBuilder();
					frb.clientId(clientId);
					frb.maxWait(maxWait);
					frb.minBytes(minBytes);
					
					for (FetchPartition fp : partitions) {
						frb.addFetch(fp.topic, fp.partition, fp.nextOffsetToRead, fetchSize);
					}
					kafka.api.FetchRequest fetchRequest = frb.build();
					LOG.debug("Issuing fetch request {}", fetchRequest);

					FetchResponse fetchResponse;
					try {
						fetchResponse = consumer.fetch(fetchRequest);
					} catch(Throwable cce) {
						//noinspection ConstantConditions
						if(cce instanceof ClosedChannelException) {
							LOG.warn("Fetch failed because of ClosedChannelException.");
							LOG.debug("Full exception", cce);
							// we don't know if the broker is overloaded or unavailable.
							// retry a few times, then return ALL partitions for new leader lookup
							if(++reconnects >= reconnectLimit) {
								LOG.warn("Unable to reach broker after {} retries. Returning all current partitions", reconnectLimit);
								for(FetchPartition fp: this.partitions) {
									if(!unassignedPartitions.addIfOpen(fp)) {
										throw new RuntimeException("Main thread closed unassigned partitions queue");
									}
								}
								this.partitions.clear();
								continue; // jump to top of loop: will close thread or subscribe to new partitions
							}
							try {
								consumer.close();
							} catch(Throwable t) {
								LOG.warn("Error while closing consumer connection", t);
							}
							// delay & retry
							Thread.sleep(500);
							consumer = new SimpleConsumer(broker.host(), broker.port(), soTimeout, bufferSize, clientId);
							continue; // retry
						} else {
							throw cce;
						}
					}
					reconnects = 0;

					// ---------------------------------------- error handling ----------------------------

					if(fetchResponse == null) {
						throw new RuntimeException("Fetch failed");
					}
					if (fetchResponse.hasError()) {
						String exception = "";
						List<FetchPartition> partitionsToGetOffsetsFor = new ArrayList<>();
						// iterate over partitions to get individual error codes
						Iterator<FetchPartition> partitionsIterator = partitions.iterator();
						boolean partitionsRemoved = false;
						while(partitionsIterator.hasNext()) {
							final FetchPartition fp = partitionsIterator.next();
							short code = fetchResponse.errorCode(fp.topic, fp.partition);

							if (code == ErrorMapping.OffsetOutOfRangeCode()) {
								// we were asked to read from an out-of-range-offset (maybe set wrong in Zookeeper)
								// Kafka's high level consumer is resetting the offset according to 'auto.offset.reset'
								partitionsToGetOffsetsFor.add(fp);
							} else if(code == ErrorMapping.NotLeaderForPartitionCode() ||
									code == ErrorMapping.LeaderNotAvailableCode() ||
									code == ErrorMapping.BrokerNotAvailableCode() ||
									code == ErrorMapping.UnknownCode()) {
								// the broker we are connected to is not the leader for the partition.
								LOG.warn("{} is not the leader of {}. Reassigning leader for partition", broker, fp);
								LOG.debug("Error code = {}", code);
								if(!unassignedPartitions.addIfOpen(fp)) {
									throw new RuntimeException("Main thread closed unassigned partitions queue");
								}
								partitionsIterator.remove(); // unsubscribe the partition ourselves
								partitionsRemoved = true;
							} else if (code != ErrorMapping.NoError()) {
								exception += "\nException for " + fp.topic +":"+ fp.partition + ": " +
										StringUtils.stringifyException(ErrorMapping.exceptionFor(code));
							}
						}
						if (partitionsToGetOffsetsFor.size() > 0) {
							// safeguard against an infinite loop.
							if (offsetOutOfRangeCount++ > 3) {
								throw new RuntimeException("Found invalid offsets more than three times in partitions "+partitionsToGetOffsetsFor.toString()+" " +
										"Exceptions: "+exception);
							}
							// get valid offsets for these partitions and try again.
							LOG.warn("The following partitions had an invalid offset: {}", partitionsToGetOffsetsFor);
							getLastOffset(consumer, partitionsToGetOffsetsFor, getInvalidOffsetBehavior(config));
							LOG.warn("The new partition offsets are {}", partitionsToGetOffsetsFor);
							continue; // jump back to create a new fetch request. The offset has not been touched.
						} else if(partitionsRemoved) {
							continue; // create new fetch request
						} else {
							// partitions failed on an error
							throw new IOException("Error while fetching from broker '" + broker +"':" + exception);
						}
					} else {
						// successful fetch, reset offsetOutOfRangeCount.
						offsetOutOfRangeCount = 0;
					}

					// ----------------------------------- process fetch response ----------------------------

					int messagesInFetch = 0;
					int deletedMessages = 0;
					Iterator<FetchPartition> partitionsIterator = partitions.iterator();
					partitionsLoop: while (partitionsIterator.hasNext()) {
						final FetchPartition fp = partitionsIterator.next();
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
									// remove partition from subscribed partitions.
									partitionsIterator.remove();
									continue partitionsLoop;
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
				} // end of fetch loop
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

		private void getMissingOffsetsFromKafka(List<FetchPartition> partitions) {
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

		public ClosableBlockingQueue<FetchPartition> getNewPartitionsQueue() {
			return newPartitionsQueue;
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
			String val = config.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
			if (val.equals("largest") || val.equals("latest")) { // largest is kafka 0.8, latest is kafka 0.9
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

	/**
	 * Returns a unique list of topics from the topic partition list
	 *
	 * @param partitionsList A lost of FetchPartitions's
	 * @return A unique list of topics from the input map
	 */
	public static List<String> getTopics(List<FetchPartition> partitionsList) {
		HashSet<String> uniqueTopics = new HashSet<>();
		for (FetchPartition fp: partitionsList) {
			uniqueTopics.add(fp.topic);
		}
		return new ArrayList<>(uniqueTopics);
	}
}
