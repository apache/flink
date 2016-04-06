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

package org.apache.flink.streaming.connectors.kafka.internals;

import kafka.common.TopicAndPartition;
import org.apache.kafka.common.Node;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A fetcher that fetches data from Kafka brokers via the Kafka 0.8 low-level consumer API.
 * The fetcher also handles the explicit communication with ZooKeeper to fetch initial offsets
 * and to write offsets to ZooKeeper.
 *
 * @param <T> The type of elements produced by the fetcher.
 */
public class Kafka08Fetcher<T> extends AbstractFetcher<T, TopicAndPartition> {
	
	static final KafkaTopicPartitionState<TopicAndPartition> MARKER = 
			new KafkaTopicPartitionState<>(new KafkaTopicPartition("n/a", -1), new TopicAndPartition("n/a", -1));

	private static final Logger LOG = LoggerFactory.getLogger(Kafka08Fetcher.class);

	// ------------------------------------------------------------------------

	/** The schema to convert between Kafka's byte messages, and Flink's objects */
	private final KeyedDeserializationSchema<T> deserializer;

	/** The properties that configure the Kafka connection */
	private final Properties kafkaConfig;

	/** The task name, to give more readable names to the spawned threads */
	private final String taskName;

	/** The class loader for dynamically loaded classes */
	private final ClassLoader userCodeClassLoader;

	/** The queue of partitions that are currently not assigned to a broker connection */
	private final ClosableBlockingQueue<KafkaTopicPartitionState<TopicAndPartition>> unassignedPartitionsQueue;

	/** The behavior to use in case that an offset is not valid (any more) for a partition */
	private final long invalidOffsetBehavior;

	/** The interval in which to automatically commit (-1 if deactivated) */
	private final long autoCommitInterval; 

	/** The handler that reads/writes offsets from/to ZooKeeper */
	private volatile ZookeeperOffsetHandler zookeeperOffsetHandler;

	/** Flag to track the main work loop as alive */
	private volatile boolean running = true;


	public Kafka08Fetcher(
			SourceContext<T> sourceContext,
			List<KafkaTopicPartition> assignedPartitions,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext,
			KeyedDeserializationSchema<T> deserializer,
			Properties kafkaProperties,
			long invalidOffsetBehavior,
			long autoCommitInterval) throws Exception
	{
		super(sourceContext, assignedPartitions, watermarksPeriodic, watermarksPunctuated, runtimeContext);

		this.deserializer = checkNotNull(deserializer);
		this.kafkaConfig = checkNotNull(kafkaProperties);
		this.taskName = runtimeContext.getTaskNameWithSubtasks();
		this.userCodeClassLoader = runtimeContext.getUserCodeClassLoader();
		this.invalidOffsetBehavior = invalidOffsetBehavior;
		this.autoCommitInterval = autoCommitInterval;
		this.unassignedPartitionsQueue = new ClosableBlockingQueue<>();

		// initially, all these partitions are not assigned to a specific broker connection
		for (KafkaTopicPartitionState<TopicAndPartition> partition : subscribedPartitions()) {
			unassignedPartitionsQueue.add(partition);
		}
	}

	// ------------------------------------------------------------------------
	//  Main Work Loop
	// ------------------------------------------------------------------------

	@Override
	public void runFetchLoop() throws Exception {
		// the map from broker to the thread that is connected to that broker
		final Map<Node, SimpleConsumerThread<T>> brokerToThread = new HashMap<>();

		// this holds possible the exceptions from the concurrent broker connection threads
		final ExceptionProxy errorHandler = new ExceptionProxy(Thread.currentThread());

		// the offset handler handles the communication with ZooKeeper, to commit externally visible offsets
		final ZookeeperOffsetHandler zookeeperOffsetHandler = new ZookeeperOffsetHandler(kafkaConfig);
		this.zookeeperOffsetHandler = zookeeperOffsetHandler;

		PeriodicOffsetCommitter periodicCommitter = null;
		try {
			// read offsets from ZooKeeper for partitions that did not restore offsets
			{
				List<KafkaTopicPartition> partitionsWithNoOffset = new ArrayList<>();
				for (KafkaTopicPartitionState<TopicAndPartition> partition : subscribedPartitions()) {
					if (!partition.isOffsetDefined()) {
						partitionsWithNoOffset.add(partition.getKafkaTopicPartition());
					}
				}

				Map<KafkaTopicPartition, Long> zkOffsets = zookeeperOffsetHandler.getOffsets(partitionsWithNoOffset);
				for (KafkaTopicPartitionState<TopicAndPartition> partition : subscribedPartitions()) {
					Long offset = zkOffsets.get(partition.getKafkaTopicPartition());
					if (offset != null) {
						partition.setOffset(offset);
					}
				}
			}

			// start the periodic offset committer thread, if necessary
			if (autoCommitInterval > 0) {
				LOG.info("Starting periodic offset committer, with commit interval of {}ms", autoCommitInterval);

				periodicCommitter = new PeriodicOffsetCommitter(zookeeperOffsetHandler, 
						subscribedPartitions(), errorHandler, autoCommitInterval);
				periodicCommitter.setName("Periodic Kafka partition offset committer");
				periodicCommitter.setDaemon(true);
				periodicCommitter.start();
			}

			// Main loop polling elements from the unassignedPartitions queue to the threads
			while (running) {
				// re-throw any exception from the concurrent fetcher threads
				errorHandler.checkAndThrowException();

				// wait for max 5 seconds trying to get partitions to assign
				// if threads shut down, this poll returns earlier, because the threads inject the
				// special marker into the queue
				List<KafkaTopicPartitionState<TopicAndPartition>> partitionsToAssign = 
						unassignedPartitionsQueue.getBatchBlocking(5000);
				partitionsToAssign.remove(MARKER);

				if (!partitionsToAssign.isEmpty()) {
					LOG.info("Assigning {} partitions to broker threads", partitionsToAssign.size());
					Map<Node, List<KafkaTopicPartitionState<TopicAndPartition>>> partitionsWithLeaders = 
							findLeaderForPartitions(partitionsToAssign, kafkaConfig);

					// assign the partitions to the leaders (maybe start the threads)
					for (Map.Entry<Node, List<KafkaTopicPartitionState<TopicAndPartition>>> partitionsWithLeader : 
							partitionsWithLeaders.entrySet())
					{
						final Node leader = partitionsWithLeader.getKey();
						final List<KafkaTopicPartitionState<TopicAndPartition>> partitions = partitionsWithLeader.getValue();
						SimpleConsumerThread<T> brokerThread = brokerToThread.get(leader);

						if (!running) {
							break;
						}

						if (brokerThread == null || !brokerThread.getNewPartitionsQueue().isOpen()) {
							// start new thread
							brokerThread = createAndStartSimpleConsumerThread(partitions, leader, errorHandler);
							brokerToThread.put(leader, brokerThread);
						}
						else {
							// put elements into queue of thread
							ClosableBlockingQueue<KafkaTopicPartitionState<TopicAndPartition>> newPartitionsQueue = 
									brokerThread.getNewPartitionsQueue();
							
							for (KafkaTopicPartitionState<TopicAndPartition> fp : partitions) {
								if (!newPartitionsQueue.addIfOpen(fp)) {
									// we were unable to add the partition to the broker's queue
									// the broker has closed in the meantime (the thread will shut down)
									// create a new thread for connecting to this broker
									List<KafkaTopicPartitionState<TopicAndPartition>> seedPartitions = new ArrayList<>();
									seedPartitions.add(fp);
									brokerThread = createAndStartSimpleConsumerThread(seedPartitions, leader, errorHandler);
									brokerToThread.put(leader, brokerThread);
									newPartitionsQueue = brokerThread.getNewPartitionsQueue(); // update queue for the subsequent partitions
								}
							}
						}
					}
				}
				else {
					// there were no partitions to assign. Check if any broker threads shut down.
					// we get into this section of the code, if either the poll timed out, or the
					// blocking poll was woken up by the marker element
					Iterator<SimpleConsumerThread<T>> bttIterator = brokerToThread.values().iterator();
					while (bttIterator.hasNext()) {
						SimpleConsumerThread<T> thread = bttIterator.next();
						if (!thread.getNewPartitionsQueue().isOpen()) {
							LOG.info("Removing stopped consumer thread {}", thread.getName());
							bttIterator.remove();
						}
					}
				}

				if (brokerToThread.size() == 0 && unassignedPartitionsQueue.isEmpty()) {
					if (unassignedPartitionsQueue.close()) {
						LOG.info("All consumer threads are finished, there are no more unassigned partitions. Stopping fetcher");
						break;
					}
					// we end up here if somebody added something to the queue in the meantime --> continue to poll queue again
				}
			}
		}
		catch (InterruptedException e) {
			// this may be thrown because an exception on one of the concurrent fetcher threads
			// woke this thread up. make sure we throw the root exception instead in that case
			errorHandler.checkAndThrowException();

			// no other root exception, throw the interrupted exception
			throw e;
		}
		finally {
			this.running = false;
			this.zookeeperOffsetHandler = null;

			// if we run a periodic committer thread, shut that down
			if (periodicCommitter != null) {
				periodicCommitter.shutdown();
			}

			// make sure that in any case (completion, abort, error), all spawned threads are stopped
			try {
				int runningThreads;
				do {
					// check whether threads are alive and cancel them
					runningThreads = 0;
					Iterator<SimpleConsumerThread<T>> threads = brokerToThread.values().iterator();
					while (threads.hasNext()) {
						SimpleConsumerThread<?> t = threads.next();
						if (t.isAlive()) {
							t.cancel();
							runningThreads++;
						} else {
							threads.remove();
						}
					}

					// wait for the threads to finish, before issuing a cancel call again
					if (runningThreads > 0) {
						for (SimpleConsumerThread<?> t : brokerToThread.values()) {
							t.join(500 / runningThreads + 1);
						}
					}
				}
				while (runningThreads > 0);
			}
			catch (Throwable t) {
				// we catch all here to preserve the original exception
				LOG.error("Exception while shutting down consumer threads", t);
			}

			try {
				zookeeperOffsetHandler.close();
			}
			catch (Throwable t) {
				// we catch all here to preserve the original exception
				LOG.error("Exception while shutting down ZookeeperOffsetHandler", t);
			}
		}
	}

	@Override
	public void cancel() {
		// signal the main thread to exit
		this.running = false;

		// make sure the main thread wakes up soon
		this.unassignedPartitionsQueue.addIfOpen(MARKER);
	}

	// ------------------------------------------------------------------------
	//  Kafka 0.8 specific class instantiation
	// ------------------------------------------------------------------------

	@Override
	public TopicAndPartition createKafkaPartitionHandle(KafkaTopicPartition partition) {
		return new TopicAndPartition(partition.getTopic(), partition.getPartition());
	}

	// ------------------------------------------------------------------------
	//  Offset handling
	// ------------------------------------------------------------------------

	@Override
	public void commitSpecificOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets) throws Exception {
		ZookeeperOffsetHandler zkHandler = this.zookeeperOffsetHandler;
		if (zkHandler != null) {
			zkHandler.writeOffsets(offsets);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private SimpleConsumerThread<T> createAndStartSimpleConsumerThread(
			List<KafkaTopicPartitionState<TopicAndPartition>> seedPartitions,
			Node leader,
			ExceptionProxy errorHandler) throws IOException, ClassNotFoundException
	{
		// each thread needs its own copy of the deserializer, because the deserializer is
		// not necessarily thread safe
		final KeyedDeserializationSchema<T> clonedDeserializer =
				InstantiationUtil.clone(deserializer, userCodeClassLoader);

		// seed thread with list of fetch partitions (otherwise it would shut down immediately again
		SimpleConsumerThread<T> brokerThread = new SimpleConsumerThread<>(
				this, errorHandler, kafkaConfig, leader, seedPartitions, unassignedPartitionsQueue, 
				clonedDeserializer, invalidOffsetBehavior);

		brokerThread.setName(String.format("SimpleConsumer - %s - broker-%s (%s:%d)",
				taskName, leader.id(), leader.host(), leader.port()));
		brokerThread.setDaemon(true);
		brokerThread.start();

		LOG.info("Starting thread {}", brokerThread.getName());
		return brokerThread;
	}

	/**
	 * Returns a list of unique topics from for the given partitions
	 *
	 * @param partitions A the partitions
	 * @return A list of unique topics
	 */
	private static List<String> getTopics(List<KafkaTopicPartitionState<TopicAndPartition>> partitions) {
		HashSet<String> uniqueTopics = new HashSet<>();
		for (KafkaTopicPartitionState<TopicAndPartition> fp: partitions) {
			uniqueTopics.add(fp.getTopic());
		}
		return new ArrayList<>(uniqueTopics);
	}

	/**
	 * Find leaders for the partitions
	 *
	 * From a high level, the method does the following:
	 *	 - Get a list of FetchPartitions (usually only a few partitions)
	 *	 - Get the list of topics from the FetchPartitions list and request the partitions for the topics. (Kafka doesn't support getting leaders for a set of partitions)
	 *	 - Build a Map<Leader, List<FetchPartition>> where only the requested partitions are contained.
	 *
	 * @param partitionsToAssign fetch partitions list
	 * @return leader to partitions map
	 */
	private static Map<Node, List<KafkaTopicPartitionState<TopicAndPartition>>> findLeaderForPartitions(
			List<KafkaTopicPartitionState<TopicAndPartition>> partitionsToAssign,
			Properties kafkaProperties) throws Exception
	{
		if (partitionsToAssign.isEmpty()) {
			throw new IllegalArgumentException("Leader request for empty partitions list");
		}

		LOG.info("Refreshing leader information for partitions {}", partitionsToAssign);
		
		// this request is based on the topic names
		PartitionInfoFetcher infoFetcher = new PartitionInfoFetcher(getTopics(partitionsToAssign), kafkaProperties);
		infoFetcher.start();

		// NOTE: The kafka client apparently locks itself up sometimes
		// when it is interrupted, so we run it only in a separate thread.
		// since it sometimes refuses to shut down, we resort to the admittedly harsh
		// means of killing the thread after a timeout.
		KillerWatchDog watchDog = new KillerWatchDog(infoFetcher, 60000);
		watchDog.start();

		// this list contains ALL partitions of the requested topics
		List<KafkaTopicPartitionLeader> topicPartitionWithLeaderList = infoFetcher.getPartitions();

		// copy list to track unassigned partitions
		List<KafkaTopicPartitionState<TopicAndPartition>> unassignedPartitions = new ArrayList<>(partitionsToAssign);

		// final mapping from leader -> list(fetchPartition)
		Map<Node, List<KafkaTopicPartitionState<TopicAndPartition>>> leaderToPartitions = new HashMap<>();

		for(KafkaTopicPartitionLeader partitionLeader: topicPartitionWithLeaderList) {
			if (unassignedPartitions.size() == 0) {
				// we are done: all partitions are assigned
				break;
			}

			Iterator<KafkaTopicPartitionState<TopicAndPartition>> unassignedPartitionsIterator = unassignedPartitions.iterator();
			while (unassignedPartitionsIterator.hasNext()) {
				KafkaTopicPartitionState<TopicAndPartition> unassignedPartition = unassignedPartitionsIterator.next();

				if (unassignedPartition.getKafkaTopicPartition().equals(partitionLeader.getTopicPartition())) {
					// we found the leader for one of the fetch partitions
					Node leader = partitionLeader.getLeader();

					List<KafkaTopicPartitionState<TopicAndPartition>> partitionsOfLeader = leaderToPartitions.get(leader);
					if (partitionsOfLeader == null) {
						partitionsOfLeader = new ArrayList<>();
						leaderToPartitions.put(leader, partitionsOfLeader);
					}
					partitionsOfLeader.add(unassignedPartition);
					unassignedPartitionsIterator.remove(); // partition has been assigned
					break;
				}
			}
		}

		if (unassignedPartitions.size() > 0) {
			throw new RuntimeException("Unable to find a leader for partitions: " + unassignedPartitions);
		}

		LOG.debug("Partitions with assigned leaders {}", leaderToPartitions);

		return leaderToPartitions;
	}
}
