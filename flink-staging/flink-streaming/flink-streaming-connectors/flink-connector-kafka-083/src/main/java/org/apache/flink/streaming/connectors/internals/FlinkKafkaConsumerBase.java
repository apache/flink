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

import kafka.common.TopicAndPartition;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.kafka.copied.clients.consumer.ConsumerConfig;
import org.apache.kafka.copied.clients.consumer.KafkaConsumer;
import org.apache.kafka.copied.common.PartitionInfo;
import org.apache.kafka.copied.common.TopicPartition;
import org.apache.kafka.copied.common.serialization.ByteArrayDeserializer;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * When using the legacy fetcher, the following additional configuration values are available:
 *
 * - socket.timeout.ms
 * - socket.receive.buffer.bytes
 * - fetch.message.max.bytes
 * - auto.offset.reset with the values "latest", "earliest" (unlike 0.8.2 behavior)
 * - flink.kafka.consumer.queue.size (Size of the queue between the fetching threads)
 * - fetch.wait.max.ms
 */
public abstract class FlinkKafkaConsumerBase<T> extends RichParallelSourceFunction<T>
		implements CheckpointNotifier, CheckpointedAsynchronously<long[]>, ResultTypeQueryable {

	public static Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumerBase.class);

	static final long OFFSET_NOT_SET = -1L;


	private final String topic;
	private final Properties props;
	private final int[] partitions; // ordered, immutable partition assignment list
	private final DeserializationSchema<T> valueDeserializer;

	private transient Fetcher fetcher;
	private final LinkedMap pendingCheckpoints = new LinkedMap();
	private long[] lastOffsets;
	protected long[] commitedOffsets;
	private ZkClient zkClient;
	private long[] restoreToOffset;
	protected OffsetStore offsetStore = OffsetStore.FLINK_ZOOKEEPER;
	protected FetcherType fetcherType = FetcherType.LEGACY;
	private boolean isNoOp = false; // no-operation instance (for example when there are fewer partitions that flink consumers)
	private boolean closed = false;

	public enum OffsetStore {
		FLINK_ZOOKEEPER,
		/*
		Let Flink manage the offsets. It will store them in Zookeeper, in the same structure as Kafka 0.8.2.x
		Use this mode when using the source with Kafka 0.8.x brokers
		*/
		KAFKA
		/*
		Use the mechanisms in Kafka to commit offsets. Depending on the Kafka configuration, different mechanism are used (broker coordinator, zookeeper)
		*/
	}

	public enum FetcherType {
		LEGACY,  /* Use this fetcher for Kafka 0.8.1 brokers */
		INCLUDED /* This fetcher works with Kafka 0.8.2 and 0.8.3 */
	}


	public FlinkKafkaConsumerBase(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		this.topic = topic;
		this.props = props; // TODO check for zookeeper properties
		this.valueDeserializer = valueDeserializer;

		// Connect to a broker to get the partitions
		List<PartitionInfo> partitionInfos = getPartitionsForTopic(this.topic, this.props);

		// get initial partitions list. The order of the partitions is important for consistent partition id assignment
		// in restart cases.
		// Note that the source will fail (= job will restart) in case of a broker failure
		partitions = new int[partitionInfos.size()];
		for(int i = 0; i < partitionInfos.size(); i++) {
			partitions[i] = partitionInfos.get(i).partition();
		}
		LOG.info("Topic {} has {} partitions", topic, partitions.length);
	}

	// ----------------------------- Source ------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		// make sure that we take care of the committing
		props.setProperty("enable.auto.commit", "false");

		// create fetcher
		switch(fetcherType){
			case INCLUDED:
				fetcher = new IncludedFetcher(props);
				break;
			case LEGACY:
				fetcher = new LegacyFetcher(topic, props);
				break;
			default:
				throw new RuntimeException("Requested unknown fetcher "+fetcher);
		}

		// tell which partitions we want to subscribe
		List<TopicPartition> partitionsToSub = assignPartitions(this.partitions);
		LOG.info("This instance (id={}) is going to subscribe to partitions {}", getRuntimeContext().getIndexOfThisSubtask(), partitionsToSub);
		if(partitionsToSub.size() == 0) {
			LOG.info("This instance is a no-op instance.");
			isNoOp = true;
			return;
		}
		fetcher.partitionsToRead(partitionsToSub);

		// set up operator state
		lastOffsets = new long[partitions.length];
		Arrays.fill(lastOffsets, OFFSET_NOT_SET);

		// prepare Zookeeper
		if(offsetStore == OffsetStore.FLINK_ZOOKEEPER) {
			String zkConnect = props.getProperty("zookeeper.connect");
			if(zkConnect == null) {
				throw new IllegalArgumentException("Required property 'zookeeper.connect' has not been set");
			}
			zkClient = new ZkClient(zkConnect,
					Integer.valueOf(props.getProperty("zookeeper.session.timeout.ms", "6000")),
					Integer.valueOf(props.getProperty("zookeeper.connection.timeout.ms", "6000")),
					new KafkaZKStringSerializer());
		}
		commitedOffsets = new long[partitions.length];


		// seek to last known pos, from restore request
		if(restoreToOffset != null) {
			LOG.info("Found offsets to restore to: "+Arrays.toString(restoreToOffset));
			for(int i = 0; i < restoreToOffset.length; i++) {
				if(restoreToOffset[i] != OFFSET_NOT_SET) {
					// if this fails because we are not subscribed to the topic, the partition assignment is not deterministic!
					// we set the offset +1 here, because seek() is accepting the next offset to read, but the restore offset is the last read offset
					fetcher.seek(new TopicPartition(topic, i), restoreToOffset[i] + 1);
				}
			}
		} else {
			// no restore request. See what we have in ZK for this consumer group. In the non ZK case, Kafka will take care of this.
			if(offsetStore == OffsetStore.FLINK_ZOOKEEPER) {
				for (TopicPartition tp : partitionsToSub) {
					long offset = getOffset(zkClient, props.getProperty(ConsumerConfig.GROUP_ID_CONFIG), topic, tp.partition());
					if (offset != OFFSET_NOT_SET) {
						LOG.info("Offset for partition {} was set to {} in ZK. Seeking consumer to that position", tp.partition(), offset);
						// the offset in Zookeeper was the last read offset, seek is accepting the next-to-read-offset.
						fetcher.seek(tp, offset + 1);
					}
				}
			}
		}

	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		if(isNoOp) {
			sourceContext.close();
			return;
		}
		fetcher.run(sourceContext, valueDeserializer, lastOffsets);
	}

	@Override
	public void cancel() {
		if(isNoOp) {
			return;
		}
		fetcher.stop();
		fetcher.close();
	}

	@Override
	public void close() throws Exception {
		super.close();
		closed = true;
	}

	@Override
	public TypeInformation getProducedType() {
		return valueDeserializer.getProducedType();
	}


	public List<TopicPartition> assignPartitions(int[] parts) {
		LOG.info("Assigning partitions from "+Arrays.toString(parts));
		List<TopicPartition> partitionsToSub = new ArrayList<TopicPartition>();

		int machine = 0;
		for(int i = 0; i < parts.length; i++) {
			if(machine == getRuntimeContext().getIndexOfThisSubtask()) {
				partitionsToSub.add(new TopicPartition(topic, parts[i]));
			}
			machine++;

			if(machine == getRuntimeContext().getNumberOfParallelSubtasks()) {
				machine = 0;
			}
		}

		return partitionsToSub;
	}


	// ----------------------------- Utilities -------------------------

	/**
	 * Send request to Kafka cluster to get partitions for topic.
	 */
	protected static List<PartitionInfo> getPartitionsForTopic(String topic, Properties properties) {
		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(properties, null,
				new ByteArrayDeserializer(), new ByteArrayDeserializer());

		try {
			List<PartitionInfo> partitions = consumer.partitionsFor(topic);
			if (partitions == null) {
				throw new RuntimeException("The topic " + topic + " does not seem to exist");
			}
			if(partitions.size() == 0) {
				throw new RuntimeException("The topic "+topic+" does not seem to have any partitions");
			}
			return partitions;
		} finally {
			consumer.close();
		}
	}

	// ----------------------------- State ------------------------------

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if(fetcher == null) {
			LOG.info("notifyCheckpointComplete() called on uninitialized source");
			return;
		}
		if(closed) {
			LOG.info("notifyCheckpointComplete() called on closed source");
			return;
		}

		LOG.info("Commit checkpoint {}", checkpointId);

		long[] checkpointOffsets;

		// the map may be asynchronously updates when snapshotting state, so we synchronize
		synchronized (pendingCheckpoints) {
			final int posInMap = pendingCheckpoints.indexOf(checkpointId);
			if (posInMap == -1) {
				LOG.warn("Unable to find pending checkpoint for id {}", checkpointId);
				return;
			}

			checkpointOffsets = (long[]) pendingCheckpoints.remove(posInMap);
			// remove older checkpoints in map:
			if (!pendingCheckpoints.isEmpty()) {
				for(int i = 0; i < posInMap; i++) {
					pendingCheckpoints.remove(0);
				}
			}
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Committing offsets {} to offset store: {}", Arrays.toString(checkpointOffsets), offsetStore);
		}

		if(offsetStore == OffsetStore.FLINK_ZOOKEEPER) {
			setOffsetsInZooKeeper(checkpointOffsets);
		} else {
			Map<TopicPartition, Long> offsetsToCommit = new HashMap<TopicPartition, Long>();
			for(int i = 0; i < checkpointOffsets.length; i++) {
				if(checkpointOffsets[i] != OFFSET_NOT_SET) {
					offsetsToCommit.put(new TopicPartition(topic, i), checkpointOffsets[i]);
				}
			}
			fetcher.commit(offsetsToCommit);
		}
	}

	@Override
	public long[] snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (lastOffsets == null) {
			LOG.warn("State snapshot requested on not yet opened source. Returning null");
			return null;
		}
		if(closed) {
			LOG.info("snapshotState() called on closed source");
			return null;
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Snapshotting state. Offsets: {}, checkpoint id {}, timestamp {}",
					Arrays.toString(lastOffsets), checkpointId, checkpointTimestamp);
		}

		long[] currentOffsets = Arrays.copyOf(lastOffsets, lastOffsets.length);

		// the map may be asynchronously updated when committing to Kafka, so we synchronize
		synchronized (pendingCheckpoints) {
			pendingCheckpoints.put(checkpointId, currentOffsets);
		}

		return currentOffsets;
	}

	@Override
	public void restoreState(long[] restoredOffsets) {
		restoreToOffset = restoredOffsets;
	}

	// ---------- Zookeeper communication ----------------

	private void setOffsetsInZooKeeper(long[] offsets) {
		for (int partition = 0; partition < offsets.length; partition++) {
			long offset = offsets[partition];
			if(offset != OFFSET_NOT_SET) {
				setOffset(partition, offset);
			}
		}
	}

	protected void setOffset(int partition, long offset) {
		// synchronize because notifyCheckpointComplete is called using asynchronous worker threads (= multiple checkpoints might be confirmed concurrently)
		synchronized (commitedOffsets) {
			if(closed) {
				// it might happen that the source has been closed while the asynchronous commit threads waited for committing the offsets.
				LOG.warn("setOffset called on closed source");
				return;
			}
			if(commitedOffsets[partition] < offset) {
				setOffset(zkClient, props.getProperty(ConsumerConfig.GROUP_ID_CONFIG), topic, partition, offset);
				commitedOffsets[partition] = offset;
			} else {
				LOG.debug("Ignoring offset {} for partition {} because it is already committed", offset, partition);
			}
		}
	}

	// the following two methods are static to allow access from the outside as well (Testcases)

	/**
	 * This method's code is based on ZookeeperConsumerConnector.commitOffsetToZooKeeper()
	 */
	public static void setOffset(ZkClient zkClient, String groupId, String topic, int partition, long offset) {
		LOG.info("Setting offset for partition {} of topic {} in group {} to offset {}", partition, topic, groupId, offset);
		TopicAndPartition tap = new TopicAndPartition(topic, partition);
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, tap.topic());
		ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir() + "/" + tap.partition(), Long.toString(offset));
	}

	public static long getOffset(ZkClient zkClient, String groupId, String topic, int partition) {
		TopicAndPartition tap = new TopicAndPartition(topic, partition);
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, tap.topic());
		scala.Tuple2<Option<String>, Stat> data = ZkUtils.readDataMaybeNull(zkClient, topicDirs.consumerOffsetDir() + "/" + tap.partition());
		if(data._1().isEmpty()) {
			return OFFSET_NOT_SET;
		} else {
			return Long.valueOf(data._1().get());
		}
	}

	// ---------------------- Zookeeper Serializer copied from Kafka (because it has private access there)  -----------------
	public static class KafkaZKStringSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			try {
				return ((String) data).getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (bytes == null) {
				return null;
			} else {
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}


}
