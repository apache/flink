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

package eu.stratosphere.pact.runtime.hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelReader;
import eu.stratosphere.nephele.services.iomanager.ChannelWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.OutOfMemoryException;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Fallback hash merge strategy which works if the build records don't fit into
 * the main memory. In the {@code initialize()} phase, build records are
 * partitioned in buckets (each smaller than the available hash memory) and are
 * then spilled on the disc, Probe records are repartitioned using the same
 * partitioning function. Upon completion, build / probe partitions are iterated
 * and hash-merged one after the other.
 * 
 * @author Alexander Alexandrov
 */
class PartitioningHashMatchStrategy<K extends Key, VB extends Value, VP extends Value> extends
		HashMatchStrategy<K, VB, VP> {
	// -------------------------------------------------------------------------
	// Constants
	// -------------------------------------------------------------------------

	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(PartitioningHashMatchStrategy.class);

	// -------------------------------------------------------------------------
	// Properties
	// -------------------------------------------------------------------------

	final KeyValuePair<K, VB> overflowCause;

	final Queue<MemorySegment> freeSegments;

	Partitioner partitioner;

	int numberOfPartitions;

	int currentPartition;

	ArrayList<Channel.ID> buildPartitionIDs;

	ArrayList<Channel.ID> probePartitionIDs;

	ArrayList<ChannelWriter> partitionWriters;

	ChannelReader currentBuildPartitionReader;

	ChannelReader currentProbePartitionReader;

	public PartitioningHashMatchStrategy(Reader<KeyValuePair<K, VB>> readerBuild,
			Reader<KeyValuePair<K, VP>> readerProbe, IOManager ioManager, SerializationFactory<K> keySerialization,
			SerializationFactory<VB> buildSerialization, SerializationFactory<VP> probeSerialization,
			SerializingHashMap<K, VB> hashMap, KeyValuePair<K, VB> overflowCause,
			Collection<MemorySegment> bufferSegments) {
		super(readerBuild, readerProbe, ioManager, keySerialization, buildSerialization, probeSerialization, hashMap);
		this.freeSegments = new LinkedList<MemorySegment>(bufferSegments);
		this.overflowCause = overflowCause;
	}

	@Override
	public void initialize() throws ServiceException, IOException, InterruptedException {
		// step 1: initialize partition output buffers
		//
		LOG.debug("initialization #1: setting up initial internal state");

		// calculate number of output buffers (double buffering)
		numberOfPartitions = freeSegments.size() / 2;
		// TODO: construct in client and set in constructor
		partitioner = new Partitioner(numberOfPartitions);

		partitionWriters = new ArrayList<ChannelWriter>(numberOfPartitions);
		buildPartitionIDs = new ArrayList<Channel.ID>(numberOfPartitions);
		probePartitionIDs = new ArrayList<Channel.ID>(numberOfPartitions);

		LOG.debug("initialization #1: using " + numberOfPartitions + " partitions");

		// step 2: repartition build side
		//
		LOG.debug("initialization #2: repartitioning build side");
		Channel.Enumerator buildEnumerator = ioManager.createChannelEnumerator();
		allocatePartitionWriters(buildEnumerator, buildPartitionIDs);
		repartitionBuildSide();
		closePartitionWriters();

		// step 3: repartition probe side
		//
		LOG.debug("initialization #3: repartitioning probe side");
		Channel.Enumerator probeEnumerator = ioManager.createChannelEnumerator();
		allocatePartitionWriters(probeEnumerator, probePartitionIDs);
		repartitionProbeSide();
		closePartitionWriters();

		// step 4: load zero partition completely into main memory prepare initial current readers
		//
		currentPartition = 0;

		currentBuildPartitionReader = ioManager.createChannelReader(buildPartitionIDs.get(currentPartition),
			freeSegments);
		KeyValuePair<K, VB> pair = buildPairSerialization.newInstance();
		while (currentBuildPartitionReader.read(pair)) {
			hashMap.put(pair.getKey(), pair.getValue());
		}
		currentBuildPartitionReader.close();

		currentProbePartitionReader = ioManager.createChannelReader(probePartitionIDs.get(currentPartition),
			freeSegments);
	}

	@Override
	public void close() {
		// release allocated resources
	}

	@Override
	public boolean next() throws IOException, InterruptedException {
		KeyValuePair<K, VP> pair = probePairSerialization.newInstance();

		// there are more pairs in the current partition
		while (currentProbePartitionReader.read(pair)) {
			K key = pair.getKey();

			if (hashMap.contains(key)) {
				currentKey = key;
				currentBuildValuesIterable = hashMap.get(currentKey);
				currentProbeValuesIterable.value = pair.getValue();
				return true;
			}
		}

		// current partition is exhausted, return if no more partitions exist
		if (currentPartition >= numberOfPartitions - 1) {
			return false;
		}

		// more partitions available, load next partition
		try {
			currentProbePartitionReader.close();

			currentPartition++;

			LOG.debug("loading partition " + currentPartition);

			hashMap.clear();

			currentBuildPartitionReader = ioManager.createChannelReader(buildPartitionIDs.get(currentPartition),
				freeSegments);
			KeyValuePair<K, VB> tmp = buildPairSerialization.newInstance();
			while (currentBuildPartitionReader.read(tmp)) {
				hashMap.put(tmp.getKey(), tmp.getValue());
				tmp = buildPairSerialization.newInstance();
			}
			currentBuildPartitionReader.close();

			currentProbePartitionReader = ioManager.createChannelReader(probePartitionIDs.get(currentPartition),
				freeSegments);

			return next();
		} catch (ServiceException e) {
			throw new IOException(e);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * Creates a partition writer for each of the current {@code
	 * numberOfPartitions} using the provided {@code Channel.Enumerator}.
	 * Created writers are appended to the {@code partitionWriters} array and {@code Channel.ID}s for the created
	 * writers are appended to the {@code
	 * partitionIDsMap}. This method assumes that the {@code partitionWriters} array is empty and all available {@code
	 * MemorySegment}s are free.
	 * 
	 * @param enumerator
	 * @param partitionIDMap
	 * @throws ServiceException
	 */
	private void allocatePartitionWriters(Channel.Enumerator enumerator, ArrayList<Channel.ID> partitionIDMap)
			throws IOException {
		for (int i = 0; i < numberOfPartitions; i++) {
			// open next channel
			Channel.ID channel = enumerator.next();
			partitionIDMap.add(channel);

			// allocate buffers for channel
			ArrayList<MemorySegment> channelSegments = new ArrayList<MemorySegment>(2);
			channelSegments.add(freeSegments.poll());
			channelSegments.add(freeSegments.poll());

			// create writer
			partitionWriters.add(ioManager.createChannelWriter(channel, channelSegments));
		}
	}

	/**
	 * Closes all active partition writers, reinserts the released segments to
	 * the {@code freeSegments} array and clears the {@code partitionWriters} array.
	 * 
	 * @throws ServiceException
	 */
	private void closePartitionWriters() throws IOException {
		// close all channel writers
		for (ChannelWriter writer : partitionWriters) {
			Collection<MemorySegment> releasedSegments = writer.close();
			freeSegments.addAll(releasedSegments);
		}

		// clear the writers array
		partitionWriters.clear();
	}

	/**
	 * Repartitions the build side.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ServiceException
	 */
	private void repartitionBuildSide() throws IOException, InterruptedException, ServiceException {
		// step 1: partition hash map contents
		//
		LOG.debug("repartition build side #1: repartition hash map contents");

		KeyValuePair<K, VB> pair = buildPairSerialization.newInstance();
		int partition;

		// repartition hash map contents
		for (K key : hashMap.keys()) {
			partition = partitioner.getPartition(key);
			// append key/value pairs into the partition's channel writer
			for (VB value : hashMap.get(key)) {
				partitionWriters.get(partition).write(new KeyValuePair<K, VB>(key, value));
			}
		}

		// clear current hash map contents
		hashMap.clear();

		// spill and read partition 0 contents
		ChannelReader partitionZeroReader = ioManager.createChannelReader(buildPartitionIDs.get(0), partitionWriters
			.get(0).close());
		while (partitionZeroReader.read(pair)) {
			hashMap.put(pair.getKey(), pair.getValue());
		}
		partitionWriters.set(0, ioManager.createChannelWriter(buildPartitionIDs.get(0), partitionZeroReader.close()));

		// step 2: repartition the record causing the hash map overflow
		//
		LOG.debug("repartition build side #2: repartition record causing hash map overflow");

		pair = overflowCause;
		partition = partitioner.getPartition(pair.getKey());

		if (partition == 0) {
			hashMap.put(pair.getKey(), pair.getValue());
		} else {
			partitionWriters.get(partition).write(pair);
		}

		// step 3: repartition remaining records from reader
		//
		LOG.debug("repartition build side #3: repartition remaining records from reader");

		while (readerBuild.hasNext()) {
			pair = readerBuild.next();
			partition = partitioner.getPartition(pair.getKey());
			if (partition == 0) {
				hashMap.put(pair.getKey(), pair.getValue());
			} else {
				partitionWriters.get(partition).write(pair);
			}
		}
	}

	/**
	 * Repartitions the probe side.
	 * 
	 * @throws OutOfMemoryException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void repartitionProbeSide() throws OutOfMemoryException, IOException, InterruptedException {
		KeyValuePair<K, VP> pair;
		int partition;

		while (readerProbe.hasNext()) {
			pair = readerProbe.next();
			partition = partitioner.getPartition(pair.getKey());

			partitionWriters.get(partition).write(pair);
		}
	}

	/**
	 * Partitioner class for determining the output partitions of records.
	 * 
	 * @author Alexander Alexandrov
	 */
	private final class Partitioner {
		public byte[] salt;

		public int numberOfPartitions;

		public Partitioner(int numberOfPartitions) {
			this.salt = new byte[] { 32, 02, 74, 27, 42, 67, -121, 12 };
			this.numberOfPartitions = numberOfPartitions;
		}

		public int getPartition(K key) {
			int hash = 1315423911 ^ ((1315423911 << 5) + key.hashCode() + (1315423911 >> 2));

			for (int i = 0; i < salt.length; i++) {
				hash ^= ((hash << 5) + salt[i] + (hash >> 2));
			}

			return (hash < 0) ? -hash % numberOfPartitions : hash % numberOfPartitions;
		}
	}
}
