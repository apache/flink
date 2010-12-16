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
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;

/**
 * MergeTaskIterator implementation of a grace join algorithm.
 * 
 * @author Alexander Alexandrov
 */
public class HybridHashMatchIterator<K extends Key, V1 extends Value, V2 extends Value> implements
		MatchTaskIterator<K, V1, V2> {
	// -------------------------------------------------------------------------
	// Types
	// -------------------------------------------------------------------------

	public enum InputRoles {
		BUILD_PROBE, PROBE_BUILD;
	}

	// -------------------------------------------------------------------------
	// Constants
	// -------------------------------------------------------------------------

	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(HybridHashMatchIterator.class);

	private static final double MEMORY_CONSUMPTION_RATIO = 4;

	private static final int NUMBER_OF_PARTITIONS = 6;

	// -------------------------------------------------------------------------
	// Generic type classes and generic parameters
	// -------------------------------------------------------------------------

	private final Class<K> keyClass;

	private final Class<V1> value1Class;

	private final Class<V2> value2Class;

	private final Reader<KeyValuePair<K, V1>> reader1;

	private final Reader<KeyValuePair<K, V2>> reader2;

	private HashMatchStrategy<K, ?, ?> activeStrategy;

	private HashMatchStrategy<K, V2, V1> strategyProbeBuild;

	private HashMatchStrategy<K, V1, V2> strategyBuildProbe;

	// -------------------------------------------------------------------------
	// Serialization factories
	// -------------------------------------------------------------------------

	private final SerializationFactory<K> keySerialization;

	private final SerializationFactory<V1> value1Serialization;

	private final SerializationFactory<V2> value2Serialization;

	// -------------------------------------------------------------------------
	// Memory / IOManager related
	// -------------------------------------------------------------------------

	private final IOManager ioManager;

	private final MemoryManager memoryManager;

	private final int ioBuffersMemorySize;

	private final int hashMapMemorySize;

	private final int numberOfIOBuffers;

	private final int ioBufferSize;

	private final InputRoles inputRoles;

	private MemorySegment hashMapSegment;

	private Collection<MemorySegment> bufferSegments;

	public HybridHashMatchIterator(MemoryManager memoryManager, IOManager ioManager,
			Reader<KeyValuePair<K, V1>> reader1, Reader<KeyValuePair<K, V2>> reader2, Class<K> keyClass,
			Class<V1> value1Class, Class<V2> value2Class, InputRoles inputRoles, int availableMemorySize) {
		// set generic type classes
		this.keyClass = keyClass;
		this.value1Class = value1Class;
		this.value2Class = value2Class;

		// set serialization factories
		this.keySerialization = new WritableSerializationFactory<K>(keyClass);
		this.value1Serialization = new WritableSerializationFactory<V1>(value1Class);
		this.value2Serialization = new WritableSerializationFactory<V2>(value2Class);

		// set readers
		this.reader1 = reader1;
		this.reader2 = reader2;

		// set io / memory managers
		this.ioManager = ioManager;
		this.memoryManager = memoryManager;

		// calculate memory consumption costs for the hash map and the io buffers
		// TODO: this is unflexible, configuration parameters should be used for initial values
		this.ioBuffersMemorySize = (int) (availableMemorySize / (MEMORY_CONSUMPTION_RATIO + 1));
		this.numberOfIOBuffers = 2 * NUMBER_OF_PARTITIONS;
		this.ioBufferSize = ioBuffersMemorySize / numberOfIOBuffers;
		this.hashMapMemorySize = availableMemorySize - ioBuffersMemorySize;

		this.inputRoles = inputRoles;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open() throws IOException, InterruptedException {
		try {
			hashMapSegment = memoryManager.allocate(hashMapMemorySize);
			bufferSegments = memoryManager.allocate(numberOfIOBuffers, ioBufferSize);

			if (inputRoles == InputRoles.BUILD_PROBE) {
				SerializingHashMap<K, V1> hashMap = new SerializingHashMap<K, V1>(keyClass, value1Class, hashMapSegment);
				HashMatchStrategy<K, V1, V2> strategy;

				LOG.debug("Initializing HybridHashMergeIterator (build := reader1, probe := reader2)");

				try {
					// try initializing an in memory strategyProbeBuild
					strategy = new InMemoryHashMatchStrategy<K, V1, V2>(reader1, reader2, ioManager, keySerialization,
						value1Serialization, value2Serialization, hashMap);
					strategy.initialize();
					LOG.debug("Using InMemoryHashMergeStrategy");
				} catch (SerializingHashMap.OverflowException e) {
					LOG.warn("Overflow cause is " + e.cause + ". Falling back to PartitioningHashMergeStrategy");

					// fallback to external partitioning strategyProbeBuild
					strategy = new PartitioningHashMatchStrategy<K, V1, V2>(reader1, reader2, ioManager,
						keySerialization, value1Serialization, value2Serialization, hashMap,
						(KeyValuePair<K, V1>) e.cause, bufferSegments);
					strategy.initialize();
					LOG.debug("Using PartitioningHashMergeStrategy");
				}

				this.strategyBuildProbe = strategy;
				this.activeStrategy = strategy;
			} else {
				SerializingHashMap<K, V2> hashMap = new SerializingHashMap<K, V2>(keyClass, value2Class, hashMapSegment);
				HashMatchStrategy<K, V2, V1> strategy;

				LOG.debug("Initializing HashMergeStrategy (build := reader2, probe := reader1)");

				try {
					// try initializing an in memory strategyProbeBuild
					strategy = new InMemoryHashMatchStrategy<K, V2, V1>(reader2, reader1, ioManager, keySerialization,
						value2Serialization, value1Serialization, hashMap);
					strategy.initialize();
					LOG.debug("Using InMemoryHashMergeStrategy");
				} catch (SerializingHashMap.OverflowException e) {
					LOG.warn("Overflow cause is " + e.cause + ". Falling back to PartitioningHashMergeStrategy");
					// fallback to external partitioning strategyProbeBuild
					strategy = new PartitioningHashMatchStrategy<K, V2, V1>(reader2, reader1, ioManager,
						keySerialization, value2Serialization, value1Serialization, hashMap,
						(KeyValuePair<K, V2>) e.cause, bufferSegments);
					strategy.initialize();
					LOG.debug("Using PartitioningHashMergeStrategy");
				}

				this.strategyProbeBuild = strategy;
				this.activeStrategy = strategy;
			}

		} catch (ServiceException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() {
		LOG.debug("Closing HybridHashMergeIterator");

		if (activeStrategy != null) {
			activeStrategy.close();
		}

		memoryManager.release(hashMapSegment);
		memoryManager.release(bufferSegments);
	}

	@Override
	public boolean next() throws IOException, InterruptedException {
		return activeStrategy.next();
	}

	@Override
	public K getKey() {
		return activeStrategy.getKey();
	}

	@Override
	public Iterator<V1> getValues1() {
		if (inputRoles == InputRoles.PROBE_BUILD) {
			return strategyProbeBuild.getValuesProbe();
		} else {
			return strategyBuildProbe.getValuesBuild();
		}
	}

	@Override
	public Iterator<V2> getValues2() {
		if (inputRoles == InputRoles.PROBE_BUILD) {
			return strategyProbeBuild.getValuesBuild();
		} else {
			return strategyBuildProbe.getValuesProbe();
		}
	}

}
