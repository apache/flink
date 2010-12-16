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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.OutOfMemoryException;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.hash.SerializingHashMap.OverflowException;

/**
 * Basic in-memory hash merge strategy. This strategy will try to initialize the
 * build hash table using the available memory. If an {@link OutOfMemoryException} is thrown by the {@code
 * hashMap.put(...)} call,
 * an {@link OverflowException} indicating the strategy failure is propagated to
 * the client.
 * 
 * @author Alexander Alexandrov
 */
class InMemoryHashMatchStrategy<K extends Key, VB extends Value, VP extends Value> extends HashMatchStrategy<K, VB, VP> {
	// -------------------------------------------------------------------------
	// Constants
	// -------------------------------------------------------------------------

	/**
	 * Logging.
	 */
	private static final Log LOG = LogFactory.getLog(InMemoryHashMatchStrategy.class);

	public InMemoryHashMatchStrategy(Reader<KeyValuePair<K, VB>> readerBuild, Reader<KeyValuePair<K, VP>> readerProbe,
			IOManager ioManager, SerializationFactory<K> keySerialization, SerializationFactory<VB> buildSerialization,
			SerializationFactory<VP> probeSerialization, SerializingHashMap<K, VB> hashMap) {
		super(readerBuild, readerProbe, ioManager, keySerialization, buildSerialization, probeSerialization, hashMap);
	}

	@Override
	public void initialize() throws ServiceException, IOException, InterruptedException {
		KeyValuePair<K, VB> pair = null;

		try {
			LOG.debug("Start building inmemory hash table");

			// try to build the hash table
			while (readerBuild.hasNext()) {
				pair = readerBuild.next();
				hashMap.put(pair.getKey(), pair.getValue());
			}
		} catch (OutOfMemoryException e) {
			throw new RuntimeException("Caught OutOfMemoryException while building hash side", e);
		}

		LOG.debug("Finished building inmemory hash table");

	}

	@Override
	public void close() {
		// close logic
	}

	@Override
	public boolean next() throws IOException, InterruptedException {
		while (readerProbe.hasNext()) {
			KeyValuePair<K, VP> pair = readerProbe.next();
			K key = pair.getKey();

			if (hashMap.contains(key)) {
				currentKey = key;
				currentBuildValuesIterable = hashMap.get(currentKey);
				currentProbeValuesIterable.value = pair.getValue();
				return true;
			}
		}

		return false;
	}
}
