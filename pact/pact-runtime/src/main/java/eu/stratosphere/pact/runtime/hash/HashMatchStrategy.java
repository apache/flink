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
import java.util.Iterator;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairSerializationFactory;

/**
 * Abstract base class for the hash merge strategies (in-memory and
 * partitioning).
 * 
 * @author Alexander Alexandrov
 */
abstract class HashMatchStrategy<K extends Key, VB extends Value, VP extends Value> {
	protected final Reader<KeyValuePair<K, VB>> readerBuild;

	protected final Reader<KeyValuePair<K, VP>> readerProbe;

	protected final SerializationFactory<K> keySerialization;

	protected final SerializationFactory<VB> buildValueSerialization;

	protected final SerializationFactory<VP> probeValueSerialization;

	protected final SerializationFactory<KeyValuePair<K, VB>> buildPairSerialization;

	protected final SerializationFactory<KeyValuePair<K, VP>> probePairSerialization;

	protected final SerializingHashMap<K, VB> hashMap;

	protected final IOManager ioManager;

	protected K currentKey;

	protected Iterable<VB> currentBuildValuesIterable;

	protected ProbeValueIterable currentProbeValuesIterable;

	public HashMatchStrategy(Reader<KeyValuePair<K, VB>> readerBuild, Reader<KeyValuePair<K, VP>> readerProbe,
			IOManager ioManager, SerializationFactory<K> keySerialization, SerializationFactory<VB> buildSerialization,
			SerializationFactory<VP> probeSerialization, SerializingHashMap<K, VB> hashMap) {
		this.readerBuild = readerBuild;
		this.readerProbe = readerProbe;

		this.keySerialization = keySerialization;
		this.buildValueSerialization = buildSerialization;
		this.probeValueSerialization = probeSerialization;
		this.buildPairSerialization = new KeyValuePairSerializationFactory<K, VB>(keySerialization, buildSerialization);
		this.probePairSerialization = new KeyValuePairSerializationFactory<K, VP>(keySerialization, probeSerialization);

		this.ioManager = ioManager;
		this.hashMap = hashMap;

		this.currentProbeValuesIterable = new ProbeValueIterable();
	}

	abstract public void initialize() throws ServiceException, IOException, InterruptedException;

	abstract public boolean next() throws IOException, InterruptedException;

	abstract public void close();

	public K getKey() {
		return currentKey;
	}

	public Iterator<VB> getValuesBuild() {
		return currentBuildValuesIterable.iterator();
	}

	public Iterator<VP> getValuesProbe() {
		return currentProbeValuesIterable.iterator();
	}

	protected final class ProbeValueIterable implements Iterable<VP> {
		protected VP value;

		@Override
		public Iterator<VP> iterator() {
			return new ValueIterator();
		}

		private final class ValueIterator implements Iterator<VP> {
			boolean finished = false;

			@Override
			public boolean hasNext() {
				return !finished;
			}

			@Override
			public VP next() {
				finished = true;
				return value;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		}
	}
}
