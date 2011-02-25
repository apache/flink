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

package eu.stratosphere.pact.runtime.task.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairSerializationFactory;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;

/**
 * The OutputCollector collects {@link Key} and {@link Value}, creates a {@link KeyValuePair}, and 
 * emits the pair to a set of Nephele {@link RecordWriter}.
 * The OutputCollector tracks to which writers a deep-copy must be given and which not.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public class OutputCollector<K extends Key, V extends Value> implements Collector<K, V> {
	
	// serialization copier to create deep-copies
	private final SerializationCopier<KeyValuePair<K, V>> kvpCopier;
	// serialization factories
	private KeyValuePairSerializationFactory<K,V> kvpSerialization;
	// list of writers
	protected final List<RecordWriter<KeyValuePair<K, V>>> writers;
	// bit mask for copy flags
	protected int fwdCopyFlags;

	/**
	 * Initializes the output collector with no writers.
	 */
	public OutputCollector() {
		this.writers = new ArrayList<RecordWriter<KeyValuePair<K, V>>>();
		this.fwdCopyFlags = 0;
		this.kvpCopier = new SerializationCopier<KeyValuePair<K, V>>();
	}
	
	/**
	 * Initializes the output collector with a set of writers. 
	 * To specify for a writer that it must be fed with a deep-copy, set the bit in the copy flag bit mask to 1 that 
	 * corresponds to the position of the writer within the {@link List}.
	 * 
	 * @param writers List of all writers.
	 * @param fwdCopyFlags Bit mask that specifies which writer is fed with deep-copies.
	 */
	public OutputCollector(List<RecordWriter<KeyValuePair<K, V>>> writers, int fwdCopyFlags) {
		
		this.writers = writers;
		this.fwdCopyFlags = fwdCopyFlags;
		this.kvpCopier = new SerializationCopier<KeyValuePair<K, V>>();		
	}
	
	/**
	 * Adds a writer to the OutputCollector.
	 * 
	 * @param writer The writer to add.
	 * @param fwdCopy Set true if writer requires a deep-copy. Set to false otherwise.
	 */
	public void addWriter(RecordWriter<KeyValuePair<K,V>> writer, boolean fwdCopy) {
		this.writers.add(writer);
		if (fwdCopy) {
			this.fwdCopyFlags |= 0x1 << (this.writers.size() - 1);
		}
	}

	/**
	 * Collects a {@link Key} and {@link Value}, wraps them in a KeyValuePair, and emit them to all writers.
	 * Writers which require a deep-copy are fed with a copy obtained through de/serialization.
	 */
	@Override
	public void collect(K key, V value) {
		try {
			
			KeyValuePair<K,V> emitPair = new KeyValuePair<K, V>(key,value);
			
			if (fwdCopyFlags == 0) {
				for (int i = 0; i < writers.size(); i++) {
					writers.get(i).emit(emitPair);
				}
			}
			else {
				if(kvpSerialization == null) {
					// TODO: can we do this nicer?
					this.kvpSerialization = new KeyValuePairSerializationFactory<K, V>(
							new WritableSerializationFactory<K>((Class<K>)key.getClass()),
							new WritableSerializationFactory<V>((Class<V>)value.getClass())); 
				}
				
				kvpCopier.setCopy(emitPair);
				
				for (int i = 0; i < writers.size(); i++) {
					if (((fwdCopyFlags >> i) & 0x1) != 0) {
						emitPair = kvpSerialization.newInstance();
						kvpCopier.getCopy(emitPair);
					}
					writers.get(i).emit(emitPair);
				}
			}
				
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stub.Collector#close()
	 */
	@Override
	public void close() {
	}
}
