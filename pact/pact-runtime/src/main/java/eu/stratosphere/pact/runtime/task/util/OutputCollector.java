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

import java.util.List;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

public class OutputCollector<K extends Key, V extends Value> implements Collector<K, V> {
	private final List<RecordWriter<KeyValuePair<K, V>>> writers;

	public OutputCollector(List<RecordWriter<KeyValuePair<K, V>>> writers) {
		this.writers = writers;
	}

	@Override
	public void collect(K key, V value) {
		try {
			for (RecordWriter<KeyValuePair<K, V>> writer : writers) {
				// TODO: is new KeyValuePair necessary?
				writer.emit(new KeyValuePair<K, V>(key, value));
			}
		} catch (java.io.IOException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {

	}
}
