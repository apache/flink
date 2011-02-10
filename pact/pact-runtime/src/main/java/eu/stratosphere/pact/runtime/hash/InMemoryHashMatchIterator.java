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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;


@SuppressWarnings("rawtypes")
public class InMemoryHashMatchIterator implements MatchTaskIterator {
	
	private Reader<? extends KeyValuePair> readerBuild;

	private Reader<? extends KeyValuePair> readerProbe;

	private Key currentKey;

	private Value currentValue1;

	private Iterable<Value> currentValue2Iterable;

	private HashMap<Key, Collection<Value>> build;

	public InMemoryHashMatchIterator(Reader<? extends KeyValuePair> reader1, Reader<? extends KeyValuePair> reader2) {
		readerProbe = reader1;
		readerBuild = reader2;
	}

	@Override
	public void open() throws IOException, InterruptedException {
		buildHash();
	}

	@Override
	public void close() {
		readerBuild = null;
		readerProbe = null;

		currentKey = null;
		currentValue1 = null;
	}

	@Override
	public Key getKey() {
		return currentKey;
	}

	@Override
	public Iterator<Value> getValues1() {
		return new Iterator<Value>() {
			boolean finished = false;

			@Override
			public boolean hasNext() {
				return !finished;
			}

			@Override
			public Value next() {
				finished = true;
				return currentValue1;
			}

			@Override
			public void remove() {
			}
		};
	}

	@Override
	public Iterator<Value> getValues2() {
		return currentValue2Iterable.iterator();
	}

	@Override
	public boolean next() throws IOException, InterruptedException {
		while (readerProbe.hasNext()) {
			KeyValuePair pair = readerProbe.next();
			currentKey = pair.getKey();
			currentValue1 = pair.getValue();
			currentValue2Iterable = build.get(currentKey);

			if (currentValue2Iterable != null) {
				return true;
			}
		}

		return false;
	}

	@SuppressWarnings("unchecked")
	private void buildHash() throws IOException, InterruptedException {
		build = new HashMap<Key, Collection<Value>>();

		while (readerBuild.hasNext()) {
			KeyValuePair<Key, Value> pair = readerBuild.next();

			Key key = pair.getKey();
			Value value = pair.getValue();

			if (!build.containsKey(key)) {
				build.put(key, new LinkedList<Value>());
			}

			Collection<Value> values = build.get(key);
			values.add(value);
		}
	}

}
