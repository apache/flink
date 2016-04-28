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

package org.apache.flink.runtime.state;

import java.io.Serializable;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class PartitionedStateSnapshot implements Serializable {
	private static final long serialVersionUID = 7043475572141783706L;

	private final Map<String, KvStateSnapshot<?, ?, ?, ?, ?>> namedKvStateSnapshots;

	public PartitionedStateSnapshot() {
		namedKvStateSnapshots = new HashMap<>();
	}

	public KvStateSnapshot<?, ?, ?, ?, ?> get(String key) {
		return namedKvStateSnapshots.get(key);
	}

	public boolean containsKey(String key) {
		return namedKvStateSnapshots.containsKey(key);
	}

	public void put(String name, KvStateSnapshot<?, ?, ?, ?, ?> kvStateSnapshot) {
		namedKvStateSnapshots.put(name, kvStateSnapshot);
	}

	public Set<String> keySet() {
		return namedKvStateSnapshots.keySet();
	}

	public Set<Map.Entry<String, KvStateSnapshot<?, ?, ?, ?, ?>>> entrySet() {
		return namedKvStateSnapshots.entrySet();
	}

	public long getStateSize() throws Exception {
		long stateSize = 0;

		for (KvStateSnapshot<?, ?, ?, ?, ?> kvStateSnapshot: namedKvStateSnapshots.values()) {
			stateSize += kvStateSnapshot.getStateSize();
		}

		return stateSize;
	}

	public void discardState() throws Exception {

		while (!namedKvStateSnapshots.isEmpty()) {
			try {
				Iterator<KvStateSnapshot<?, ?, ?, ?, ?>> iterator = namedKvStateSnapshots.values().iterator();

				while (iterator.hasNext()) {
					KvStateSnapshot<?, ?, ?, ?, ?> kvStateSnapshot = iterator.next();
					kvStateSnapshot.discardState();
					iterator.remove();
				}
			} catch (ConcurrentModificationException e) {
				// fall through the loop
			}
		}


		for (KvStateSnapshot<?, ?, ?, ?, ?> kvStateSnapshot : namedKvStateSnapshots.values()) {
			kvStateSnapshot.discardState();
		}
	}

	public Collection<KvStateSnapshot<?, ?, ?, ?, ?>> values() {
		return namedKvStateSnapshots.values();
	}
}
