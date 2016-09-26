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
package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;

/**
 * Internal class for keeping track of in-flight timers.
 *
 * @param <K> Type of the keys to which timers are scoped.
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public class InternalTimer<K, N> implements Comparable<InternalTimer<K, N>> {
	private final long timestamp;
	private final K key;
	private final N namespace;

	public InternalTimer(long timestamp, K key, N namespace) {
		this.timestamp = timestamp;
		this.key = key;
		this.namespace = namespace;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public K getKey() {
		return key;
	}

	public N getNamespace() {
		return namespace;
	}

	@Override
	public int compareTo(InternalTimer<K, N> o) {
		return Long.compare(this.timestamp, o.timestamp);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()){
			return false;
		}

		InternalTimer<?, ?> timer = (InternalTimer<?, ?>) o;

		return timestamp == timer.timestamp
				&& key.equals(timer.key)
				&& namespace.equals(timer.namespace);

	}

	@Override
	public int hashCode() {
		int result = (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + key.hashCode();
		result = 31 * result + namespace.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Timer{" +
				"timestamp=" + timestamp +
				", key=" + key +
				", namespace=" + namespace +
				'}';
	}
}
