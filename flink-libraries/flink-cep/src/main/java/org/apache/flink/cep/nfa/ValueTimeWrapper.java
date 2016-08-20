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

package org.apache.flink.cep.nfa;

/**
 * Wrapper for a value timestamp pair.
 *
 * @param <V> Type of the value
 */
class ValueTimeWrapper<V> {
	public final V value;
	private final long timestamp;

	public ValueTimeWrapper(final V value, final long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
	}

	public V getValue() {
		return value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "ValueTimeWrapper(" + value + ", " + timestamp + ")";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ValueTimeWrapper) {
			@SuppressWarnings("unchecked")
			ValueTimeWrapper<V> other = (ValueTimeWrapper<V>)obj;

			return timestamp == other.getTimestamp() && value.equals(other.getValue());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (int) (this.timestamp ^ this.timestamp >>> 32) + 31 * value.hashCode();
	}
}
