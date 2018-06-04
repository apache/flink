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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class MockInternalMapState<K, N, UK, UV>
	extends MockInternalKvState<K, N, Map<UK, UV>>
	implements InternalMapState<K, N, UK, UV> {

	MockInternalMapState() {
		super(HashMap::new);
	}

	@Override
	public void clear() {
		getInternal().clear();
	}

	@Override
	public UV get(UK key) {
		return getInternal().get(key);
	}

	@Override
	public void put(UK key, UV value) {
		this.getInternal().put(key, value);
	}

	@Override
	public void putAll(Map<UK, UV> map) {
		getInternal().putAll(map);
	}

	@Override
	public void remove(UK key) {
		getInternal().remove(key);
	}

	@Override
	public boolean contains(UK key) {
		return getInternal().containsKey(key);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() {
		return copy().entrySet();
	}

	private Map<UK, UV> copy() {
		return new HashMap<>(getInternal());
	}

	@Override
	public Iterable<UK> keys() {
		return copy().keySet();
	}

	@Override
	public Iterable<UV> values() {
		return copy().values();
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {
		return entries().iterator();
	}
}
