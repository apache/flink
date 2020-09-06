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

package org.apache.flink.runtime.state.ttl.mock;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** In memory mock internal map state. */
public class MockInternalMapState<K, N, UK, UV>
	extends MockInternalKvState<K, N, Map<UK, UV>>
	implements InternalMapState<K, N, UK, UV> {

	private MockInternalMapState() {
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
		return getInternal().entrySet();
	}

	@Override
	public Iterable<UK> keys() {
		return getInternal().keySet();
	}

	@Override
	public Iterable<UV> values() {
		return getInternal().values();
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {
		return entries().iterator();
	}

	@Override
	public boolean isEmpty() {
		return getInternal().isEmpty();
	}

	@SuppressWarnings({"unchecked", "unused"})
	static <N, T, S extends State, IS extends S> IS createState(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, T> stateDesc) {
		return (IS) new MockInternalMapState<>();
	}
}
