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

package org.apache.flink.storm.util;

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;

import org.apache.storm.Config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * {@link StormConfig} is used to provide a user-defined Storm configuration (ie, a raw {@link Map} or {@link Config}
 * object) for embedded Spouts and Bolts.
 */
@SuppressWarnings("rawtypes")
public final class StormConfig extends GlobalJobParameters implements Map {
	private static final long serialVersionUID = 8019519109673698490L;

	/** Contains the actual configuration that is provided to Spouts and Bolts. */
	private final Map config = new HashMap();

	/**
	 * Creates an empty configuration.
	 */
	public StormConfig() {
	}

	/**
	 * Creates an configuration with initial values provided by the given {@code Map}.
	 *
	 * @param config
	 *            Initial values for this configuration.
	 */
	@SuppressWarnings("unchecked")
	public StormConfig(Map config) {
		this.config.putAll(config);
	}

	@Override
	public int size() {
		return this.config.size();
	}

	@Override
	public boolean isEmpty() {
		return this.config.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return this.config.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return this.config.containsValue(value);
	}

	@Override
	public Object get(Object key) {
		return this.config.get(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object put(Object key, Object value) {
		return this.config.put(key, value);
	}

	@Override
	public Object remove(Object key) {
		return this.config.remove(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putAll(Map m) {
		this.config.putAll(m);
	}

	@Override
	public void clear() {
		this.config.clear();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<Object> keySet() {
		return this.config.keySet();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<Object> values() {
		return this.config.values();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<java.util.Map.Entry<Object, Object>> entrySet() {
		return this.config.entrySet();
	}

}
