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

package org.apache.flink.streaming.connectors.pulsar.internal;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Parse mode for the JSON record deserializer.
 */
public enum ParseMode {

	PERMISSIVE("PERMISSIVE"),

	DROPMALFORMED("DROPMALFORMED"),

	FAILFAST("FAILFAST");

	private String name;

	private static final Map<String, ParseMode> ENUM_MAP;

	ParseMode(String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	}

	static {
		Map<String, ParseMode> map = new ConcurrentHashMap<>();
		for (ParseMode instance : ParseMode.values()) {
			map.put(instance.getName(), instance);
		}
		ENUM_MAP = Collections.unmodifiableMap(map);
	}

	public static ParseMode get(String name) {
		return ENUM_MAP.getOrDefault(name, PERMISSIVE);
	}
}
