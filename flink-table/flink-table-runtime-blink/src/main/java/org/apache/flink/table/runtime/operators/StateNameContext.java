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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A StateNameContext stores all state names and the number of visits.
 * This context can return a unique state name by adding access number as suffix.
 */
@Internal
public class StateNameContext implements Serializable {
	private static final long serialVersionUID = 1L;

	private final Map<String, Integer> nameToCountMap;

	public StateNameContext() {
		nameToCountMap = new HashMap<>();
	}

	/**
	 * Returns unique state name by adding access number as suffix to the given name.
	 */
	public String getUniqueStateName(String name) {
		int count = nameToCountMap.getOrDefault(name, 0);
		nameToCountMap.put(name, count + 1);
		if (count == 0) {
			return name;
		} else {
			return name + count;
		}
	}
}
