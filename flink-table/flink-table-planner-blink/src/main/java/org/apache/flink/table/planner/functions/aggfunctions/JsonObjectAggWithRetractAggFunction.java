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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.AggregateFunction;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;

/**
 * built-in JsonObjectAgg aggregate function.
 */
public class JsonObjectAggWithRetractAggFunction<T> extends
	AggregateFunction<T, HashMap<String, Object>> {

	@Override
	public HashMap<String, Object> createAccumulator() {
		return new HashMap<>();
	}

	public void accumulate(
		HashMap<String, Object> acc,
		String key, Object value) throws Exception {
		if (!key.isEmpty()) {
			if (value != null) {
				acc.put(key, value);
			} else {
				acc.put(key, null);
			}
		}
	}

	public void resetAccumulator(HashMap<String, Object> acc) {
		acc.clear();
	}

	public void retract(HashMap<String, Object> acc, String key, Object value) {
		if (!key.isEmpty()) {
			acc.put(key, value);
		}
	}

	@Override
	public TypeInformation<T> getResultType() {
		return super.getResultType();
	}

	@Override
	public T getValue(HashMap<String, Object> accumulator) {
		return null;
	}

	/**
	 * Built-in String JsonObjectAgg aggregate function.
	 */
	public static class StringJsonObjectAggWithRetractAggFunction extends
		JsonObjectAggWithRetractAggFunction<String> {

		private ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public TypeInformation<String> getResultType() {
			return Types.STRING;
		}

		@Override
		public String getValue(HashMap<String, Object> acc) {
			try {
				return objectMapper.writeValueAsString(acc);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
