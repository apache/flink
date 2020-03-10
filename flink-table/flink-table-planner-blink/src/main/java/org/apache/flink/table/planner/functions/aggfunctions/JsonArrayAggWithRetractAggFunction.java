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

import java.util.ArrayList;
import java.util.List;

/**
 * built-in JsonArrayAgg aggregate function.
 */
public class JsonArrayAggWithRetractAggFunction<T> extends
	AggregateFunction<T, List<Object>> {

	@Override
	public List<Object> createAccumulator() {
		return new ArrayList<>();
	}

	public void accumulate(List<Object> acc, Object value) throws Exception {
		acc.add(value);
	}

	public void resetAccumulator(List<Object> acc) {
		acc.clear();
	}

	public void retract(List<Object> acc, Object value) {
		if (acc.indexOf(value) == -1) {
			acc.add(value);
		} else {
			acc.remove(value);
		}
	}

	@Override
	public TypeInformation<T> getResultType() {
		return super.getResultType();
	}

	@Override
	public T getValue(List<Object> accumulator) {
		return null;
	}

	/**
	 * Built-in String JsonObjectAgg aggregate function.
	 */
	public static class StringJsonArrayAggWithRetractAggFunction extends
		JsonArrayAggWithRetractAggFunction<String> {

		private ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public TypeInformation<String> getResultType() {
			return Types.STRING;
		}

		@Override
		public String getValue(List<Object> acc) {
			try {
				return objectMapper.writeValueAsString(acc);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
