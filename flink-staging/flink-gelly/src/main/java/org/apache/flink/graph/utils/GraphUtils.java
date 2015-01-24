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

package org.apache.flink.graph.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;

@SuppressWarnings("serial")
public class GraphUtils {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static DataSet<Integer> count(DataSet set, ExecutionEnvironment env) {
		List<Integer> list = new ArrayList<Integer>();
		list.add(0);
		DataSet<Integer> initialCount = env.fromCollection(list);
		return set.map(new OneMapper()).union(initialCount)
				.reduce(new AddOnesReducer()).first(1);
	}

	private static final class OneMapper<T extends Tuple> implements
			MapFunction<T, Integer> {
		@Override
		public Integer map(T o) throws Exception {
			return 1;
		}
	}

	private static final class AddOnesReducer implements
			ReduceFunction<Integer> {
		@Override
		public Integer reduce(Integer one, Integer two) throws Exception {
			return one + two;
		}
	}
}