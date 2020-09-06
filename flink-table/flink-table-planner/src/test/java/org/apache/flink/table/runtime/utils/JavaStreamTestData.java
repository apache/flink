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

package org.apache.flink.table.runtime.utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test data.
 */
public class JavaStreamTestData {

	public static DataStream<Tuple3<Integer, Long, String>> getSmall3TupleDataSet(StreamExecutionEnvironment env) {

		List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
		data.add(new Tuple3<>(1, 1L, "Hi"));
		data.add(new Tuple3<>(2, 2L, "Hello"));
		data.add(new Tuple3<>(3, 2L, "Hello world"));

		Collections.shuffle(data);

		return env.fromCollection(data);
	}

	public static DataStream<Tuple5<Integer, Long, Integer, String, Long>> get5TupleDataStream(StreamExecutionEnvironment env) {

		List<Tuple5<Integer, Long, Integer, String, Long>> data = new ArrayList<>();
		data.add(new Tuple5<>(1, 1L, 0, "Hallo", 1L));
		data.add(new Tuple5<>(2, 2L, 1, "Hallo Welt", 2L));
		data.add(new Tuple5<>(2, 3L, 2, "Hallo Welt wie", 1L));
		data.add(new Tuple5<>(3, 4L, 3, "Hallo Welt wie gehts?", 2L));
		data.add(new Tuple5<>(3, 5L, 4, "ABC", 2L));
		data.add(new Tuple5<>(3, 6L, 5, "BCD", 3L));
		data.add(new Tuple5<>(4, 7L, 6, "CDE", 2L));
		data.add(new Tuple5<>(4, 8L, 7, "DEF", 1L));
		data.add(new Tuple5<>(4, 9L, 8, "EFG", 1L));
		data.add(new Tuple5<>(4, 10L, 9, "FGH", 2L));
		data.add(new Tuple5<>(5, 11L, 10, "GHI", 1L));
		data.add(new Tuple5<>(5, 12L, 11, "HIJ", 3L));
		data.add(new Tuple5<>(5, 13L, 12, "IJK", 3L));
		data.add(new Tuple5<>(5, 15L, 14, "KLM", 2L));
		data.add(new Tuple5<>(5, 14L, 13, "JKL", 2L));
		return env.fromCollection(data);
	}
}
