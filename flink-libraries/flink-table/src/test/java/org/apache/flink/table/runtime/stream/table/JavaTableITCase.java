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

package org.apache.flink.table.runtime.stream.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.JavaStreamTestData;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.utils.TopN;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for streaming TableApi.
 */
public class JavaTableITCase extends AbstractTestBase {

	@Test
	public void testRegisterTableAggregateFunction() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		tableEnv.registerFunction("top3", new TopN(3));

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds =
			JavaStreamTestData.get5TupleDataStream(env);
		Table result = tableEnv
			.fromDataStream(ds, "a, b, d, c, e")
			.flatAggregate("top3(1, b) as (a, b, c)") // get top3 from all the data
			.select("*");

		DataStream<Tuple2<Boolean, Row>> resultSet = tableEnv.toRetractStream(result, Row.class);
		resultSet.addSink(new StreamITCase.JRetractingSink()).setParallelism(1);
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,15,0");
		expected.add("1,14,1");
		expected.add("1,13,2");

		StreamITCase.compareRetractWithList(expected);
	}
}
