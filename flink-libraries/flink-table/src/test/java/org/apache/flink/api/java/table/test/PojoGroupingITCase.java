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

package org.apache.flink.api.java.table.test;

import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PojoGroupingITCase extends MultipleProgramsTestBase {

	public PojoGroupingITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testPojoGrouping() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<String, Double, String>> data = env.fromElements(
			new Tuple3<>("A", 23.0, "Z"),
			new Tuple3<>("A", 24.0, "Y"),
			new Tuple3<>("B", 1.0, "Z"));

		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		Table table = tableEnv
			.fromDataSet(data, "groupMe, value, name")
			.select("groupMe, value, name")
			.where("groupMe != 'B'");

		DataSet<MyPojo> myPojos = tableEnv.toDataSet(table, MyPojo.class);

		DataSet<MyPojo> result = myPojos.groupBy("groupMe")
			.sortGroup("value", Order.DESCENDING)
			.first(1);

		List<MyPojo> resultList = result.collect();
		compareResultAsText(resultList, "A,24.0,Y");
	}

	public static class MyPojo implements Serializable {
		private static final long serialVersionUID = 8741918940120107213L;

		public String groupMe;
		public double value;
		public String name;

		public MyPojo() {
			// for serialization
		}

		public MyPojo(String groupMe, double value, String name) {
			this.groupMe = groupMe;
			this.value = value;
			this.name = name;
		}

		@Override
		public String toString() {
			return groupMe + "," + value + "," + name;
		}
	}
}
