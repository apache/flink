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

package org.apache.flink.table.client.gateway.utils;

import org.apache.flink.table.api.functions.AggregateFunction;
import org.apache.flink.table.api.functions.ScalarFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.types.Row;

/**
 * A bunch of UDFs for testing the SQL Client.
 */
public class UserDefinedFunctions {

	/**
	 * The scalar function for SQL Client test.
	 */
	public static class ScalarUDF extends ScalarFunction {

		private int offset;

		public ScalarUDF() {
			this.offset = 5;
		}

		public ScalarUDF(Integer offset) {
			this.offset = offset;
		}

		public String eval(Integer i) {
			return String.valueOf(i + offset);
		}
	}

	/**
	 * The aggregate function for SQL Client test.
	 */
	public static class AggregateUDF extends AggregateFunction<Long, Long> {

		public AggregateUDF(String name, Boolean flag, Integer value) {
			// do nothing
		}

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long getValue(Long accumulator) {
			return 100L;
		}

		public void accumulate(Long acc, Long value) {
			// do nothing
		}

		@Override
		public DataType getResultType() {
			return DataTypes.LONG;
		}
	}

	/**
	 * The table function for SQL Client test.
	 */
	public static class TableUDF extends TableFunction<Row> {
		private long extra;

		public TableUDF(Long extra) {
			this.extra = extra;
		}

		public void eval(String str) {
			for (String s : str.split(" ")) {
				Row r = new Row(2);
				r.setField(0, s);
				r.setField(1, s.length() + extra);
				collect(r);
			}
		}

		@Override
		public DataType getResultType(Object[] arguments, Class[] argTypes) {
			return new RowType(DataTypes.STRING, DataTypes.LONG);
		}
	}
}
