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

package org.apache.flink.connector.hbase.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.hbase.source.HBaseInputFormat;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple stub for HBase DataSet read
 *
 * <p>To run the test first create the test table with hbase shell.
 *
 * <p>Use the following commands:
 * <ul>
 *     <li>create 'test-table', 'someCf'</li>
 *     <li>put 'test-table', '1', 'someCf:someQual', 'someString'</li>
 *     <li>put 'test-table', '2', 'someCf:someQual', 'anotherString'</li>
 * </ul>
 *
 * <p>The test should return just the first entry.
 *
 */
public class HBaseReadExample {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		@SuppressWarnings("serial")
		DataSet<Tuple2<String, String>> hbaseDs = env.createInput(new HBaseInputFormat<Tuple2<String, String>>(HBaseConfiguration.create()) {

				@Override
				public String getTableName() {
					return HBaseFlinkTestConstants.TEST_TABLE_NAME;
				}

				@Override
				protected Scan getScanner() {
					Scan scan = new Scan();
					scan.addColumn(HBaseFlinkTestConstants.CF_SOME, HBaseFlinkTestConstants.Q_SOME);
					return scan;
				}

				private Tuple2<String, String> reuse = new Tuple2<>();

				@Override
				protected Tuple2<String, String> mapResultToTuple(Result r) {
					String key = Bytes.toString(r.getRow());
					String val = Bytes.toString(r.getValue(HBaseFlinkTestConstants.CF_SOME, HBaseFlinkTestConstants.Q_SOME));
					reuse.setField(key, 0);
					reuse.setField(val, 1);
					return reuse;
				}
		})
		.filter((FilterFunction<Tuple2<String, String>>) t -> {
			String val = t.getField(1);
			return val.startsWith("someStr");
		});

		hbaseDs.print();

	}

}
