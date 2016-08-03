/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase.example;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.hbase.HBaseMapper;
import org.apache.flink.streaming.connectors.hbase.HBaseSink;
import org.apache.flink.streaming.connectors.hbase.MutationActions;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * This is an example showing how to use the HBaseSink in the Streaming API.
 *
 * To run the example you need a local HBase database that has a table "flink-example" with a column family "cf".
 * In the example, the HBase sink takes an input of type {@link Tuple3} and perform different operations based on the input.
 * The first field of a input value is used as the row key, the second field is treated as an opcode that
 * determines which type of HBase operation is performed and the third field is the value to written.
 */
public class HBaseSinkExample {

	private static final List<Tuple3<String, Integer, Integer>> dataSource = new ArrayList<>(100);
	private static final String TABLE_NAME = "flink-example";
	private static final String FAMILY = "cf";
	private static final String COLUMN1 = "c1";
	private static final String COLUMN2 = "c2";
	private static final String ROWKEY_PREFIX = "row-";

	static {
		Random random = new Random();
		for (int i = 0; i < 99; i++) {
			String rowKey = ROWKEY_PREFIX + (i % 20);
			int opcode = random.nextInt(9);
			int value = i;
			dataSource.add(new Tuple3<>(rowKey, opcode, value));
		}
	}

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple3<String, Integer, Integer>> source = env.fromCollection(dataSource);

		source.addSink(new HBaseSink<>(TABLE_NAME, new HBaseMapperExample(FAMILY, COLUMN1, COLUMN2)));
		env.execute();
	}

	/**
	 * This class implements {@link HBaseMapper}.
	 */
	private static class HBaseMapperExample implements HBaseMapper<Tuple3<String, Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		private byte[] family;
		private byte[] col1;
		private byte[] col2;

		public HBaseMapperExample(String family, String col1, String col2) {
			this.family = Bytes.toBytes(family);
			this.col1 = Bytes.toBytes(col1);
			this.col2 = Bytes.toBytes(col2);
		}

		@Override
		public byte[] rowKey(Tuple3<String, Integer, Integer> value) {
			return Bytes.toBytes(value.f0);
		}

		@Override
		public MutationActions actions(Tuple3<String, Integer, Integer> value) {
			MutationActions mutationActions = new MutationActions();
			if (value.f1 == 0) {
				mutationActions.addPut(family, col1, Bytes.toBytes(value.f2 / 10));
			} else if (value.f1 == 1) {
				mutationActions.addPut(family, col2, Bytes.toBytes(value.f2 % 10));
			} else if (value.f1 == 2) {
				mutationActions.addPut(family, col1, Bytes.toBytes(value.f2 / 10));
				mutationActions.addPut(family, col2, Bytes.toBytes(value.f2 % 10));
			} else if (value.f1 == 3) {
				mutationActions.addAppend(family, col1, Bytes.toBytes(value.f2 / 10));
			} else if (value.f1 == 4) {
				mutationActions.addAppend(family, col2, Bytes.toBytes(value.f2 % 10));
			} else if (value.f1 == 5) {
				mutationActions.addPut(family, col1, Bytes.toBytes(value.f2 / 10));
				mutationActions.addPut(family, col2, Bytes.toBytes(value.f2 % 10));
			} else if (value.f1 == 6) {
				mutationActions.addDeleteColumns(family, col1);
			} else if (value.f1 == 7) {
				mutationActions.addDeleteColumns(family, col2);
			} else {
				mutationActions.addDeleteColumns(family, col1);
				mutationActions.addDeleteColumns(family, col2);
			}
			return mutationActions;
		}
	}
}
