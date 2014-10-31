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

package org.apache.flink.addons.hbase.example;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.addons.hbase.common.HBaseKey;
import org.apache.flink.addons.hbase.common.HBaseResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 */
public class HBaseReadExample {

	public static class MyTableInputFormat extends TableInputFormat {

		private static final long serialVersionUID = 1L;

		private final byte[] META_FAMILY = "meta".getBytes();
		private final byte[] USER_COLUMN = "user".getBytes();
		private final byte[] TIMESTAMP_COLUMN = "timestamp".getBytes();
		private final byte[] TEXT_FAMILY = "text".getBytes();
		private final byte[] TWEET_COLUMN = "tweet".getBytes();

		StringValue row_string = new StringValue();
		StringValue user_string = new StringValue();
		StringValue timestamp_string = new StringValue();
		StringValue tweet_string = new StringValue();
		
		public MyTableInputFormat(Configuration parameters) {
			super(parameters);
		}

		@Override
		protected HTable createTable(Configuration parameters) {
			return super.createTable(parameters);
		}

		@Override
		protected Scan createScanner(Configuration parameters) {
			Scan scan = new Scan();
			scan.addColumn(META_FAMILY, USER_COLUMN);
			scan.addColumn(META_FAMILY, TIMESTAMP_COLUMN);
			scan.addColumn(TEXT_FAMILY, TWEET_COLUMN);
			return scan;
		}

		@Override
		public void mapResultToRecord(Record record, HBaseKey key,
				HBaseResult result) {
			Result res = result.getResult();
			byte[] keyBytes = res.getRow();
			record.setField(0, toString(row_string, keyBytes));
			record.setField(1, toString(user_string, res.getValue(META_FAMILY, USER_COLUMN)));
			record.setField(2, toString(timestamp_string, res.getValue(META_FAMILY, TIMESTAMP_COLUMN)));
			record.setField(3, toString(tweet_string, res.getValue(TEXT_FAMILY, TWEET_COLUMN)));
		}

		private final StringValue toString(StringValue string, byte[] bytes) {
			string.setValueAscii(bytes, 0, bytes.length);
			return string;
		}

	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.createLocalEnvironment();
		Configuration conf = new Configuration();
		conf.setString(TableInputFormat.INPUT_TABLE, "twitter");
		MyTableInputFormat inputFormat = new MyTableInputFormat(conf);
		DataSet<Record> hbaseDs = env.createInput(inputFormat);

		String output = "file:///tmp/flink-test";
		// HBase String dump
		hbaseDs.writeAsCsv(output, "\n", " ", WriteMode.OVERWRITE);
		env.execute();
	}

}
