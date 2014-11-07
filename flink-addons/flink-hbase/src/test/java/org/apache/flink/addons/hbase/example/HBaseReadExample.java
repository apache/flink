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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.StringValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.serializer.Deserializer;

/**
 * Simple stub for HBase DataSet
 */
public class HBaseReadExample {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		@SuppressWarnings("serial")
		DataSet<String> hbaseDs = env.createInput(new TableInputFormat() {
				@Override
				public String getTableName() {
					return "test-table";
				}

				@Override
				protected Scan getScanner() {
					Scan scan = new Scan();
					scan.addColumn("someCf".getBytes(), "someQual".getBytes());
					return scan;
				}
		})
		.map(new RichMapFunction<Tuple2<ImmutableBytesWritable,byte[]>, String>() {
			transient Deserializer<Result> deserializer;
			
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				deserializer = new ResultSerialization().getDeserializer(Result.class);
			}

			@Override
			public String map(Tuple2<ImmutableBytesWritable, byte[]> value) throws Exception {
				StringValue s = new StringValue();
				byte[] resBytes = value.getField(1);
		        InputStream bais = new ByteArrayInputStream(resBytes);
				deserializer.open(bais);
				//TODO fix Flink GenericType serialization to avoid to manually ser/deser
		        Result result = deserializer.deserialize(null);
		        deserializer.close();
		        byte[] row = result.getRow();
		        s.setValueAscii(row, 0, row.length);
		        return s.toString();
			}
		})
		.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value)
					throws Exception {
				if(value.startsWith("someStr"))
					return true;
				return false;
			}
		});

		String output = "file:///tmp/flink-test";
		hbaseDs.writeAsText(output, WriteMode.OVERWRITE);
//		hbaseDs.output(new GenericTableOutputFormat());
		env.execute();
	}

}
