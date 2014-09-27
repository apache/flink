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


package org.apache.flink.hadoopcompatibility.mapred.record.datatypes;

import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


/**
 * Converter for the default hadoop writables.
 * Key will be in field 0, Value in field 1 of a Record.
 */
public class DefaultHadoopTypeConverter<K, V> implements HadoopTypeConverter<K, V> {
	private static final long serialVersionUID = 1L;

	@Override
	public void convert(Record flinkRecord, K hadoopKey, V hadoopValue) {
		flinkRecord.setField(0, convert(hadoopKey));
		flinkRecord.setField(1, convert(hadoopValue));
	}
	
	protected Value convert(Object hadoopType) {
		if(hadoopType instanceof org.apache.hadoop.io.LongWritable ) {
			return new LongValue(((LongWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.Text) {
			return new StringValue(((Text)hadoopType).toString());
		}
		if(hadoopType instanceof org.apache.hadoop.io.IntWritable) {
			return new IntValue(((IntWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.FloatWritable) {
			return new FloatValue(((FloatWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.DoubleWritable) {
			return new DoubleValue(((DoubleWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.BooleanWritable) {
			return new BooleanValue(((BooleanWritable)hadoopType).get());
		}
		if(hadoopType instanceof org.apache.hadoop.io.ByteWritable) {
			return new ByteValue(((ByteWritable)hadoopType).get());
		}
		if (hadoopType instanceof NullWritable) {
			return NullValue.getInstance();
		}
		
		throw new RuntimeException("Unable to convert Hadoop type ("+hadoopType.getClass().getCanonicalName()+") to a Flink data type.");
	}
}
