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
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Convert Flink Record into the default hadoop writables.
 */
public class DefaultFlinkTypeConverter<K,V> implements FlinkTypeConverter<K,V> {
	private static final long serialVersionUID = 1L;

	private Class<K> keyClass;
	private Class<V> valueClass;

	public DefaultFlinkTypeConverter(Class<K> keyClass, Class<V> valueClass) {
		this.keyClass= keyClass;
		this.valueClass = valueClass;
	}
	@Override
	public K convertKey(Record flinkRecord) {
		if(flinkRecord.getNumFields() > 0) {
			return convert(flinkRecord, 0, this.keyClass);
		} else {
			return null;
		}
	}

	@Override
	public V convertValue(Record flinkRecord) {
		if(flinkRecord.getNumFields() > 1) {
			return convert(flinkRecord, 1, this.valueClass);
		} else {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private<T> T convert(Record flinkType, int pos, Class<T> hadoopType) {
		if(hadoopType == LongWritable.class ) {
			return (T) new LongWritable((flinkType.getField(pos, LongValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.Text.class) {
			return (T) new Text((flinkType.getField(pos, StringValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.IntWritable.class) {
			return (T) new IntWritable((flinkType.getField(pos, IntValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.FloatWritable.class) {
			return (T) new FloatWritable((flinkType.getField(pos, FloatValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.DoubleWritable.class) {
			return (T) new DoubleWritable((flinkType.getField(pos, DoubleValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.BooleanWritable.class) {
			return (T) new BooleanWritable((flinkType.getField(pos, BooleanValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.ByteWritable.class) {
			return (T) new ByteWritable((flinkType.getField(pos, ByteValue.class)).getValue());
		}

		throw new RuntimeException("Unable to convert Flink type ("+flinkType.getClass().getCanonicalName()+") to Hadoop.");
	}
}
