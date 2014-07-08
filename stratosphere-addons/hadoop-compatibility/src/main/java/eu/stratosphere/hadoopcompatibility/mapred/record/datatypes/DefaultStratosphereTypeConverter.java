/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.hadoopcompatibility.mapred.record.datatypes;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.ByteValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

/**
 * Converter Stratosphere Record into the default hadoop writables.
 *
 */
public class DefaultStratosphereTypeConverter<K,V> implements StratosphereTypeConverter<K,V> {
	private static final long serialVersionUID = 1L;

	private Class<K> keyClass;
	private Class<V> valueClass;

	public DefaultStratosphereTypeConverter(Class<K> keyClass, Class<V> valueClass) {
		this.keyClass= keyClass;
		this.valueClass = valueClass;
	}
	@Override
	public K convertKey(Record stratosphereRecord) {
		if(stratosphereRecord.getNumFields() > 0) {
			return convert(stratosphereRecord, 0, this.keyClass);
		} else {
			return null;
		}
	}

	@Override
	public V convertValue(Record stratosphereRecord) {
		if(stratosphereRecord.getNumFields() > 1) {
			return convert(stratosphereRecord, 1, this.valueClass);
		} else {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private<T> T convert(Record stratosphereType, int pos, Class<T> hadoopType) {
		if(hadoopType == LongWritable.class ) {
			return (T) new LongWritable((stratosphereType.getField(pos, LongValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.Text.class) {
			return (T) new Text((stratosphereType.getField(pos, StringValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.IntWritable.class) {
			return (T) new IntWritable((stratosphereType.getField(pos, IntValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.FloatWritable.class) {
			return (T) new FloatWritable((stratosphereType.getField(pos, FloatValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.DoubleWritable.class) {
			return (T) new DoubleWritable((stratosphereType.getField(pos, DoubleValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.BooleanWritable.class) {
			return (T) new BooleanWritable((stratosphereType.getField(pos, BooleanValue.class)).getValue());
		}
		if(hadoopType == org.apache.hadoop.io.ByteWritable.class) {
			return (T) new ByteWritable((stratosphereType.getField(pos, ByteValue.class)).getValue());
		}

		throw new RuntimeException("Unable to convert Stratosphere type ("+stratosphereType.getClass().getCanonicalName()+") to Hadoop.");
	}
}
