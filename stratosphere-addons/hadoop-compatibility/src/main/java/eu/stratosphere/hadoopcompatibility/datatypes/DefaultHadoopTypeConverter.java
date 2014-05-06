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

package eu.stratosphere.hadoopcompatibility.datatypes;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.ByteValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;


/**
 * Converter for the default hadoop writables.
 * Key will be in field 0, Value in field 1 of a Stratosphere Record.
 */
public class DefaultHadoopTypeConverter<K, V> implements HadoopTypeConverter<K, V> {
	private static final long serialVersionUID = 1L;

	@Override
	public void convert(Record stratosphereRecord, K hadoopKey, V hadoopValue) {
		stratosphereRecord.setField(0, convert(hadoopKey));
		stratosphereRecord.setField(1, convert(hadoopValue));
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
		
		throw new RuntimeException("Unable to convert Hadoop type ("+hadoopType.getClass().getCanonicalName()+") to Stratosphere.");
	}
}
