/**
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


package org.apache.flink.hadoopcompatibility.mapred.record;

import java.util.List;

import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.record.operators.GenericDataSink;
import org.apache.flink.compiler.contextcheck.Validatable;
import org.apache.flink.hadoopcompatibility.mapred.record.datatypes.DefaultFlinkTypeConverter;
import org.apache.flink.hadoopcompatibility.mapred.record.datatypes.FlinkTypeConverter;
import org.apache.flink.types.Record;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * The HadoopDataSink is a generic wrapper for all Hadoop OutputFormats.
 *
 * Example usage:
 * <pre>
 * 		HadoopDataSink out = new HadoopDataSink(new org.apache.hadoop.mapred.TextOutputFormat<Text, IntWritable>(), new JobConf(), "Hadoop TextOutputFormat",reducer, Text.class,IntWritable.class);
 *		org.apache.hadoop.mapred.TextOutputFormat.setOutputPath(out.getJobConf(), new Path(output));
 * </pre>
 *
 * Note that it is possible to provide custom data type converter.
 *
 * The HadoopDataSink provides a default converter: {@link org.apache.flink.hadoopcompatibility.mapred.record.datatypes.DefaultFlinkTypeConverter}
 **/
public class HadoopDataSink<K,V> extends GenericDataSink implements Validatable {

	private static String DEFAULT_NAME = "<Unnamed Hadoop Data Sink>";

	private JobConf jobConf;

	public HadoopDataSink(OutputFormat<K,V> hadoopFormat, JobConf jobConf, String name, Operator<Record> input, FlinkTypeConverter<K,V> conv, Class<K> keyClass, Class<V> valueClass) {
		this(hadoopFormat, jobConf, name, ImmutableList.<Operator<Record>>of(input), conv, keyClass, valueClass);
	}

	public HadoopDataSink(OutputFormat<K,V> hadoopFormat, JobConf jobConf, String name, Operator<Record> input, Class<K> keyClass, Class<V> valueClass) {
		this(hadoopFormat, jobConf, name, input, new DefaultFlinkTypeConverter<K, V>(keyClass, valueClass), keyClass, valueClass);
	}

	public HadoopDataSink(OutputFormat<K,V> hadoopFormat, JobConf jobConf, Operator<Record> input, Class<K> keyClass, Class<V> valueClass) {
		this(hadoopFormat, jobConf, DEFAULT_NAME, input, new DefaultFlinkTypeConverter<K, V>(keyClass, valueClass), keyClass, valueClass);
	}

	public HadoopDataSink(OutputFormat<K,V> hadoopFormat, Operator<Record> input, Class<K> keyClass, Class<V> valueClass) {
		this(hadoopFormat, new JobConf(), DEFAULT_NAME, input, new DefaultFlinkTypeConverter<K, V>(keyClass, valueClass), keyClass, valueClass);
	}



	@SuppressWarnings("deprecation")
	public HadoopDataSink(OutputFormat<K,V> hadoopFormat, JobConf jobConf, String name, List<Operator<Record>> input, FlinkTypeConverter<K,V> conv, Class<K> keyClass, Class<V> valueClass) {
		super(new HadoopRecordOutputFormat<K,V>(hadoopFormat, jobConf, conv),input, name);
		Preconditions.checkNotNull(hadoopFormat);
		Preconditions.checkNotNull(jobConf);
		this.name = name;
		this.jobConf = jobConf;
		jobConf.setOutputKeyClass(keyClass);
		jobConf.setOutputValueClass(valueClass);
	}

	public HadoopDataSink(OutputFormat<K,V> hadoopFormat, JobConf jobConf, String name, List<Operator<Record>> input, Class<K> keyClass, Class<V> valueClass) {
		this(hadoopFormat, jobConf, name, input, new DefaultFlinkTypeConverter<K, V>(keyClass, valueClass), keyClass, valueClass);
	}

	public HadoopDataSink(OutputFormat<K,V> hadoopFormat, JobConf jobConf, List<Operator<Record>> input, Class<K> keyClass, Class<V> valueClass) {
		this(hadoopFormat, jobConf, DEFAULT_NAME, input, new DefaultFlinkTypeConverter<K, V>(keyClass, valueClass), keyClass, valueClass);
	}

	public HadoopDataSink(OutputFormat<K,V> hadoopFormat, List<Operator<Record>> input, Class<K> keyClass, Class<V> valueClass) {
		this(hadoopFormat, new JobConf(), DEFAULT_NAME, input, new DefaultFlinkTypeConverter<K, V>(keyClass, valueClass), keyClass, valueClass);
	}

	public JobConf getJobConf() {
		return this.jobConf;
	}

	@Override
	public void check() {
		// see for more details https://github.com/stratosphere/stratosphere/pull/531
		Preconditions.checkNotNull(FileOutputFormat.getOutputPath(jobConf), "The HadoopDataSink currently expects a correct outputPath.");
	}

}
