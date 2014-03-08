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

package eu.stratosphere.hadoopcompatibility;


import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.hadoopcompatibility.datatypes.DefaultHadoopTypeConverter;
import eu.stratosphere.hadoopcompatibility.datatypes.HadoopTypeConverter;

/**
 * The HadoopDataSource is a generic wrapper for all Hadoop InputFormats.
 * 
 * Example usage:
 * <pre>
 * 		HadoopDataSource source = new HadoopDataSource(new org.apache.hadoop.mapred.TextInputFormat(), new JobConf(), "Input Lines");
 *		org.apache.hadoop.mapred.TextInputFormat.addInputPath(source.getJobConf(), new Path(dataInput));
 * </pre>
 * 
 * Note that it is possible to provide custom data type converter.
 * 
 * The HadoopDataSource provides two different standard converters:
 * * WritableWrapperConverter: Converts Hadoop Types to a record that contains a WritableComparableWrapper (key) and a WritableWrapper
 * * DefaultHadoopTypeConverter: Converts the standard hadoop types (longWritable, Text) to Stratosphere's standard types.
 *
 */
public class HadoopDataSource<K,V> extends GenericDataSource<HadoopInputFormatWrapper<K,V>> {

	private static String DEFAULT_NAME = "<Unnamed Hadoop Data Source>";
	
	private JobConf jobConf;
	
	/**
	 * 
	 * @param hadoopFormat Implementation of a Hadoop input format
	 * @param jobConf JobConf object (Hadoop)
	 * @param name Name of the DataSource
	 * @param conv Definition of a custom type converter {@link DefaultHadoopTypeConverter}.
	 */
	public HadoopDataSource(InputFormat<K,V> hadoopFormat, JobConf jobConf, String name, HadoopTypeConverter<K,V> conv) {
		super(new HadoopInputFormatWrapper<K,V>(hadoopFormat, jobConf, conv),name);
		Preconditions.checkNotNull(hadoopFormat);
		Preconditions.checkNotNull(jobConf);
		Preconditions.checkNotNull(conv);
		this.name = name;
		this.jobConf = jobConf;
	}
	
	public HadoopDataSource(InputFormat<K,V> hadoopFormat, JobConf jobConf, String name) {
		this(hadoopFormat, jobConf, name, new DefaultHadoopTypeConverter<K,V>() );
	}
	public HadoopDataSource(InputFormat<K,V> hadoopFormat, JobConf jobConf) {
		this(hadoopFormat, jobConf, DEFAULT_NAME);
	}
	
	public HadoopDataSource(InputFormat<K,V> hadoopFormat) {
		this(hadoopFormat, new JobConf(), DEFAULT_NAME);
	}

	public JobConf getJobConf() {
		return this.jobConf;
	}

}
