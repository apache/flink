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


package org.apache.flink.hadoopcompatibility.mapred.record;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.hadoopcompatibility.mapred.record.datatypes.HadoopTypeConverter;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopInputSplit;
import org.apache.flink.types.Record;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

public class HadoopRecordInputFormat<K, V> implements InputFormat<Record, HadoopInputSplit> {

	private static final long serialVersionUID = 1L;

	public org.apache.hadoop.mapred.InputFormat<K, V> hadoopInputFormat;
	public HadoopTypeConverter<K,V> converter;
	private String hadoopInputFormatName;
	public JobConf jobConf;
	public transient K key;
	public transient V value;
	public RecordReader<K, V> recordReader;
	private boolean fetched = false;
	private boolean hasNext;
		
	public HadoopRecordInputFormat() {
		super();
	}
	
	public HadoopRecordInputFormat(org.apache.hadoop.mapred.InputFormat<K,V> hadoopInputFormat, JobConf job, HadoopTypeConverter<K,V> conv) {
		super();
		this.hadoopInputFormat = hadoopInputFormat;
		this.hadoopInputFormatName = hadoopInputFormat.getClass().getName();
		this.converter = conv;
		HadoopUtils.mergeHadoopConf(job);
		this.jobConf = job;
	}

	@Override
	public void configure(Configuration parameters) {
		
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public HadoopInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {
		org.apache.hadoop.mapred.InputSplit[] splitArray = hadoopInputFormat.getSplits(jobConf, minNumSplits);
		HadoopInputSplit[] hiSplit = new HadoopInputSplit[splitArray.length];
		for(int i=0;i<splitArray.length;i++){
			hiSplit[i] = new HadoopInputSplit(i, splitArray[i], jobConf);
		}
		return hiSplit;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HadoopInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(HadoopInputSplit split) throws IOException {
		this.recordReader = this.hadoopInputFormat.getRecordReader(split.getHadoopInputSplit(), jobConf, new HadoopDummyReporter());
		key = this.recordReader.createKey();
		value = this.recordReader.createValue();
		this.fetched = false;
	}

	private void fetchNext() throws IOException {
		hasNext = this.recordReader.next(key, value);
		fetched = true;
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		if(!fetched) {
			fetchNext();
		}
		return !hasNext;
	}

	@Override
	public Record nextRecord(Record record) throws IOException {
		if(!fetched) {
			fetchNext();
		}
		if(!hasNext) {
			return null;
		}
		converter.convert(record, key, value);
		fetched = false;
		return record;
	}

	@Override
	public void close() throws IOException {
		this.recordReader.close();
	}
	
	/**
	 * Custom serialization methods.
	 *  @see "http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html"
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeUTF(hadoopInputFormatName);
		jobConf.write(out);
		out.writeObject(converter);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		hadoopInputFormatName = in.readUTF();
		if(jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
		try {
			this.hadoopInputFormat = (org.apache.hadoop.mapred.InputFormat<K,V>) Class.forName(this.hadoopInputFormatName).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop input format", e);
		}
		ReflectionUtils.setConf(hadoopInputFormat, jobConf);
		converter = (HadoopTypeConverter<K,V>) in.readObject();
	}
	
	public void setJobConf(JobConf job) {
		this.jobConf = job;
	}
		

	public org.apache.hadoop.mapred.InputFormat<K,V> getHadoopInputFormat() {
		return hadoopInputFormat;
	}
	
	public void setHadoopInputFormat(org.apache.hadoop.mapred.InputFormat<K,V> hadoopInputFormat) {
		this.hadoopInputFormat = hadoopInputFormat;
	}
	
	public JobConf getJobConf() {
		return jobConf;
	}
}
