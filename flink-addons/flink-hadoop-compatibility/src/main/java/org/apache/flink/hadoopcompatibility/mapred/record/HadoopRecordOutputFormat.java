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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.mapred.record.datatypes.HadoopFileOutputCommitter;
import org.apache.flink.hadoopcompatibility.mapred.record.datatypes.FlinkTypeConverter;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyProgressable;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.types.Record;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;


public class HadoopRecordOutputFormat<K,V> implements OutputFormat<Record> {

	private static final long serialVersionUID = 1L;

	public JobConf jobConf;

	public org.apache.hadoop.mapred.OutputFormat<K,V> hadoopOutputFormat;

	private String hadoopOutputFormatName;

	public RecordWriter<K,V> recordWriter;

	public FlinkTypeConverter<K,V> converter;

	public HadoopFileOutputCommitter fileOutputCommitterWrapper;

	public HadoopRecordOutputFormat(org.apache.hadoop.mapred.OutputFormat<K,V> hadoopFormat, JobConf job, FlinkTypeConverter<K,V> conv) {
		super();
		this.hadoopOutputFormat = hadoopFormat;
		this.hadoopOutputFormatName = hadoopFormat.getClass().getName();
		this.converter = conv;
		this.fileOutputCommitterWrapper = new HadoopFileOutputCommitter();
		HadoopUtils.mergeHadoopConf(job);
		this.jobConf = job;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	/**
	 * create the temporary output file for hadoop RecordWriter.
	 * @param taskNumber The number of the parallel instance.
	 * @param numTasks The number of parallel tasks.
	 * @throws IOException
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.fileOutputCommitterWrapper.setupJob(this.jobConf);
		if (Integer.toString(taskNumber + 1).length() <= 6) {
			this.jobConf.set("mapred.task.id", "attempt__0000_r_" + String.format("%" + (6 - Integer.toString(taskNumber + 1).length()) + "s"," ").replace(" ", "0") + Integer.toString(taskNumber + 1) + "_0");
			//compatible for hadoop 2.2.0, the temporary output directory is different from hadoop 1.2.1
			this.jobConf.set("mapreduce.task.output.dir", this.fileOutputCommitterWrapper.getTempTaskOutputPath(this.jobConf,TaskAttemptID.forName(this.jobConf.get("mapred.task.id"))).toString());
		} else {
			throw new IOException("task id too large");
		}
		this.recordWriter = this.hadoopOutputFormat.getRecordWriter(null, this.jobConf, Integer.toString(taskNumber + 1), new HadoopDummyProgressable());
	}


	@Override
	public void writeRecord(Record record) throws IOException {
		K key = this.converter.convertKey(record);
		V value = this.converter.convertValue(record);
		this.recordWriter.write(key, value);
	}

	/**
	 * commit the task by moving the output file out from the temporary directory.
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		this.recordWriter.close(new HadoopDummyReporter());
		if (this.fileOutputCommitterWrapper.needsTaskCommit(this.jobConf, TaskAttemptID.forName(this.jobConf.get("mapred.task.id")))) {
			this.fileOutputCommitterWrapper.commitTask(this.jobConf, TaskAttemptID.forName(this.jobConf.get("mapred.task.id")));
		}
	//TODO: commitjob when all the tasks are finished
	}


	/**
	 * Custom serialization methods.
	 *  @see "http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html"
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeUTF(hadoopOutputFormatName);
		jobConf.write(out);
		out.writeObject(converter);
		out.writeObject(fileOutputCommitterWrapper);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		hadoopOutputFormatName = in.readUTF();
		if(jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
		try {
			this.hadoopOutputFormat = (org.apache.hadoop.mapred.OutputFormat<K,V>) Class.forName(this.hadoopOutputFormatName).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop output format", e);
		}
		ReflectionUtils.setConf(hadoopOutputFormat, jobConf);
		converter = (FlinkTypeConverter<K,V>) in.readObject();
		fileOutputCommitterWrapper = (HadoopFileOutputCommitter) in.readObject();
	}


	public void setJobConf(JobConf job) {
		this.jobConf = job;
	}

	public JobConf getJobConf() {
		return jobConf;
	}

	public org.apache.hadoop.mapred.OutputFormat<K,V> getHadoopOutputFormat() {
		return hadoopOutputFormat;
	}

	public void setHadoopOutputFormat(org.apache.hadoop.mapred.OutputFormat<K,V> hadoopOutputFormat) {
		this.hadoopOutputFormat = hadoopOutputFormat;
	}

}
