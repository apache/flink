/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.hadoopcompatibility.mapred;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.hadoopcompatibility.mapred.utils.HadoopUtils;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopDummyProgressable;
import eu.stratosphere.hadoopcompatibility.mapred.wrapper.HadoopDummyReporter;


public class HadoopOutputFormat<K extends Writable,V extends Writable> implements OutputFormat<Tuple2<K, V>> {
	
	private static final long serialVersionUID = 1L;
	
	private JobConf jobConf;	
	private org.apache.hadoop.mapred.OutputFormat<K,V> mapredOutputFormat;	
	private transient RecordWriter<K,V> recordWriter;	
	private transient FileOutputCommitter fileOutputCommitter;
	private transient TaskAttemptContext context;
	private transient JobContext jobContext;
	
	public HadoopOutputFormat(org.apache.hadoop.mapred.OutputFormat<K,V> mapredOutputFormat, JobConf job) {
		super();
		this.mapredOutputFormat = mapredOutputFormat;
		HadoopUtils.mergeHadoopConf(job);
		this.jobConf = job;
	}
	
	public void setJobConf(JobConf job) {
		this.jobConf = job;
	}
	
	public JobConf getJobConf() {
		return jobConf;
	}
	
	public org.apache.hadoop.mapred.OutputFormat<K,V> getHadoopOutputFormat() {
		return mapredOutputFormat;
	}
	
	public void setHadoopOutputFormat(org.apache.hadoop.mapred.OutputFormat<K,V> mapredOutputFormat) {
		this.mapredOutputFormat = mapredOutputFormat;
	}
	
	// --------------------------------------------------------------------------------------------
	//  OutputFormat
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void configure(Configuration parameters) {
		// nothing to do
	}
	
	/**
	 * create the temporary output file for hadoop RecordWriter.
	 * @param taskNumber The number of the parallel instance.
	 * @param numTasks The number of parallel tasks.
	 * @throws IOException
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		if (Integer.toString(taskNumber + 1).length() > 6) {
			throw new IOException("Task id too large.");
		}
		
		TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt__0000_r_" 
				+ String.format("%" + (6 - Integer.toString(taskNumber + 1).length()) + "s"," ").replace(" ", "0") 
				+ Integer.toString(taskNumber + 1) 
				+ "_0");
		
		try {
			this.context = HadoopUtils.instantiateTaskAttemptContext(this.jobConf, taskAttemptID);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		this.jobConf.set("mapred.task.id", taskAttemptID.toString());
		// for hadoop 2.2
		this.jobConf.set("mapreduce.task.attempt.id", taskAttemptID.toString());
		
		this.fileOutputCommitter = new FileOutputCommitter();
		
		try {
			this.jobContext = HadoopUtils.instantiateJobContext(this.jobConf, new JobID());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		this.fileOutputCommitter.setupJob(jobContext);
		
		this.recordWriter = this.mapredOutputFormat.getRecordWriter(null, this.jobConf, Integer.toString(taskNumber + 1), new HadoopDummyProgressable());
	}
	
	@Override
	public void writeRecord(Tuple2<K, V> record) throws IOException {
		this.recordWriter.write(record.f0, record.f1);
	}
	
	/**
	 * commit the task by moving the output file out from the temporary directory.
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		this.recordWriter.close(new HadoopDummyReporter());
		
		if (this.fileOutputCommitter.needsTaskCommit(this.context)) {
			this.fileOutputCommitter.commitTask(this.context);
		}
		this.fileOutputCommitter.commitJob(this.jobContext);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------
	
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeUTF(mapredOutputFormat.getClass().getName());
		jobConf.write(out);
	}
	
	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		String hadoopOutputFormatName = in.readUTF();
		if(jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
		try {
			this.mapredOutputFormat = (org.apache.hadoop.mapred.OutputFormat<K,V>) Class.forName(hadoopOutputFormatName, true, Thread.currentThread().getContextClassLoader()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop output format", e);
		}
		ReflectionUtils.setConf(mapredOutputFormat, jobConf);
	}
}
