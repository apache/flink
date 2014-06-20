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

package eu.stratosphere.hadoopcompatibility.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.hadoopcompatibility.mapreduce.utils.HadoopUtils;


public class HadoopOutputFormat<K extends Writable,V extends Writable> implements OutputFormat<Tuple2<K, V>> {
	
	private static final long serialVersionUID = 1L;
	
	private org.apache.hadoop.conf.Configuration configuration;
	private org.apache.hadoop.mapreduce.OutputFormat<K,V> mapreduceOutputFormat;
	private transient RecordWriter<K,V> recordWriter;
	private transient FileOutputCommitter fileOutputCommitter;
	private transient TaskAttemptContext context;
	
	public HadoopOutputFormat(org.apache.hadoop.mapreduce.OutputFormat<K,V> mapreduceOutputFormat, Job job) {
		super();
		this.mapreduceOutputFormat = mapreduceOutputFormat;
		this.configuration = job.getConfiguration();
		HadoopUtils.mergeHadoopConf(configuration);
	}
	
	public void setConfiguration(org.apache.hadoop.conf.Configuration configuration) {
		this.configuration = configuration;
	}
	
	public org.apache.hadoop.conf.Configuration getConfiguration() {
		return this.configuration;
	}
	
	public org.apache.hadoop.mapreduce.OutputFormat<K,V> getHadoopOutputFormat() {
		return this.mapreduceOutputFormat;
	}
	
	public void setHadoopOutputFormat(org.apache.hadoop.mapreduce.OutputFormat<K,V> mapreduceOutputFormat) {
		this.mapreduceOutputFormat = mapreduceOutputFormat;
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
		
		// for hadoop 2.2
		this.configuration.set("mapreduce.output.basename", "tmp");
		
		TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt__0000_r_" 
				+ String.format("%" + (6 - Integer.toString(taskNumber + 1).length()) + "s"," ").replace(" ", "0") 
				+ Integer.toString(taskNumber + 1) 
				+ "_0");
		
		try {
			this.context = HadoopUtils.instantiateTaskAttemptContext(this.configuration, taskAttemptID);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		this.configuration.set("mapred.task.id", taskAttemptID.toString());
		// for hadoop 2.2
		this.configuration.set("mapreduce.task.attempt.id", taskAttemptID.toString());
		
		this.fileOutputCommitter = new FileOutputCommitter(new Path(this.configuration.get("mapred.output.dir")), context);
		
		try {
			this.fileOutputCommitter.setupJob(HadoopUtils.instantiateJobContext(this.configuration, new JobID()));
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		// compatible for hadoop 2.2.0, the temporary output directory is different from hadoop 1.2.1
		this.configuration.set("mapreduce.task.output.dir", this.fileOutputCommitter.getWorkPath().toString());
		
		try {
			this.recordWriter = this.mapreduceOutputFormat.getRecordWriter(this.context);
		} catch (InterruptedException e) {
			throw new IOException("Could not create RecordWriter.", e);
		}
	}
	
	
	@Override
	public void writeRecord(Tuple2<K, V> record) throws IOException {
		try {
			this.recordWriter.write(record.f0, record.f1);
		} catch (InterruptedException e) {
			throw new IOException("Could not write Record.", e);
		}
	}
	
	/**
	 * commit the task by moving the output file out from the temporary directory.
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	@Override
	public void close() throws IOException {
		try {
			this.recordWriter.close(this.context);
		} catch (InterruptedException e) {
			throw new IOException("Could not close RecordReader.", e);
		}
		
		if (this.fileOutputCommitter.needsTaskCommit(this.context)) {
			this.fileOutputCommitter.commitTask(this.context);
		}
		this.fileOutputCommitter.commitJob(this.context);
		
		// rename tmp-* files to final name
		FileSystem fs = FileSystem.get(this.configuration);
		
		Path outputPath = new Path(this.configuration.get("mapred.output.dir"));

		final Pattern p = Pattern.compile("tmp-(.)-([0-9]+)");
		
		// isDirectory does not work in hadoop 1
		if(fs.getFileStatus(outputPath).isDir()) {
			FileStatus[] files = fs.listStatus(outputPath);
			
			for(FileStatus f : files) {
				Matcher m = p.matcher(f.getPath().getName());
				if(m.matches()) {
					int part = Integer.valueOf(m.group(2));
					fs.rename(f.getPath(), new Path(outputPath.toString()+"/"+part));
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------
	
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeUTF(this.mapreduceOutputFormat.getClass().getName());
		this.configuration.write(out);
	}
	
	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		String hadoopOutputFormatClassName = in.readUTF();
		
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		configuration.readFields(in);
		
		if(this.configuration == null) {
			this.configuration = configuration;
		}
		
		try {
			this.mapreduceOutputFormat = (org.apache.hadoop.mapreduce.OutputFormat<K,V>) Class.forName(hadoopOutputFormatClassName, true, Thread.currentThread().getContextClassLoader()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop output format", e);
		}
	}
}