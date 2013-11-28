/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.addons.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.generic.io.OutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

public abstract class GenericTableOutputFormat implements OutputFormat<PactRecord> {

	public static final String JT_ID_KEY = "pact.hbase.jtkey";

	public static final String JOB_ID_KEY = "pact.job.id";

	private RecordWriter<ImmutableBytesWritable, KeyValue> writer;

	private Configuration config;

	private org.apache.hadoop.conf.Configuration hadoopConfig;

	private TaskAttemptContext context;

	private String jtID;

	private int jobId;


	@Override
	public void configure(Configuration parameters) {
		this.config = parameters;

		// get the ID parameters
		this.jtID = parameters.getString(JT_ID_KEY, null);
		if (this.jtID == null) {
			throw new RuntimeException("Missing JT_ID entry in hbase config.");
		}
		this.jobId = parameters.getInteger(JOB_ID_KEY, -1);
		if (this.jobId < 0) {
			throw new RuntimeException("Missing or invalid job id in input config.");
		}
	}

	@Override
	public void open(int taskNumber) throws IOException {
		this.hadoopConfig = getHadoopConfig(this.config);
		
		/**
		 * PLASE NOTE:
		 * If you are a Eclipse+Maven Integration user and you have two (or more) warnings here, please
		 * close the pact-hbase project OR set the maven profile to hadoop_yarn
		 * 
		 * pact-hbase requires hadoop_yarn, but Eclipse is not able to parse maven profiles properly. Therefore,
		 * it imports the pact-hbase project even if it is not included in the standard profile (hadoop_v1)
		 */
		final TaskAttemptID attemptId = new TaskAttemptID(this.jtID, this.jobId, TaskType.MAP, taskNumber - 1, 0);

		this.context = new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(this.hadoopConfig, attemptId);
		final HFileOutputFormat outFormat = new HFileOutputFormat();
		try {
			this.writer = outFormat.getRecordWriter(this.context);
		} catch (InterruptedException iex) {
			throw new IOException("Opening the writer was interrupted.", iex);
		}
	}

	@Override
	public void close() throws IOException {
		final RecordWriter<ImmutableBytesWritable, KeyValue> writer = this.writer;
		this.writer = null;
		if (writer != null) {
			try {
				writer.close(this.context);
			} catch (InterruptedException iex) {
				throw new IOException("Closing was interrupted.", iex);
			}
		}
	}

	public void collectKeyValue(KeyValue kv) throws IOException {
		try {
			this.writer.write(null, kv);
		} catch (InterruptedException iex) {
			throw new IOException("Write request was interrupted.", iex);
		}
	}

	public abstract org.apache.hadoop.conf.Configuration getHadoopConfig(Configuration config);
}
