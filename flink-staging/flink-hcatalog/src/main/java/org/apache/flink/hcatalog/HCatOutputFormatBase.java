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

package org.apache.flink.hcatalog;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

public abstract class HCatOutputFormatBase<T> implements OutputFormat<T>, FinalizeOnMaster {
	private static final long serialVersionUID = 1L;

	private Configuration configuration;
	//a Job is used to create OutputJobInfo
	private Job job;

	private org.apache.hive.hcatalog.mapreduce.HCatOutputFormat hCatOutputFormat;
	private RecordWriter<WritableComparable<?>, HCatRecord> recordWriter;

	//required data type by the table
	protected TypeInformation<T> reqType;

	protected String[] fieldNames = new String[0];
	protected HCatSchema outputSchema;
	private transient TaskAttemptContext context;
	private transient JobContext jobContext;


	@Override
	public void configure(org.apache.flink.configuration.Configuration parameters) {
		// configure MR OutputFormat if necessary
		if (this.hCatOutputFormat instanceof Configurable) {
			((Configurable) this.hCatOutputFormat).setConf(configuration);
		} else if (this.hCatOutputFormat instanceof JobConfigurable) {
			((JobConfigurable) this.hCatOutputFormat).configure(new JobConf(configuration));
		}
	}

	public HCatOutputFormatBase() {
	}

	/**
	 * Creates a HCatOutputFormat for the given database and table and a Map of PartitionValues
	 * By default. The RecordWriter of
	 * {@link HCatInputFormatBase#asFlinkTuples()}.
	 *
	 * @param database The name of the database to read from.
	 * @param table    The name of the table to read.
	 * @throws java.io.IOException
	 */
	public HCatOutputFormatBase(String database, String table, Map<String, String> partitionValues)
			throws IOException {
		this(database, table, partitionValues, new Configuration());
	}

	protected abstract HCatRecord TupleToHCatRecord(T record) throws IOException;

	protected abstract int getMaxFlinkTupleSize();

	/**
	 * Creates a HCatOutputFormat for the given database, table, partitionValues
	 * {@link org.apache.hadoop.conf.Configuration}.
	 * if
	 *
	 * @param database        The name of the database to read from.
	 * @param table           The name of the table to read.
	 * @param config          The Configuration for the InputFormat.
	 * @param partitionValues the map of partition values. if null, the job writes to unpartitioned
	 *                        table
	 * @throws java.io.IOException
	 */
	public HCatOutputFormatBase(String database, String table, Map<String, String> partitionValues,
								Configuration config) throws IOException {
		super();
		this.configuration = config;
		HadoopUtils.mergeHadoopConf(this.configuration);
		this.job = Job.getInstance(this.configuration);


		OutputJobInfo jobInfo = OutputJobInfo.create(database, table, partitionValues);

		org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.setOutput(job, jobInfo);

		this.hCatOutputFormat = new org.apache.hive.hcatalog.mapreduce.HCatOutputFormat();

		this.configuration.set("mapreduce.lib.hcatoutput.info", HCatUtil.serialize(jobInfo));
		this.outputSchema = jobInfo.getOutputSchema();
		org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.
				setSchema(this.configuration, this.outputSchema);
		int numFields = outputSchema.getFields().size();
		if (numFields > this.getMaxFlinkTupleSize()) {
			throw new IllegalArgumentException("Only up to " + this.getMaxFlinkTupleSize() +
					" fields can be accepted as Flink tuples target table.");
		}
		TypeInformation[] fieldTypes = new TypeInformation[numFields];
		fieldNames = new String[numFields];
		for (String fieldName : outputSchema.getFieldNames()) {
			HCatFieldSchema field = outputSchema.get(fieldName);

			int fieldPos = outputSchema.getPosition(fieldName);
			TypeInformation fieldType = getFieldType(field);

			fieldTypes[fieldPos] = fieldType;
			fieldNames[fieldPos] = fieldName;

		}
		this.reqType = new TupleTypeInfo(fieldTypes);
	}


	private TypeInformation getFieldType(HCatFieldSchema fieldSchema) {

		switch (fieldSchema.getType()) {
			case INT:
				return BasicTypeInfo.INT_TYPE_INFO;
			case TINYINT:
				return BasicTypeInfo.BYTE_TYPE_INFO;
			case SMALLINT:
				return BasicTypeInfo.SHORT_TYPE_INFO;
			case BIGINT:
				return BasicTypeInfo.LONG_TYPE_INFO;
			case BOOLEAN:
				return BasicTypeInfo.BOOLEAN_TYPE_INFO;
			case FLOAT:
				return BasicTypeInfo.FLOAT_TYPE_INFO;
			case DOUBLE:
				return BasicTypeInfo.DOUBLE_TYPE_INFO;
			case STRING:
				return BasicTypeInfo.STRING_TYPE_INFO;
			case BINARY:
				return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			case ARRAY:
				return new GenericTypeInfo(List.class);
			case MAP:
				return new GenericTypeInfo(Map.class);
			case STRUCT:
				return new GenericTypeInfo(List.class);
			default:
				throw new IllegalArgumentException("Unknown data type \"" +
						fieldSchema.getType() + "\" encountered.");
		}
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		/*code adapted from
		 *{@link org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormatBase}
		 */
		if (Integer.toString(taskNumber + 1).length() > 6) {
			throw new IOException("Task id too large.");
		}

		TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt__0000_r_"
				+ String.format("%" + (6 - Integer.toString(taskNumber + 1).length()) + "s", " ")
				.replace(" ", "0")
				+ Integer.toString(taskNumber + 1)
				+ "_0");

		try {
			this.context = HadoopUtils.instantiateTaskAttemptContext(this.configuration,
					taskAttemptID);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		// for mapred api
		this.context.getConfiguration().set("mapred.task.id", taskAttemptID.toString());
		this.context.getConfiguration().setInt("mapred.task.partition", taskNumber + 1);
		// for mapreduce api
		this.context.getConfiguration().set("mapreduce.task.attempt.id", taskAttemptID.toString());
		this.context.getConfiguration().setInt("mapreduce.task.partition", taskNumber + 1);

		try {
			this.recordWriter = this.hCatOutputFormat.getRecordWriter(this.context);
		} catch (InterruptedException e) {
			throw new IOException("Could not create RecordReader.", e);
		}
	}


	@Override
	public void writeRecord(T record) throws IOException {
		try {
			this.recordWriter.write(NullWritable.get(), TupleToHCatRecord(record));
		} catch (InterruptedException e) {
			throw new IOException("Could not write Record.", e);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			this.recordWriter.close(this.context);
		} catch (InterruptedException e) {
			throw new IOException("Could not close RecordReader.", e);
		}
		try {
			OutputCommitter committer = this.hCatOutputFormat.getOutputCommitter(this.context);
			if (committer.needsTaskCommit(this.context)) {
				committer.commitTask(this.context);
			}
		} catch (InterruptedException e) {
			throw new IOException("Could not commit task " + this.context.getTaskAttemptID(), e);
		}

	}


	@Override
	public void finalizeGlobal(int parallelism) throws IOException {
		// finalize the job
		try {
			OutputCommitter committer = this.hCatOutputFormat.getOutputCommitter(this.context);
			committer.commitJob(context);
		} catch (InterruptedException e) {
			throw new IOException("Could not commit job " + jobContext.getJobID(), e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeInt(this.fieldNames.length);
		for (String fieldName : this.fieldNames) {
			out.writeUTF(fieldName);
		}
		this.configuration.write(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.fieldNames = new String[in.readInt()];
		for (int i = 0; i < this.fieldNames.length; i++) {
			this.fieldNames[i] = in.readUTF();
		}

		Configuration config = new Configuration();
		config.readFields(in);

		if (this.configuration == null) {
			this.configuration = config;
		}

		this.hCatOutputFormat = new org.apache.hive.hcatalog.mapreduce.HCatOutputFormat();
		this.outputSchema = (HCatSchema) HCatUtil.deserialize(
				this.configuration.get("mapreduce.lib.hcat.output.schema"));
		org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.
				setSchema(this.configuration, this.outputSchema);
	}
}
