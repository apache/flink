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

package org.apache.flink.table.sinks.orc;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.SafetyNetWrapperFileSystem;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sources.orc.OrcSchemaConverter;
import org.apache.flink.table.sources.orc.OrcSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.orc.CompressionKind;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.apache.orc.OrcConf.COMPRESS;
import static org.apache.orc.OrcConf.MAPRED_OUTPUT_SCHEMA;
import static org.apache.orc.OrcConf.ROW_INDEX_STRIDE;

/**
 * A subclass of {@link OutputFormat} to write {@link BaseRow} to Orc files.
 */
public class RowOrcOutputFormat implements OutputFormat<BaseRow> {

	private static final Logger LOG = LoggerFactory.getLogger(RowOrcOutputFormat.class);

	private static final String FILE_PREFIX_NAME = "orc-";

	private static final int DEFAULT_ROW_INDEX_STRIDE = 10000;

	private final InternalType[] fieldTypes;
	private final String[] fieldNames;

	private final String dir;
	private final CompressionKind compression;
	private final String filePrefixName;
	private final int rowIndexStride;

	private org.apache.hadoop.mapreduce.RecordWriter<Void, OrcStruct> realWriter;
	private TaskAttemptContext taskContext;

	private final OrcSerializer serializer;
	private final String typeDescription;
	private transient OrcStruct struct;

	public RowOrcOutputFormat(
		InternalType[] fieldTypes, String[] fieldNames, String dir) {
		this(fieldTypes, fieldNames, dir, CompressionKind.NONE, FILE_PREFIX_NAME, DEFAULT_ROW_INDEX_STRIDE);
	}

	public RowOrcOutputFormat(
		InternalType[] fieldTypes, String[] fieldNames, String dir, CompressionKind compression) {
		this(fieldTypes, fieldNames, dir, compression, FILE_PREFIX_NAME, DEFAULT_ROW_INDEX_STRIDE);
	}

	public RowOrcOutputFormat(
		InternalType[] fieldTypes, String[] fieldNames, String dir,
		CompressionKind compression, String filePrefixName, int rowIndexStride) {
		Preconditions.checkArgument(fieldNames != null && fieldNames.length > 0);
		Preconditions.checkArgument(fieldTypes != null && fieldTypes.length == fieldNames.length);

		this.fieldTypes = fieldTypes;
		this.fieldNames = fieldNames;
		this.dir = dir;
		this.compression = compression;
		this.filePrefixName = filePrefixName;
		this.rowIndexStride = rowIndexStride;

		this.serializer = new OrcSerializer(fieldTypes, fieldNames);

		this.typeDescription = OrcSchemaConverter.convert(fieldTypes, fieldNames).toString();
		this.struct = (OrcStruct) OrcStruct.createValue(TypeDescription.fromString(this.typeDescription));

	}

	@Override
	public void configure(Configuration parameters) {

	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		JobConf jobConf = new JobConf();

		// init and register file system
		String fileName = filePrefixName + numTasks + "-" +
			taskNumber + "-" + System.currentTimeMillis() + ".orc";

		org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(
			new org.apache.flink.core.fs.Path(dir, String.valueOf(taskNumber)), fileName);
		FileSystem fs = path.getFileSystem();
		if (fs instanceof SafetyNetWrapperFileSystem) {
			fs = ((SafetyNetWrapperFileSystem) fs).getWrappedDelegate();
		}

		if (fs instanceof HadoopFileSystem) {
			jobConf.addResource(((HadoopFileSystem) fs).getConfig());
		}

		if (!(fs instanceof LocalFileSystem || fs instanceof HadoopFileSystem)) {
			throw new RuntimeException("FileSystem: " + fs.getClass().getCanonicalName() + " is not supported.");
		}

		// clean up output file in case of failover.
		fs.delete(path, true);

		OrcOutputFormat realOutputFormat = new OrcOutputFormat<OrcStruct>() {
			@Override
			public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
				return new Path(new Path(dir, String.valueOf(taskNumber)), fileName);
			}
		};

		jobConf.set(MAPRED_OUTPUT_SCHEMA.getAttribute(), OrcSchemaConverter.convert(fieldTypes, fieldNames).toString());
		jobConf.set(COMPRESS.getAttribute(), compression.name());
		jobConf.set(ROW_INDEX_STRIDE.getAttribute(), String.valueOf(rowIndexStride));

		TaskAttemptID taskAttemptID = new TaskAttemptID();
		taskContext = ContextUtil.newTaskAttemptContext(jobConf, taskAttemptID);
		realWriter = realOutputFormat.getRecordWriter(taskContext);
	}

	@Override
	public void writeRecord(BaseRow record) throws IOException {
		try {
			realWriter.write(null, serializer.serialize(record, struct));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			if (realWriter != null) {
				realWriter.close(taskContext);
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();

		this.struct.write(out);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		if (this.struct == null) {
			this.struct = new OrcStruct(TypeDescription.fromString(this.typeDescription));
		}
		this.struct.readFields(in);
	}
}
