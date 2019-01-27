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

package org.apache.flink.table.sources.orc;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.SafetyNetWrapperFileSystem;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.sources.parquet.RecordReaderIterator;
import org.apache.flink.util.Preconditions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The base InputFormat class to read from Orc files.
 * For specific input types the {@link #convert(Object)} method need to be implemented.
 */
public abstract class OrcInputFormat<T, R> extends FileInputFormat<T> {

	private static final long serialVersionUID = -603475013213133955L;

	protected final InternalType[] fieldTypes;

	protected final String[] fieldNames;

	private byte[] filterBytes;

	private boolean isCaseSensitive = true;

	protected String[] schemaFieldNames;

	protected int[] columnIds;

	protected transient RecordReaderIterator<R> readerIterator;

	protected OrcInputFormat(
			Path filePath,
			InternalType[] fieldTypes,
			String[] fieldNames) {
		super(filePath);

		Preconditions.checkArgument(fieldTypes != null && fieldTypes.length > 0);
		Preconditions.checkArgument(fieldNames != null && fieldNames.length == fieldTypes.length);

		this.fieldTypes = fieldTypes;
		this.fieldNames = fieldNames;
	}

	public void setFilterPredicate(SearchArgument sarg) throws Exception {
		if (sarg != null) {
			Output out = new Output(100000);
			new Kryo().writeObject(out, sarg);
			filterBytes = out.toBytes();
		} else {
			filterBytes = null;
		}
	}

	public void setSchemaFields(String[] schemaFieldNames) {
		this.schemaFieldNames = schemaFieldNames;
	}

	private SearchArgument getFilterPredicate() {
		if (filterBytes != null) {
			try {
				return new Kryo().readObject(new Input(filterBytes), SearchArgumentImpl.class);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			return null;
		}
	}

	@Override
	public void open(FileInputSplit fileSplit) throws IOException {
		// init and register file system
		Configuration hadoopConf = new Configuration();
		FileSystem fs = fileSplit.getPath().getFileSystem();
		if (fs instanceof SafetyNetWrapperFileSystem) {
			fs = ((SafetyNetWrapperFileSystem) fs).getWrappedDelegate();
		}

		if (fs instanceof HadoopFileSystem) {
			hadoopConf.addResource(((HadoopFileSystem) fs).getConfig());
		}

		if (!(fs instanceof LocalFileSystem || fs instanceof HadoopFileSystem)) {
			throw new RuntimeException("FileSystem: " + fs.getClass().getCanonicalName() + " is not supported.");
		}

		SearchArgument filter = getFilterPredicate();
		if (null != filter) {
			org.apache.orc.mapreduce.OrcInputFormat.setSearchArgument(hadoopConf, filter, fieldNames);
		}

		org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(fileSplit.getPath().toUri());
		Reader reader = OrcFile.createReader(filePath,
			OrcFile.readerOptions(hadoopConf).maxLength(OrcConf.MAX_FILE_LENGTH.getLong(hadoopConf)));

		columnIds = OrcUtils.requestedColumnIds(isCaseSensitive, fieldNames, schemaFieldNames, reader);

		if (columnIds != null) {
			String includeColumns =
				Arrays.stream(columnIds).filter(column -> column != -1)
					.mapToObj(String::valueOf).collect(Collectors.joining(","));
			hadoopConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute(), includeColumns);
		} else {
			// TODO: bug? should we return an empty iterator?
			hadoopConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute(), "");
		}

		TaskAttemptID attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0);
		TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(hadoopConf, attemptId);

		RecordReader recordReader = createReader(fileSplit, taskAttemptContext);

		InputSplit split = new FileSplit(
				new org.apache.hadoop.fs.Path(fileSplit.getPath().toUri()),
				fileSplit.getStart(),
				fileSplit.getLength(),
				fileSplit.getHostnames());

		try {
			recordReader.initialize(split, taskAttemptContext);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		readerIterator = new RecordReaderIterator<>(recordReader);
	}

	@Override
	public void close() throws IOException {
		if (null != readerIterator) {
			readerIterator.close();
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !readerIterator.hasNext();
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		R next = readerIterator.next();
		return convert(next);
	}

	/**
	 * convert the Orc row to specific type T.
	 * NOTES: `current` is reused to avoid creating row instance for each record,
	 * so the implementation of this method should copy the values of `current` to `reuse` instead of return
	 * `current` directly.
	 */
	protected abstract T convert(R current);

	protected RecordReader createReader(
			FileInputSplit fileSplit, TaskAttemptContext taskAttemptContext) throws IOException {
		// by default, we use org.apache.orc.mapreduce.OrcMapreduceRecordReader
		Configuration hadoopConf = taskAttemptContext.getConfiguration();
		org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(fileSplit.getPath().toUri());
		Reader file = OrcFile.createReader(filePath,
			OrcFile.readerOptions(hadoopConf).maxLength(OrcConf.MAX_FILE_LENGTH.getLong(hadoopConf)));
		return new OrcMapreduceRecordReader<>(file,
			org.apache.orc.mapred.OrcInputFormat.buildOptions(hadoopConf,
				file, fileSplit.getStart(), fileSplit.getLength()));
	}
}
