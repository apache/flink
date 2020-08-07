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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.api.TableException;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * File system {@link OutputFormat} for batch job. It commit in {@link #finalizeGlobal(int)}.
 *
 * @param <T> The type of the consumed records.
 */
@Internal
public class FileSystemOutputFormat<T> implements OutputFormat<T>, FinalizeOnMaster, Serializable {

	private static final long serialVersionUID = 1L;

	private static final long CHECKPOINT_ID = 0;

	private final FileSystemFactory fsFactory;
	private final TableMetaStoreFactory msFactory;
	private final boolean overwrite;
	private final Path tmpPath;
	private final String[] partitionColumns;
	private final boolean dynamicGrouped;
	private final LinkedHashMap<String, String> staticPartitions;
	private final PartitionComputer<T> computer;
	private final OutputFormatFactory<T> formatFactory;
	private final OutputFileConfig outputFileConfig;

	private transient PartitionWriter<T> writer;
	private transient Configuration parameters;

	private FileSystemOutputFormat(
			FileSystemFactory fsFactory,
			TableMetaStoreFactory msFactory,
			boolean overwrite,
			Path tmpPath,
			String[] partitionColumns,
			boolean dynamicGrouped,
			LinkedHashMap<String, String> staticPartitions,
			OutputFormatFactory<T> formatFactory,
			PartitionComputer<T> computer,
			OutputFileConfig outputFileConfig) {
		this.fsFactory = fsFactory;
		this.msFactory = msFactory;
		this.overwrite = overwrite;
		this.tmpPath = tmpPath;
		this.partitionColumns = partitionColumns;
		this.dynamicGrouped = dynamicGrouped;
		this.staticPartitions = staticPartitions;
		this.formatFactory = formatFactory;
		this.computer = computer;
		this.outputFileConfig = outputFileConfig;
	}

	@Override
	public void finalizeGlobal(int parallelism) {
		try {
			FileSystemCommitter committer = new FileSystemCommitter(
					fsFactory,
					msFactory,
					overwrite,
					tmpPath,
					partitionColumns.length);
			committer.commitUpToCheckpoint(CHECKPOINT_ID);
		} catch (Exception e) {
			throw new TableException("Exception in finalizeGlobal", e);
		} finally {
			new File(tmpPath.getPath()).delete();
		}
	}

	@Override
	public void configure(Configuration parameters) {
		this.parameters = parameters;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			PartitionTempFileManager fileManager = new PartitionTempFileManager(
					fsFactory, tmpPath, taskNumber, CHECKPOINT_ID, outputFileConfig);
			PartitionWriter.Context<T> context = new PartitionWriter.Context<>(
					parameters, formatFactory);
			writer = PartitionWriterFactory.<T>get(
					partitionColumns.length - staticPartitions.size() > 0,
					dynamicGrouped,
					staticPartitions).create(context, fileManager, computer);
		} catch (Exception e) {
			throw new TableException("Exception in open", e);
		}
	}

	@Override
	public void writeRecord(T record) {
		try {
			writer.write(record);
		} catch (Exception e) {
			throw new TableException("Exception in writeRecord", e);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			writer.close();
		} catch (Exception e) {
			throw new TableException("Exception in close", e);
		}
	}

	/**
	 * Builder to build {@link FileSystemOutputFormat}.
	 */
	public static class Builder<T> {

		private String[] partitionColumns;
		private OutputFormatFactory<T> formatFactory;
		private TableMetaStoreFactory metaStoreFactory;
		private Path tmpPath;

		private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
		private boolean dynamicGrouped = false;
		private boolean overwrite = false;
		private FileSystemFactory fileSystemFactory = FileSystem::get;

		private PartitionComputer<T> computer;

		private OutputFileConfig outputFileConfig = new OutputFileConfig("", "");

		public Builder<T> setPartitionColumns(String[] partitionColumns) {
			this.partitionColumns = partitionColumns;
			return this;
		}

		public Builder<T> setStaticPartitions(LinkedHashMap<String, String> staticPartitions) {
			this.staticPartitions = staticPartitions;
			return this;
		}

		public Builder<T> setDynamicGrouped(boolean dynamicGrouped) {
			this.dynamicGrouped = dynamicGrouped;
			return this;
		}

		public Builder<T> setFormatFactory(OutputFormatFactory<T> formatFactory) {
			this.formatFactory = formatFactory;
			return this;
		}

		public Builder<T> setFileSystemFactory(FileSystemFactory fileSystemFactory) {
			this.fileSystemFactory = fileSystemFactory;
			return this;
		}

		public Builder<T> setMetaStoreFactory(TableMetaStoreFactory metaStoreFactory) {
			this.metaStoreFactory = metaStoreFactory;
			return this;
		}

		public Builder<T> setOverwrite(boolean overwrite) {
			this.overwrite = overwrite;
			return this;
		}

		public Builder<T> setTempPath(Path tmpPath) {
			this.tmpPath = tmpPath;
			return this;
		}

		public Builder<T> setPartitionComputer(PartitionComputer<T> computer) {
			this.computer = computer;
			return this;
		}

		public Builder<T> setOutputFileConfig(OutputFileConfig outputFileConfig) {
			this.outputFileConfig = outputFileConfig;
			return this;
		}

		public FileSystemOutputFormat<T> build() {
			checkNotNull(partitionColumns, "partitionColumns should not be null");
			checkNotNull(formatFactory, "formatFactory should not be null");
			checkNotNull(metaStoreFactory, "metaStoreFactory should not be null");
			checkNotNull(tmpPath, "tmpPath should not be null");
			checkNotNull(computer, "partitionComputer should not be null");

			return new FileSystemOutputFormat<>(
					fileSystemFactory,
					metaStoreFactory,
					overwrite,
					tmpPath,
					partitionColumns,
					dynamicGrouped,
					staticPartitions,
					formatFactory,
					computer,
					outputFileConfig);
		}
	}
}
