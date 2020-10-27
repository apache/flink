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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BucketsBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;
import org.apache.flink.table.filesystem.stream.StreamingSink;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_FILE_SIZE;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL;

/**
 * File system {@link DynamicTableSink}.
 */
public class FileSystemTableSink extends AbstractFileSystemTable implements
		DynamicTableSink,
		SupportsPartitioning,
		SupportsOverwrite {

	private boolean overwrite = false;
	private boolean dynamicGrouping = false;
	private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();

	FileSystemTableSink(DynamicTableFactory.Context context) {
		super(context);
	}

	private FileSystemTableSink(
			DynamicTableFactory.Context context,
			boolean overwrite,
			boolean dynamicGrouping,
			LinkedHashMap<String, String> staticPartitions) {
		this(context);
		this.overwrite = overwrite;
		this.dynamicGrouping = dynamicGrouping;
		this.staticPartitions = staticPartitions;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		return (DataStreamSinkProvider) dataStream -> consume(dataStream, context.isBounded());
	}

	private DataStreamSink<?> consume(DataStream<RowData> dataStream, boolean isBounded) {
		RowDataPartitionComputer computer = new RowDataPartitionComputer(
				defaultPartName,
				schema.getFieldNames(),
				schema.getFieldDataTypes(),
				partitionKeys.toArray(new String[0]));

		EmptyMetaStoreFactory metaStoreFactory = new EmptyMetaStoreFactory(path);
		OutputFileConfig outputFileConfig = OutputFileConfig.builder()
				.withPartPrefix("part-" + UUID.randomUUID().toString())
				.build();
		FileSystemFactory fsFactory = FileSystem::get;

		if (isBounded) {
			FileSystemOutputFormat.Builder<RowData> builder = new FileSystemOutputFormat.Builder<>();
			builder.setPartitionComputer(computer);
			builder.setDynamicGrouped(dynamicGrouping);
			builder.setPartitionColumns(partitionKeys.toArray(new String[0]));
			builder.setFormatFactory(createOutputFormatFactory());
			builder.setMetaStoreFactory(metaStoreFactory);
			builder.setFileSystemFactory(fsFactory);
			builder.setOverwrite(overwrite);
			builder.setStaticPartitions(staticPartitions);
			builder.setTempPath(toStagingPath());
			builder.setOutputFileConfig(outputFileConfig);
			return dataStream.writeUsingOutputFormat(builder.build())
					.setParallelism(dataStream.getParallelism());
		} else {
			Object writer = createWriter();
			TableBucketAssigner assigner = new TableBucketAssigner(computer);
			TableRollingPolicy rollingPolicy = new TableRollingPolicy(
					!(writer instanceof Encoder),
					tableOptions.get(SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
					tableOptions.get(SINK_ROLLING_POLICY_ROLLOVER_INTERVAL).toMillis());

			BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> bucketsBuilder;
			if (writer instanceof Encoder) {
				//noinspection unchecked
				bucketsBuilder = StreamingFileSink.forRowFormat(
						path, new ProjectionEncoder((Encoder<RowData>) writer, computer))
						.withBucketAssigner(assigner)
						.withOutputFileConfig(outputFileConfig)
						.withRollingPolicy(rollingPolicy);
			} else {
				//noinspection unchecked
				bucketsBuilder = StreamingFileSink.forBulkFormat(
						path, new ProjectionBulkFactory((BulkWriter.Factory<RowData>) writer, computer))
						.withBucketAssigner(assigner)
						.withOutputFileConfig(outputFileConfig)
						.withRollingPolicy(rollingPolicy);
			}
			return createStreamingSink(
					tableOptions,
					path,
					partitionKeys,
					tableIdentifier,
					overwrite,
					dataStream,
					bucketsBuilder,
					metaStoreFactory,
					fsFactory,
					tableOptions.get(SINK_ROLLING_POLICY_CHECK_INTERVAL).toMillis());
		}
	}

	public static DataStreamSink<?> createStreamingSink(
			Configuration conf,
			Path path,
			List<String> partitionKeys,
			ObjectIdentifier tableIdentifier,
			boolean overwrite,
			DataStream<RowData> inputStream,
			BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> bucketsBuilder,
			TableMetaStoreFactory msFactory,
			FileSystemFactory fsFactory,
			long rollingCheckInterval) {
		if (overwrite) {
			throw new IllegalStateException("Streaming mode not support overwrite.");
		}

		DataStream<PartitionCommitInfo> writer = StreamingSink.writer(
				inputStream, rollingCheckInterval, bucketsBuilder);

		return StreamingSink.sink(
				writer, path, tableIdentifier, partitionKeys, msFactory, fsFactory, conf);
	}

	private Path toStagingPath() {
		Path stagingDir = new Path(path, ".staging_" + System.currentTimeMillis());
		try {
			FileSystem fs = stagingDir.getFileSystem();
			Preconditions.checkState(
					fs.exists(stagingDir) || fs.mkdirs(stagingDir),
					"Failed to create staging dir " + stagingDir);
			return stagingDir;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private OutputFormatFactory<RowData> createOutputFormatFactory() {
		Object writer = createWriter();
		return writer instanceof Encoder ?
				path -> createEncoderOutputFormat((Encoder<RowData>) writer, path) :
				path -> createBulkWriterOutputFormat((BulkWriter.Factory<RowData>) writer, path);
	}

	private Object createWriter() {
		FileSystemFormatFactory formatFactory = createFormatFactory(tableOptions);

		FileSystemFormatFactory.WriterContext context = new FileSystemFormatFactory.WriterContext() {

			@Override
			public TableSchema getSchema() {
				return schema;
			}

			@Override
			public ReadableConfig getFormatOptions() {
				return new DelegatingConfiguration(tableOptions, formatFactory.factoryIdentifier() + ".");
			}

			@Override
			public List<String> getPartitionKeys() {
				return partitionKeys;
			}
		};

		Optional<Encoder<RowData>> encoder = formatFactory.createEncoder(context);
		Optional<BulkWriter.Factory<RowData>> bulk = formatFactory.createBulkWriterFactory(context);

		if (encoder.isPresent()) {
			return encoder.get();
		} else if (bulk.isPresent()) {
			return bulk.get();
		} else {
			throw new TableException(
					formatFactory + " format should implement at least one Encoder or BulkWriter");
		}
	}

	private static OutputFormat<RowData> createBulkWriterOutputFormat(
			BulkWriter.Factory<RowData> factory,
			Path path) {
		return new OutputFormat<RowData>() {

			private static final long serialVersionUID = 1L;

			private transient BulkWriter<RowData> writer;

			@Override
			public void configure(Configuration parameters) {
			}

			@Override
			public void open(int taskNumber, int numTasks) throws IOException {
				this.writer = factory.create(path.getFileSystem()
						.create(path, FileSystem.WriteMode.OVERWRITE));
			}

			@Override
			public void writeRecord(RowData record) throws IOException {
				writer.addElement(record);
			}

			@Override
			public void close() throws IOException {
				writer.flush();
				writer.finish();
			}
		};
	}

	private static OutputFormat<RowData> createEncoderOutputFormat(
			Encoder<RowData> encoder,
			Path path) {
		return new OutputFormat<RowData>() {

			private static final long serialVersionUID = 1L;

			private transient FSDataOutputStream output;

			@Override
			public void configure(Configuration parameters) {
			}

			@Override
			public void open(int taskNumber, int numTasks) throws IOException {
				this.output = path.getFileSystem()
						.create(path, FileSystem.WriteMode.OVERWRITE);
			}

			@Override
			public void writeRecord(RowData record) throws IOException {
				encoder.encode(record, output);
			}

			@Override
			public void close() throws IOException {
				this.output.flush();
				this.output.close();
			}
		};
	}

	private LinkedHashMap<String, String> toPartialLinkedPartSpec(Map<String, String> part) {
		LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
		for (String partitionKey : partitionKeys) {
			if (part.containsKey(partitionKey)) {
				partSpec.put(partitionKey, part.get(partitionKey));
			}
		}
		return partSpec;
	}

	@Override
	public boolean requiresPartitionGrouping(boolean supportsGrouping) {
		this.dynamicGrouping = supportsGrouping;
		return dynamicGrouping;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return ChangelogMode.insertOnly();
	}

	@Override
	public DynamicTableSink copy() {
		return new FileSystemTableSink(context, overwrite, dynamicGrouping, staticPartitions);
	}

	@Override
	public String asSummaryString() {
		return "Filesystem";
	}

	@Override
	public void applyOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	@Override
	public void applyStaticPartition(Map<String, String> partition) {
		this.staticPartitions = toPartialLinkedPartSpec(partition);
	}

	/**
	 * Table bucket assigner, wrap {@link PartitionComputer}.
	 */
	public static class TableBucketAssigner implements BucketAssigner<RowData, String> {

		private final PartitionComputer<RowData> computer;

		public TableBucketAssigner(PartitionComputer<RowData> computer) {
			this.computer = computer;
		}

		@Override
		public String getBucketId(RowData element, Context context) {
			try {
				return PartitionPathUtils.generatePartitionPath(
						computer.generatePartValues(element));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	/**
	 * Table {@link RollingPolicy}, it extends {@link CheckpointRollingPolicy} for bulk writers.
	 */
	public static class TableRollingPolicy extends CheckpointRollingPolicy<RowData, String> {

		private final boolean rollOnCheckpoint;
		private final long rollingFileSize;
		private final long rollingTimeInterval;

		public TableRollingPolicy(
				boolean rollOnCheckpoint,
				long rollingFileSize,
				long rollingTimeInterval) {
			this.rollOnCheckpoint = rollOnCheckpoint;
			Preconditions.checkArgument(rollingFileSize > 0L);
			Preconditions.checkArgument(rollingTimeInterval > 0L);
			this.rollingFileSize = rollingFileSize;
			this.rollingTimeInterval = rollingTimeInterval;
		}

		@Override
		public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
			try {
				return rollOnCheckpoint || partFileState.getSize() > rollingFileSize;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean shouldRollOnEvent(
				PartFileInfo<String> partFileState,
				RowData element) throws IOException {
			return partFileState.getSize() > rollingFileSize;
		}

		@Override
		public boolean shouldRollOnProcessingTime(
				PartFileInfo<String> partFileState,
				long currentTime) {
			return currentTime - partFileState.getCreationTime() >= rollingTimeInterval;
		}
	}

	private static class ProjectionEncoder implements Encoder<RowData> {

		private final Encoder<RowData> encoder;
		private final RowDataPartitionComputer computer;

		private ProjectionEncoder(Encoder<RowData> encoder, RowDataPartitionComputer computer) {
			this.encoder = encoder;
			this.computer = computer;
		}

		@Override
		public void encode(RowData element, OutputStream stream) throws IOException {
			encoder.encode(computer.projectColumnsToWrite(element), stream);
		}
	}

	/**
	 * Project row to non-partition fields.
	 */
	public static class ProjectionBulkFactory implements BulkWriter.Factory<RowData> {

		private final BulkWriter.Factory<RowData> factory;
		private final RowDataPartitionComputer computer;

		public ProjectionBulkFactory(BulkWriter.Factory<RowData> factory, RowDataPartitionComputer computer) {
			this.factory = factory;
			this.computer = computer;
		}

		@Override
		public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
			BulkWriter<RowData> writer = factory.create(out);
			return new BulkWriter<RowData>() {

				@Override
				public void addElement(RowData element) throws IOException {
					writer.addElement(computer.projectColumnsToWrite(element));
				}

				@Override
				public void flush() throws IOException {
					writer.flush();
				}

				@Override
				public void finish() throws IOException {
					writer.finish();
				}
			};
		}
	}
}
