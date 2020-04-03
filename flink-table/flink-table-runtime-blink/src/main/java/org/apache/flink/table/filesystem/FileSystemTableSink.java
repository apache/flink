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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.filesystem.FileSystemTableFactory.createFormatFactory;

/**
 * File system {@link TableSink}.
 */
public class FileSystemTableSink implements
		AppendStreamTableSink<BaseRow>,
		PartitionableTableSink,
		OverwritableTableSink {

	private final TableSchema schema;
	private final List<String> partitionKeys;
	private final Path path;
	private final String defaultPartName;
	private final Map<String, String> formatProperties;

	private boolean overwrite = false;
	private boolean dynamicGrouping = false;
	private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();

	/**
	 * Construct a file system table sink.
	 *
	 * @param schema schema of the table.
	 * @param path directory path of the file system table.
	 * @param partitionKeys partition keys of the table.
	 * @param defaultPartName The default partition name in case the dynamic partition column value
	 *                        is null/empty string.
	 * @param formatProperties format properties.
	 */
	public FileSystemTableSink(
			TableSchema schema,
			Path path,
			List<String> partitionKeys,
			String defaultPartName,
			Map<String, String> formatProperties) {
		this.schema = schema;
		this.path = path;
		this.defaultPartName = defaultPartName;
		this.formatProperties = formatProperties;
		this.partitionKeys = partitionKeys;
	}

	@Override
	public final DataStreamSink<BaseRow> consumeDataStream(DataStream<BaseRow> dataStream) {
		RowDataPartitionComputer computer = new RowDataPartitionComputer(
				defaultPartName,
				schema.getFieldNames(),
				schema.getFieldDataTypes(),
				partitionKeys.toArray(new String[0]));

		FileSystemOutputFormat.Builder<BaseRow> builder = new FileSystemOutputFormat.Builder<>();
		builder.setPartitionComputer(computer);
		builder.setDynamicGrouped(dynamicGrouping);
		builder.setPartitionColumns(partitionKeys.toArray(new String[0]));
		builder.setFormatFactory(createOutputFormatFactory());
		builder.setMetaStoreFactory(createTableMetaStoreFactory(path));
		builder.setOverwrite(overwrite);
		builder.setStaticPartitions(staticPartitions);
		builder.setTempPath(toStagingPath());
		return dataStream.writeUsingOutputFormat(builder.build())
				.setParallelism(dataStream.getParallelism());
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

	private OutputFormatFactory<BaseRow> createOutputFormatFactory() {
		FileSystemFormatFactory formatFactory = createFormatFactory(formatProperties);
		FileSystemFormatFactory.WriterContext context = new FileSystemFormatFactory.WriterContext() {

			@Override
			public TableSchema getSchema() {
				return schema;
			}

			@Override
			public Map<String, String> getFormatProperties() {
				return formatProperties;
			}

			@Override
			public List<String> getPartitionKeys() {
				return partitionKeys;
			}
		};

		Optional<Encoder<BaseRow>> encoder = formatFactory.createEncoder(context);
		Optional<BulkWriter.Factory<BaseRow>> bulk = formatFactory.createBulkWriterFactory(context);

		if (!encoder.isPresent() && !bulk.isPresent()) {
			throw new TableException(
					formatFactory + " format should implement at least one Encoder or BulkWriter");
		}
		return encoder
				.<OutputFormatFactory<BaseRow>>map(en -> path -> createEncoderOutputFormat(en, path))
				.orElseGet(() -> {
					// Optional is not serializable.
					BulkWriter.Factory<BaseRow> bulkWriterFactory = bulk.get();
					return path -> createBulkWriterOutputFormat(bulkWriterFactory, path);
				});
	}

	private static OutputFormat<BaseRow> createBulkWriterOutputFormat(
			BulkWriter.Factory<BaseRow> factory,
			Path path) {
		return new OutputFormat<BaseRow>() {

			private static final long serialVersionUID = 1L;

			private transient BulkWriter<BaseRow> writer;

			@Override
			public void configure(Configuration parameters) {
			}

			@Override
			public void open(int taskNumber, int numTasks) throws IOException {
				this.writer = factory.create(path.getFileSystem()
						.create(path, FileSystem.WriteMode.OVERWRITE));
			}

			@Override
			public void writeRecord(BaseRow record) throws IOException {
				writer.addElement(record);
			}

			@Override
			public void close() throws IOException {
				writer.flush();
				writer.finish();
			}
		};
	}

	private static OutputFormat<BaseRow> createEncoderOutputFormat(
			Encoder<BaseRow> encoder,
			Path path) {
		return new OutputFormat<BaseRow>() {

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
			public void writeRecord(BaseRow record) throws IOException {
				encoder.encode(record, output);
			}

			@Override
			public void close() throws IOException {
				this.output.flush();
				this.output.close();
			}
		};
	}

	private static TableMetaStoreFactory createTableMetaStoreFactory(Path path) {
		return (TableMetaStoreFactory) () -> new TableMetaStoreFactory.TableMetaStore() {

			@Override
			public Path getLocationPath() {
				return path;
			}

			@Override
			public Optional<Path> getPartition(LinkedHashMap<String, String> partitionSpec) {
				return Optional.empty();
			}

			@Override
			public void createPartition(
					LinkedHashMap<String, String> partitionSpec,
					Path partitionPath) {
			}

			@Override
			public void close() {
			}
		};
	}

	@Override
	public FileSystemTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return this;
	}

	@Override
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	@Override
	public void setStaticPartition(Map<String, String> partitions) {
		this.staticPartitions = toPartialLinkedPartSpec(partitions);
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
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public DataType getConsumedDataType() {
		return schema.toRowDataType().bridgedTo(BaseRow.class);
	}

	@Override
	public boolean configurePartitionGrouping(boolean supportsGrouping) {
		this.dynamicGrouping = supportsGrouping;
		return dynamicGrouping;
	}
}
