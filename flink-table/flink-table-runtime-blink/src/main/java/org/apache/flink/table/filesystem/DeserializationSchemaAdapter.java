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

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.ArrayResultIterator;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.UserCodeClassLoader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.flink.connector.file.src.util.CheckpointedPosition.NO_OFFSET;
import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;

/**
 * Adapter to turn a {@link DeserializationSchema} into a {@link BulkFormat}.
 */
public class DeserializationSchemaAdapter implements BulkFormat<RowData, FileSourceSplit> {

	private static final int BATCH_SIZE = 100;

	// NOTE, deserializationSchema produce full format fields with original order
	private final DeserializationSchema<RowData> deserializationSchema;

	private final String[] fieldNames;
	private final DataType[] fieldTypes;
	private final int[] projectFields;
	private final RowType projectedRowType;

	private final List<String> partitionKeys;
	private final String defaultPartValue;

	private final int[] toProjectedField;
	private final RowData.FieldGetter[] formatFieldGetters;

	public DeserializationSchemaAdapter(
			DeserializationSchema<RowData> deserializationSchema,
			TableSchema schema,
			int[] projectFields,
			List<String> partitionKeys,
			String defaultPartValue) {
		this.deserializationSchema = deserializationSchema;
		this.fieldNames = schema.getFieldNames();
		this.fieldTypes = schema.getFieldDataTypes();
		this.projectFields = projectFields;
		this.partitionKeys = partitionKeys;
		this.defaultPartValue = defaultPartValue;

		List<String> projectedNames = Arrays.stream(projectFields)
				.mapToObj(idx -> schema.getFieldNames()[idx])
				.collect(Collectors.toList());

		this.projectedRowType = RowType.of(
				Arrays.stream(projectFields).mapToObj(idx ->
						schema.getFieldDataTypes()[idx].getLogicalType()).toArray(LogicalType[]::new),
				projectedNames.toArray(new String[0]));

		List<String> formatFields = Arrays.stream(schema.getFieldNames())
				.filter(field -> !partitionKeys.contains(field))
				.collect(Collectors.toList());

		List<String> formatProjectedFields = projectedNames.stream()
				.filter(field -> !partitionKeys.contains(field))
				.collect(Collectors.toList());

		this.toProjectedField = formatProjectedFields.stream()
				.mapToInt(projectedNames::indexOf)
				.toArray();

		this.formatFieldGetters = new RowData.FieldGetter[formatProjectedFields.size()];
		for (int i = 0; i < formatProjectedFields.size(); i++) {
			String name = formatProjectedFields.get(i);
			this.formatFieldGetters[i] = RowData.createFieldGetter(
					schema.getFieldDataType(name).get().getLogicalType(),
					formatFields.indexOf(name));
		}
	}

	private DeserializationSchema<RowData> createDeserialization() throws IOException {
		try {
			DeserializationSchema<RowData> deserialization = InstantiationUtil.clone(deserializationSchema);
			deserialization.open(new DeserializationSchema.InitializationContext() {
				@Override
				public MetricGroup getMetricGroup() {
					throw new UnsupportedOperationException("MetricGroup is unsupported in BulkFormat.");
				}

				@Override
				public UserCodeClassLoader getUserCodeClassLoader() {
					return (UserCodeClassLoader) Thread.currentThread().getContextClassLoader();
				}
			});
			return deserialization;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public Reader createReader(Configuration config, FileSourceSplit split) throws IOException {
		return new Reader(config, split);
	}

	@Override
	public Reader restoreReader(Configuration config, FileSourceSplit split) throws IOException {
		Reader reader = new Reader(config, split);
		reader.seek(split.getReaderPosition().get().getRecordsAfterOffset());
		return reader;
	}

	@Override
	public boolean isSplittable() {
		return true;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return InternalTypeInfo.of(projectedRowType);
	}

	private class Reader implements BulkFormat.Reader<RowData> {

		private final LineBytesInputFormat inputFormat;
		private long numRead = 0;

		private Reader(Configuration config, FileSourceSplit split) throws IOException {
			this.inputFormat = new LineBytesInputFormat(split.path(), config);
			this.inputFormat.open(new FileInputSplit(0, split.path(), split.offset(), split.length(), null));
		}

		@SuppressWarnings({"unchecked", "rawtypes"})
		@Nullable
		@Override
		public RecordIterator<RowData> readBatch() throws IOException {
			RowData[] records = new RowData[DEFAULT_SIZE];
			int num = 0;
			final long skipCount = numRead;
			for (int i = 0; i < BATCH_SIZE; i++) {
				RowData record = inputFormat.nextRecord(null);
				if (record == null) {
					break;
				}
				records[num++] = record;
			}
			if (num == 0) {
				return null;
			}
			numRead += num;

			ArrayResultIterator<RowData> iterator = new ArrayResultIterator<>();
			iterator.set(records, num, NO_OFFSET, skipCount);
			return iterator;
		}

		private void seek(long toSkip) throws IOException {
			while (toSkip > 0) {
				inputFormat.nextRecord(null);
				toSkip--;
			}
		}

		@Override
		public void close() throws IOException {
			inputFormat.close();
		}
	}

	private class LineBytesInputFormat extends DelimitedInputFormat<RowData> {

		private static final long serialVersionUID = 1L;

		/**
		 * Code of \r, used to remove \r from a line when the line ends with \r\n.
		 */
		private static final byte CARRIAGE_RETURN = (byte) '\r';

		/**
		 * Code of \n, used to identify if \n is used as delimiter.
		 */
		private static final byte NEW_LINE = (byte) '\n';

		private final DeserializationSchema<RowData> deserializationSchema;

		private transient boolean end;
		private transient RecordCollector collector;
		private transient GenericRowData rowData;

		public LineBytesInputFormat(Path path, Configuration config) throws IOException {
			super(path, config);
			this.deserializationSchema = createDeserialization();
		}

		@Override
		public void open(FileInputSplit split) throws IOException {
			super.open(split);
			this.end = false;
			this.collector = new RecordCollector();
			this.rowData = PartitionPathUtils.fillPartitionValueForRecord(
					fieldNames,
					fieldTypes,
					projectFields,
					partitionKeys,
					currentSplit.getPath(),
					defaultPartValue);
		}

		private GenericRowData newOutputRow() {
			GenericRowData row = new GenericRowData(rowData.getArity());
			for (int i = 0; i < row.getArity(); i++) {
				row.setField(i, rowData.getField(i));
			}
			return row;
		}

		@Override
		public boolean reachedEnd() {
			return end;
		}

		@Override
		public RowData readRecord(RowData reuse, byte[] bytes, int offset, int numBytes) throws IOException {
			// remove \r from a line when the line ends with \r\n
			if (this.getDelimiter() != null && this.getDelimiter().length == 1
					&& this.getDelimiter()[0] == NEW_LINE && offset + numBytes >= 1
					&& bytes[offset + numBytes - 1] == CARRIAGE_RETURN) {
				numBytes -= 1;
			}
			byte[] trimBytes = Arrays.copyOfRange(bytes, offset, offset + numBytes);
			deserializationSchema.deserialize(trimBytes, collector);
			return null;
		}

		private RowData convert(RowData record) {
			GenericRowData outputRow = newOutputRow();

			for (int i = 0; i < toProjectedField.length; i++) {
				outputRow.setField(
						toProjectedField[i],
						formatFieldGetters[i].getFieldOrNull(record));
			}

			outputRow.setRowKind(record.getRowKind());
			return outputRow;
		}

		@Override
		public RowData nextRecord(RowData reuse) throws IOException {
			while (true) {
				RowData record = collector.records.poll();
				if (record != null) {
					return convert(record);
				}

				if (readLine()) {
					readRecord(reuse, this.currBuffer, this.currOffset, this.currLen);
				} else {
					this.end = true;
					return null;
				}
			}
		}

		private class RecordCollector implements Collector<RowData> {

			private final Queue<RowData> records = new ArrayDeque<>();

			@Override
			public void collect(RowData record) {
				records.add(record);
			}

			@Override
			public void close() {
			}
		}
	}
}
