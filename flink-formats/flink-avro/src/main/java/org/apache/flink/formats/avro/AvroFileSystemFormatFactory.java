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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;

/**
 * Avro format factory for file system.
 */
public class AvroFileSystemFormatFactory implements FileSystemFormatFactory {

	public static final String AVRO_OUTPUT_CODEC = "format." + DataFileConstants.CODEC;

	@Override
	public boolean supportsSchemaDerivation() {
		return true;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> options = new ArrayList<>();
		options.add(AVRO_OUTPUT_CODEC);
		return options;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(FORMAT, "avro");
		return context;
	}

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(context.getFormatProperties());

		String[] fieldNames = context.getSchema().getFieldNames();
		List<String> projectFields = Arrays.stream(context.getProjectFields())
			.mapToObj(idx -> fieldNames[idx])
			.collect(Collectors.toList());
		List<String> csvFields = Arrays.stream(fieldNames)
			.filter(field -> !context.getPartitionKeys().contains(field))
			.collect(Collectors.toList());

		int[] selectFieldToProjectField = context.getFormatProjectFields().stream()
			.mapToInt(projectFields::indexOf)
			.toArray();
		int[] selectFieldToFormatField = context.getFormatProjectFields().stream()
			.mapToInt(csvFields::indexOf)
			.toArray();

		//noinspection unchecked
		return new RowDataAvroInputFormat(
				context.getPaths(),
				context.getFormatRowType(),
				context.getSchema().getFieldDataTypes(),
				context.getSchema().getFieldNames(),
				context.getProjectFields(),
				context.getPartitionKeys(),
				context.getDefaultPartName(),
				context.getPushedDownLimit(),
				selectFieldToProjectField,
				selectFieldToFormatField);
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		return Optional.empty();
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		return Optional.of(new RowDataAvroWriterFactory(
				context.getFormatRowType(),
				context.getFormatProperties().get(AVRO_OUTPUT_CODEC)));
	}

	/**
	 * InputFormat that reads avro record into {@link RowData}.
	 *
	 * <p>This extends {@link AvroInputFormat}, but {@link RowData} is not a avro record,
	 * so now we remove generic information.
	 * TODO {@link AvroInputFormat} support type conversion.
	 */
	private static class RowDataAvroInputFormat extends AvroInputFormat {

		private static final long serialVersionUID = 1L;

		private final DataType[] fieldTypes;
		private final String[] fieldNames;
		private final int[] selectFields;
		private final List<String> partitionKeys;
		private final String defaultPartValue;
		private final long limit;
		private final int[] selectFieldToProjectField;
		private final int[] selectFieldToFormatField;
		private final RowType formatRowType;

		private transient long emitted;
		// reuse object for per record
		private transient GenericRowData rowData;
		private transient IndexedRecord record;
		private transient AvroRowDataDeserializationSchema.DeserializationRuntimeConverter converter;

		public RowDataAvroInputFormat(
				Path[] filePaths,
				RowType formatRowType,
				DataType[] fieldTypes,
				String[] fieldNames,
				int[] selectFields,
				List<String> partitionKeys,
				String defaultPartValue,
				long limit,
				int[] selectFieldToProjectField,
				int[] selectFieldToFormatField) {
			super(filePaths[0], GenericRecord.class);
			super.setFilePaths(filePaths);
			this.formatRowType = formatRowType;
			this.fieldTypes = fieldTypes;
			this.fieldNames = fieldNames;
			this.partitionKeys = partitionKeys;
			this.defaultPartValue = defaultPartValue;
			this.selectFields = selectFields;
			this.limit = limit;
			this.emitted = 0;
			this.selectFieldToProjectField = selectFieldToProjectField;
			this.selectFieldToFormatField = selectFieldToFormatField;
		}

		@Override
		public void open(FileInputSplit split) throws IOException {
			super.open(split);
			Schema schema = AvroSchemaConverter.convertToSchema(formatRowType);
			record = new GenericData.Record(schema);
			rowData = PartitionPathUtils.fillPartitionValueForRecord(
					fieldNames,
					fieldTypes,
					selectFields,
					partitionKeys,
					currentSplit.getPath(),
					defaultPartValue);
			this.converter = AvroRowDataDeserializationSchema.createRowConverter(formatRowType);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return emitted >= limit || super.reachedEnd();
		}

		@Override
		public Object nextRecord(Object reuse) throws IOException {
			@SuppressWarnings("unchecked")
			IndexedRecord r = (IndexedRecord) super.nextRecord(record);
			if (r == null) {
				return null;
			}
			GenericRowData row = (GenericRowData) converter.convert(r);

			for (int i = 0; i < selectFieldToFormatField.length; i++) {
				rowData.setField(selectFieldToProjectField[i],
						row.getField(selectFieldToFormatField[i]));
			}
			emitted++;
			return rowData;
		}
	}

	/**
	 * A {@link BulkWriter.Factory} to convert {@link RowData} to {@link GenericRecord} and
	 * wrap {@link AvroWriterFactory}.
	 */
	private static class RowDataAvroWriterFactory implements BulkWriter.Factory<RowData> {

		private static final long serialVersionUID = 1L;

		private final AvroWriterFactory<GenericRecord> factory;
		private final RowType rowType;

		private RowDataAvroWriterFactory(RowType rowType, String codec) {
			this.rowType = rowType;
			this.factory = new AvroWriterFactory<>((AvroBuilder<GenericRecord>) out -> {
				Schema schema = AvroSchemaConverter.convertToSchema(rowType);
				DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
				DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

				if (codec != null) {
					dataFileWriter.setCodec(CodecFactory.fromString(codec));
				}
				dataFileWriter.create(schema, out);
				return dataFileWriter;
			});
		}

		@Override
		public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
			BulkWriter<GenericRecord> writer = factory.create(out);
			AvroRowDataSerializationSchema.SerializationRuntimeConverter converter =
					AvroRowDataSerializationSchema.createRowConverter(rowType);
			return new BulkWriter<RowData>() {

				@Override
				public void addElement(RowData element) throws IOException {
					GenericRecord record = (GenericRecord) converter.convert(element);
					writer.addElement(record);
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
