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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_IGNORE_PARSE_ERRORS;

/**
 * Factory to build reader/writer to read/write json format file.
 */
public class JsonFileSystemFormatFactory implements FileSystemFormatFactory {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(FORMAT, "json");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		ArrayList<String> properties = new ArrayList<>();
		properties.add(FORMAT_FAIL_ON_MISSING_FIELD);
		properties.add(FORMAT_IGNORE_PARSE_ERRORS);
		return properties;
	}

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		DescriptorProperties properties = getValidatedProperties(context.getFormatProperties());
		boolean failOnMissingField = properties.getOptionalBoolean(FORMAT_FAIL_ON_MISSING_FIELD).orElse(false);
		boolean ignoreParseErrors = properties.getOptionalBoolean(FORMAT_IGNORE_PARSE_ERRORS).orElse(false);

		RowType formatRowType = context.getFormatRowType();
		JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
			formatRowType,
			new GenericTypeInfo(GenericRowData.class),
			failOnMissingField,
			ignoreParseErrors);

		String[] fieldNames = context.getSchema().getFieldNames();
		List<String> projectFields = Arrays.stream(context.getProjectFields())
			.mapToObj(idx -> fieldNames[idx])
			.collect(Collectors.toList());
		List<String> jsonFields = Arrays.stream(fieldNames)
			.filter(field -> !context.getPartitionKeys().contains(field))
			.collect(Collectors.toList());

		int[] jsonSelectFieldToProjectFieldMapping = context.getFormatProjectFields().stream()
			.mapToInt(projectFields::indexOf)
			.toArray();
		int[] jsonSelectFieldToJsonFieldMapping = context.getFormatProjectFields().stream()
			.mapToInt(jsonFields::indexOf)
			.toArray();

		return new JsonInputFormat(
			context.getPaths(),
			context.getSchema().getFieldDataTypes(),
			context.getSchema().getFieldNames(),
			context.getProjectFields(),
			context.getPartitionKeys(),
			context.getDefaultPartName(),
			context.getPushedDownLimit(),
			jsonSelectFieldToProjectFieldMapping,
			jsonSelectFieldToJsonFieldMapping,
			deserializationSchema);
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		return Optional.of(new JsonRowDataEncoder(new JsonRowDataSerializationSchema(context.getFormatRowType())));
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		return Optional.empty();
	}

	@Override
	public boolean supportsSchemaDerivation() {
		return true;
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(propertiesMap);
		properties.validateBoolean(FORMAT_FAIL_ON_MISSING_FIELD, true);
		properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
		return properties;
	}

	/**
	 * A {@link JsonInputFormat} is responsible to read {@link RowData} records
	 * from json format files.
	 */
	public static class JsonInputFormat extends DelimitedInputFormat<RowData> {
		/**
		 * Code of \r, used to remove \r from a line when the line ends with \r\n.
		 */
		private static final byte CARRIAGE_RETURN = (byte) '\r';

		/**
		 * Code of \n, used to identify if \n is used as delimiter.
		 */
		private static final byte NEW_LINE = (byte) '\n';

		private final DataType[] fieldTypes;
		private final String[] fieldNames;
		private final int[] selectFields;
		private final List<String> partitionKeys;
		private final String defaultPartValue;
		private final long limit;
		private final int[] jsonSelectFieldToProjectFieldMapping;
		private final int[] jsonSelectFieldToJsonFieldMapping;
		private final JsonRowDataDeserializationSchema deserializationSchema;

		private transient boolean end;
		private transient long emitted;
		// reuse object for per record
		private transient GenericRowData rowData;

		public JsonInputFormat(
			Path[] filePaths,
			DataType[] fieldTypes,
			String[] fieldNames,
			int[] selectFields,
			List<String> partitionKeys,
			String defaultPartValue,
			long limit,
			int[] jsonSelectFieldToProjectFieldMapping,
			int[] jsonSelectFieldToJsonFieldMapping,
			JsonRowDataDeserializationSchema deserializationSchema) {
			super.setFilePaths(filePaths);
			this.fieldTypes = fieldTypes;
			this.fieldNames = fieldNames;
			this.selectFields = selectFields;
			this.partitionKeys = partitionKeys;
			this.defaultPartValue = defaultPartValue;
			this.limit = limit;
			this.jsonSelectFieldToProjectFieldMapping = jsonSelectFieldToProjectFieldMapping;
			this.jsonSelectFieldToJsonFieldMapping = jsonSelectFieldToJsonFieldMapping;
			this.deserializationSchema = deserializationSchema;
		}

		@Override
		public boolean supportsMultiPaths() {
			return true;
		}

		@Override
		public void open(FileInputSplit split) throws IOException {
			super.open(split);
			this.end = false;
			this.emitted = 0L;
			this.rowData = PartitionPathUtils.fillPartitionValueForRecord(fieldNames, fieldTypes, selectFields,
				partitionKeys, currentSplit.getPath(), defaultPartValue);
		}

		@Override
		public boolean reachedEnd() {
			return emitted >= limit || end;
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
			GenericRowData jsonRow = (GenericRowData) deserializationSchema.deserialize(trimBytes);

			if (jsonRow == null) {
				return null;
			}

			GenericRowData returnRecord = rowData;
			for (int i = 0; i < jsonSelectFieldToJsonFieldMapping.length; i++) {
				returnRecord.setField(jsonSelectFieldToProjectFieldMapping[i],
					jsonRow.getField(jsonSelectFieldToJsonFieldMapping[i]));
			}

			emitted++;
			return returnRecord;
		}

		@Override
		public RowData nextRecord(RowData record) throws IOException {
			while (true) {
				if (readLine()) {
					RowData row = readRecord(record, this.currBuffer, this.currOffset, this.currLen);
					if (row == null) {
						continue;
					} else {
						return row;
					}
				} else {
					this.end = true;
					return null;
				}
			}
		}
	}

	/**
	 * A {@link JsonRowDataEncoder} is responsible to encode a {@link RowData} to {@link java.io.OutputStream}
	 * with json format.
	 */
	public static class JsonRowDataEncoder implements Encoder<RowData> {

		private static final long serialVersionUID = 1L;
		private static final String DEFAULT_LINE_DELIMITER = "\n";
		private final JsonRowDataSerializationSchema serializationSchema;

		public JsonRowDataEncoder(JsonRowDataSerializationSchema serializationSchema) {
			this.serializationSchema = serializationSchema;
		}

		@Override
		public void encode(RowData element, OutputStream stream) throws IOException {
			stream.write(serializationSchema.serialize(element));
			stream.write(DEFAULT_LINE_DELIMITER.getBytes(StandardCharsets.UTF_8));
		}
	}
}
