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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvRowDataDeserializationSchema.DeserializationRuntimeConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_ALLOW_COMMENTS;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_DISABLE_QUOTE_CHARACTER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_ESCAPE_CHARACTER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_FIELD_DELIMITER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_IGNORE_PARSE_ERRORS;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_LINE_DELIMITER;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_NULL_LITERAL;
import static org.apache.flink.table.descriptors.CsvValidator.FORMAT_QUOTE_CHARACTER;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;

/**
 * CSV format factory for file system.
 */
public class CsvFileSystemFormatFactory implements FileSystemFormatFactory {

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		DescriptorProperties properties = getValidatedProperties(context.getFormatProperties());

		RowType formatRowType = context.getFormatRowType();

		String[] fieldNames = context.getSchema().getFieldNames();
		List<String> projectFields = Arrays.stream(context.getProjectFields())
			.mapToObj(idx -> fieldNames[idx])
			.collect(Collectors.toList());
		List<String> csvFields = Arrays.stream(fieldNames)
			.filter(field -> !context.getPartitionKeys().contains(field))
			.collect(Collectors.toList());

		int[] csvSelectFieldToProjectFieldMapping = context.getFormatProjectFields().stream()
			.mapToInt(projectFields::indexOf)
			.toArray();
		int[] csvSelectFieldToCsvFieldMapping = context.getFormatProjectFields().stream()
			.mapToInt(csvFields::indexOf)
			.toArray();

		CsvSchema csvSchema = buildCsvSchema(formatRowType, properties);

		boolean ignoreParseErrors = properties.getOptionalBoolean(FORMAT_IGNORE_PARSE_ERRORS).orElse(false);

		return new CsvInputFormat(
			context.getPaths(),
			context.getSchema().getFieldDataTypes(),
			context.getSchema().getFieldNames(),
			csvSchema,
			formatRowType,
			context.getProjectFields(),
			context.getPartitionKeys(),
			context.getDefaultPartName(),
			context.getPushedDownLimit(),
			csvSelectFieldToProjectFieldMapping,
			csvSelectFieldToCsvFieldMapping,
			ignoreParseErrors);
	}

	private CsvSchema buildCsvSchema(RowType rowType, DescriptorProperties properties) {
		CsvSchema csvSchema = CsvRowSchemaConverter.convert(rowType);
		CsvSchema.Builder csvBuilder = csvSchema.rebuild();
		//format properties
		properties.getOptionalCharacter(FORMAT_FIELD_DELIMITER)
			.ifPresent(csvBuilder::setColumnSeparator);

		properties.getOptionalCharacter(FORMAT_QUOTE_CHARACTER)
			.ifPresent(csvBuilder::setQuoteChar);

		properties.getOptionalBoolean(FORMAT_ALLOW_COMMENTS)
			.ifPresent(csvBuilder::setAllowComments);

		properties.getOptionalString(FORMAT_ARRAY_ELEMENT_DELIMITER)
			.ifPresent(csvBuilder::setArrayElementSeparator);

		properties.getOptionalString(FORMAT_ARRAY_ELEMENT_DELIMITER)
			.ifPresent(csvBuilder::setArrayElementSeparator);

		properties.getOptionalCharacter(FORMAT_ESCAPE_CHARACTER)
			.ifPresent(csvBuilder::setEscapeChar);

		properties.getOptionalString(FORMAT_NULL_LITERAL)
			.ifPresent(csvBuilder::setNullValue);

		properties.getOptionalString(FORMAT_LINE_DELIMITER)
			.ifPresent(csvBuilder::setLineSeparator);

		return csvBuilder.build();
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {

		CsvRowDataSerializationSchema.Builder builder = new CsvRowDataSerializationSchema.Builder(
			context.getFormatRowType());

		DescriptorProperties properties = getValidatedProperties(context.getFormatProperties());

		properties.getOptionalCharacter(FORMAT_FIELD_DELIMITER)
			.ifPresent(builder::setFieldDelimiter);

		properties.getOptionalString(FORMAT_LINE_DELIMITER)
			.ifPresent(builder::setLineDelimiter);

		if (properties.getOptionalBoolean(FORMAT_DISABLE_QUOTE_CHARACTER).orElse(false)) {
			builder.disableQuoteCharacter();
		} else {
			properties.getOptionalCharacter(FORMAT_QUOTE_CHARACTER).ifPresent(builder::setQuoteCharacter);
		}

		properties.getOptionalString(FORMAT_ARRAY_ELEMENT_DELIMITER)
			.ifPresent(builder::setArrayElementDelimiter);

		properties.getOptionalCharacter(FORMAT_ESCAPE_CHARACTER)
			.ifPresent(builder::setEscapeCharacter);

		properties.getOptionalString(FORMAT_NULL_LITERAL)
			.ifPresent(builder::setNullLiteral);

		final CsvRowDataSerializationSchema serializationSchema = builder.build();

		return Optional.of((record, stream) -> stream.write(serializationSchema.serialize(record)));
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		return Optional.empty();
	}

	@Override
	public boolean supportsSchemaDerivation() {
		return true;
	}

	@Override
	public List<String> supportedProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(FORMAT_FIELD_DELIMITER);
		properties.add(FORMAT_LINE_DELIMITER);
		properties.add(FORMAT_DISABLE_QUOTE_CHARACTER);
		properties.add(FORMAT_QUOTE_CHARACTER);
		properties.add(FORMAT_ALLOW_COMMENTS);
		properties.add(FORMAT_IGNORE_PARSE_ERRORS);
		properties.add(FORMAT_ARRAY_ELEMENT_DELIMITER);
		properties.add(FORMAT_ESCAPE_CHARACTER);
		properties.add(FORMAT_NULL_LITERAL);
		return properties;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(FORMAT, "csv");
		return context;
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(propertiesMap);

		properties.validateString(FORMAT_FIELD_DELIMITER, true, 1, 1);
		properties.validateEnumValues(FORMAT_LINE_DELIMITER, true, Arrays.asList("\r", "\n", "\r\n", ""));
		properties.validateBoolean(FORMAT_DISABLE_QUOTE_CHARACTER, true);
		properties.validateString(FORMAT_QUOTE_CHARACTER, true, 1, 1);
		properties.validateBoolean(FORMAT_ALLOW_COMMENTS, true);
		properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
		properties.validateString(FORMAT_ARRAY_ELEMENT_DELIMITER, true, 1);
		properties.validateString(FORMAT_ESCAPE_CHARACTER, true, 1, 1);

		return properties;
	}

	/**
	 * InputFormat that reads csv record into {@link RowData}.
	 */
	public static class CsvInputFormat extends AbstractCsvInputFormat<RowData> {
		private static final long serialVersionUID = 1L;

		private final RowType formatRowType;
		private final DataType[] fieldTypes;
		private final String[] fieldNames;
		private final int[] selectFields;
		private final List<String> partitionKeys;
		private final String defaultPartValue;
		private final long limit;
		private final int[] csvSelectFieldToProjectFieldMapping;
		private final int[] csvSelectFieldToCsvFieldMapping;
		private final boolean ignoreParseErrors;

		private transient InputStreamReader inputStreamReader;
		private transient BufferedReader reader;
		private transient boolean end;
		private transient long emitted;
		// reuse object for per record
		private transient GenericRowData rowData;
		private transient DeserializationRuntimeConverter runtimeConverter;
		private transient MappingIterator<JsonNode> iterator;

		public CsvInputFormat(
			Path[] filePaths,
			DataType[] fieldTypes,
			String[] fieldNames,
			CsvSchema csvSchema,
			RowType formatRowType,
			int[] selectFields,
			List<String> partitionKeys,
			String defaultPartValue,
			long limit,
			int[] csvSelectFieldToProjectFieldMapping,
			int[] csvSelectFieldToCsvFieldMapping,
			boolean ignoreParseErrors) {
			super(filePaths, csvSchema);
			this.fieldTypes = fieldTypes;
			this.fieldNames = fieldNames;
			this.formatRowType = formatRowType;
			this.partitionKeys = partitionKeys;
			this.defaultPartValue = defaultPartValue;
			this.selectFields = selectFields;
			this.limit = limit;
			this.emitted = 0;
			this.csvSelectFieldToProjectFieldMapping = csvSelectFieldToProjectFieldMapping;
			this.csvSelectFieldToCsvFieldMapping = csvSelectFieldToCsvFieldMapping;
			this.ignoreParseErrors = ignoreParseErrors;
		}

		@Override
		public void open(FileInputSplit split) throws IOException {
			super.open(split);
			this.end = false;
			this.inputStreamReader = new InputStreamReader(csvInputStream);
			this.reader = new BufferedReader(inputStreamReader);
			this.rowData = PartitionPathUtils.fillPartitionValueForRecord(fieldNames, fieldTypes, selectFields,
				partitionKeys, currentSplit.getPath(), defaultPartValue);
			this.iterator = new CsvMapper()
				.readerFor(JsonNode.class)
				.with(csvSchema)
				.readValues(csvInputStream);
			prepareRuntimeConverter();
		}

		private void prepareRuntimeConverter() {
			CsvRowDataDeserializationSchema.Builder builder = new CsvRowDataDeserializationSchema.Builder(
				formatRowType, new GenericTypeInfo<>(RowData.class))
				.setIgnoreParseErrors(ignoreParseErrors);
			this.runtimeConverter = builder.build().createRowConverter(formatRowType, true);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return emitted >= limit || end;
		}

		@Override
		public RowData nextRecord(RowData reuse) throws IOException {
			GenericRowData csvRow = null;
			while (csvRow == null) {
				try {
					JsonNode root = iterator.nextValue();
					csvRow = (GenericRowData) runtimeConverter.convert(root);
				} catch (NoSuchElementException e) {
					end = true;
					return null;
				} catch (Throwable t) {
					if (!ignoreParseErrors) {
						throw new IOException("Failed to deserialize CSV row.", t);
					}
				}
			}

			GenericRowData returnRecord = rowData;
			for (int i = 0; i < csvSelectFieldToCsvFieldMapping.length; i++) {
				returnRecord.setField(csvSelectFieldToProjectFieldMapping[i],
					csvRow.getField(csvSelectFieldToCsvFieldMapping[i]));
			}
			emitted++;
			return returnRecord;
		}

		@Override
		public void close() throws IOException {
			super.close();
			if (reader != null) {
				reader.close();
				reader = null;
			}
			if (inputStreamReader != null) {
				inputStreamReader.close();
				inputStreamReader = null;
			}
		}
	}
}
