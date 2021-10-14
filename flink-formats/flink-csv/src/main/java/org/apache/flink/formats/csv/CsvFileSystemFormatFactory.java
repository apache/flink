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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.lang3.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.formats.csv.CsvFormatFactory.validateFormatOptions;
import static org.apache.flink.formats.csv.CsvFormatOptions.ALLOW_COMMENTS;
import static org.apache.flink.formats.csv.CsvFormatOptions.ARRAY_ELEMENT_DELIMITER;
import static org.apache.flink.formats.csv.CsvFormatOptions.DISABLE_QUOTE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.ESCAPE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.FIELD_DELIMITER;
import static org.apache.flink.formats.csv.CsvFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.csv.CsvFormatOptions.NULL_LITERAL;
import static org.apache.flink.formats.csv.CsvFormatOptions.QUOTE_CHARACTER;

/** CSV format factory for file system. */
public class CsvFileSystemFormatFactory implements FileSystemFormatFactory {

    public static final String IDENTIFIER = "csv";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FIELD_DELIMITER);
        options.add(DISABLE_QUOTE_CHARACTER);
        options.add(QUOTE_CHARACTER);
        options.add(ALLOW_COMMENTS);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(ARRAY_ELEMENT_DELIMITER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        return options;
    }

    @Override
    public InputFormat<RowData, ?> createReader(ReaderContext context) {
        ReadableConfig options = context.getFormatOptions();
        validateFormatOptions(options);

        RowType formatRowType = context.getFormatRowType();

        String[] fieldNames = context.getSchema().getFieldNames();
        List<String> projectFields =
                Arrays.stream(context.getProjectFields())
                        .mapToObj(idx -> fieldNames[idx])
                        .collect(Collectors.toList());
        List<String> csvFields =
                Arrays.stream(fieldNames)
                        .filter(field -> !context.getPartitionKeys().contains(field))
                        .collect(Collectors.toList());

        int[] csvSelectFieldToProjectFieldMapping =
                context.getFormatProjectFields().stream()
                        .mapToInt(projectFields::indexOf)
                        .toArray();
        int[] csvSelectFieldToCsvFieldMapping =
                context.getFormatProjectFields().stream().mapToInt(csvFields::indexOf).toArray();

        CsvSchema csvSchema = buildCsvSchema(formatRowType, options);

        boolean ignoreParseErrors = options.get(IGNORE_PARSE_ERRORS);

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

    private CsvSchema buildCsvSchema(RowType rowType, ReadableConfig options) {
        CsvSchema csvSchema = CsvRowSchemaConverter.convert(rowType);
        CsvSchema.Builder csvBuilder = csvSchema.rebuild();
        // format properties
        options.getOptional(FIELD_DELIMITER)
                .map(s -> StringEscapeUtils.unescapeJava(s).charAt(0))
                .ifPresent(csvBuilder::setColumnSeparator);

        options.getOptional(QUOTE_CHARACTER)
                .map(s -> s.charAt(0))
                .ifPresent(csvBuilder::setQuoteChar);

        options.getOptional(ALLOW_COMMENTS).ifPresent(csvBuilder::setAllowComments);

        options.getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(csvBuilder::setArrayElementSeparator);

        options.getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(csvBuilder::setArrayElementSeparator);

        options.getOptional(ESCAPE_CHARACTER)
                .map(s -> s.charAt(0))
                .ifPresent(csvBuilder::setEscapeChar);

        options.getOptional(NULL_LITERAL).ifPresent(csvBuilder::setNullValue);

        return csvBuilder.build();
    }

    /** InputFormat that reads csv record into {@link RowData}. */
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
        private transient CsvToRowDataConverters.CsvToRowDataConverter runtimeConverter;
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
            this.rowData =
                    PartitionPathUtils.fillPartitionValueForRecord(
                            fieldNames,
                            fieldTypes,
                            selectFields,
                            partitionKeys,
                            currentSplit.getPath(),
                            defaultPartValue);
            this.iterator =
                    new CsvMapper()
                            .readerFor(JsonNode.class)
                            .with(csvSchema)
                            .readValues(csvInputStream);
            prepareRuntimeConverter();
        }

        private void prepareRuntimeConverter() {
            this.runtimeConverter =
                    new CsvToRowDataConverters(ignoreParseErrors)
                            .createRowConverter(formatRowType, true);
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
                returnRecord.setField(
                        csvSelectFieldToProjectFieldMapping[i],
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
