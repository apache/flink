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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;

/**
 * A {@link StreamTableSource} for simple CSV files with a (logically) unlimited number of fields.
 *
 * @deprecated The legacy CSV connector has been replaced by {@code FileSource}. It is kept only to
 *     support tests for the legacy connector stack.
 */
@Internal
@Deprecated
public class CsvTableSource
        implements StreamTableSource<Row>, LookupableTableSource<Row>, ProjectableTableSource<Row> {

    private final CsvInputFormatConfig config;

    /**
     * A {@link InputFormatTableSource} and {@link LookupableTableSource} for simple CSV files with
     * a (logically) unlimited number of fields.
     *
     * @param path The path to the CSV file.
     * @param fieldNames The names of the table fields.
     * @param fieldTypes The types of the table fields.
     */
    public CsvTableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this(
                path,
                fieldNames,
                fieldTypes,
                IntStream.range(0, fieldNames.length).toArray(),
                CsvInputFormat.DEFAULT_FIELD_DELIMITER,
                CsvInputFormat.DEFAULT_LINE_DELIMITER,
                null,
                false,
                null,
                false);
    }

    /**
     * A {@link InputFormatTableSource} and {@link LookupableTableSource} for simple CSV files with
     * a (logically) unlimited number of fields.
     *
     * @param path The path to the CSV file.
     * @param fieldNames The names of the table fields.
     * @param fieldTypes The types of the table fields.
     * @param fieldDelim The field delimiter, "," by default.
     * @param lineDelim The row delimiter, "\n" by default.
     * @param quoteCharacter An optional quote character for String values, null by default.
     * @param ignoreFirstLine Flag to ignore the first line, false by default.
     * @param ignoreComments An optional prefix to indicate comments, null by default.
     * @param lenient Flag to skip records with parse error instead to fail, false by default.
     */
    public CsvTableSource(
            String path,
            String[] fieldNames,
            TypeInformation<?>[] fieldTypes,
            String fieldDelim,
            String lineDelim,
            Character quoteCharacter,
            boolean ignoreFirstLine,
            String ignoreComments,
            boolean lenient) {
        this(
                path,
                fieldNames,
                fieldTypes,
                IntStream.range(0, fieldNames.length).toArray(),
                fieldDelim,
                lineDelim,
                quoteCharacter,
                ignoreFirstLine,
                ignoreComments,
                lenient);
    }

    /**
     * A {@link InputFormatTableSource} and {@link LookupableTableSource} for simple CSV files with
     * a (logically) unlimited number of fields.
     *
     * @param path The path to the CSV file.
     * @param fieldNames The names of the table fields.
     * @param fieldTypes The types of the table fields.
     * @param selectedFields The fields which will be read and returned by the table source. If
     *     None, all fields are returned.
     * @param fieldDelim The field delimiter, "," by default.
     * @param lineDelim The row delimiter, "\n" by default.
     * @param quoteCharacter An optional quote character for String values, null by default.
     * @param ignoreFirstLine Flag to ignore the first line, false by default.
     * @param ignoreComments An optional prefix to indicate comments, null by default.
     * @param lenient Flag to skip records with parse error instead to fail, false by default.
     */
    public CsvTableSource(
            String path,
            String[] fieldNames,
            TypeInformation<?>[] fieldTypes,
            int[] selectedFields,
            String fieldDelim,
            String lineDelim,
            Character quoteCharacter,
            boolean ignoreFirstLine,
            String ignoreComments,
            boolean lenient) {
        this(
                new CsvInputFormatConfig(
                        path,
                        fieldNames,
                        TypeConversions.fromLegacyInfoToDataType(fieldTypes),
                        selectedFields,
                        fieldDelim,
                        lineDelim,
                        quoteCharacter,
                        ignoreFirstLine,
                        ignoreComments,
                        lenient,
                        false));
    }

    private CsvTableSource(CsvInputFormatConfig config) {
        this.config = config;
    }

    /**
     * Return a new builder that builds a CsvTableSource. For example:
     *
     * <pre>
     * CsvTableSource source = new CsvTableSource.builder()
     *     .path("/path/to/your/file.csv")
     *     .field("myfield", Types.STRING)
     *     .field("myfield2", Types.INT)
     *     .build();
     * </pre>
     *
     * @return a new builder to build a CsvTableSource
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public DataType getProducedDataType() {
        return TableSchema.builder()
                .fields(config.getSelectedFieldNames(), config.getSelectedFieldDataTypes())
                .build()
                .toRowDataType();
    }

    @SuppressWarnings("unchecked")
    private TypeInformation<Row> getProducedTypeInformation() {
        return (TypeInformation<Row>) fromDataTypeToLegacyInfo(getProducedDataType());
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(config.fieldNames, config.fieldTypes).build();
    }

    @Override
    public CsvTableSource projectFields(int[] fields) {
        if (fields.length == 0) {
            fields = new int[0];
        }
        return new CsvTableSource(config.select(fields));
    }

    @Override
    public boolean isBounded() {
        return true;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.createInput(config.createInputFormat(), getProducedTypeInformation())
                .name(explainSource());
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return new CsvLookupFunction(config, lookupKeys);
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        throw new UnsupportedOperationException("CSV do not support async lookup");
    }

    @Override
    public boolean isAsyncEnabled() {
        return false;
    }

    @Override
    public String explainSource() {
        String[] fields = config.getSelectedFieldNames();
        return "CsvTableSource(read fields: " + String.join(", ", fields) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CsvTableSource that = (CsvTableSource) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

    /** A builder for creating CsvTableSource instances. */
    @Internal
    public static class Builder {
        private LinkedHashMap<String, DataType> schema = new LinkedHashMap<>();
        private Character quoteCharacter;
        private String path;
        private String fieldDelim = CsvInputFormat.DEFAULT_FIELD_DELIMITER;
        private String lineDelim = CsvInputFormat.DEFAULT_LINE_DELIMITER;
        private boolean isIgnoreFirstLine = false;
        private String commentPrefix;
        private boolean lenient = false;
        private boolean emptyColumnAsNull = false;

        /**
         * Sets the path to the CSV file. Required.
         *
         * @param path the path to the CSV file
         */
        public Builder path(String path) {
            this.path = path;
            return this;
        }

        /**
         * Sets the field delimiter, "," by default.
         *
         * @param delim the field delimiter
         */
        public Builder fieldDelimiter(String delim) {
            this.fieldDelim = delim;
            return this;
        }

        /**
         * Sets the line delimiter, "\n" by default.
         *
         * @param delim the line delimiter
         */
        public Builder lineDelimiter(String delim) {
            this.lineDelim = delim;
            return this;
        }

        /**
         * Adds a field with the field name and the data type. Required. This method can be called
         * multiple times. The call order of this method defines also the order of the fields in a
         * row.
         *
         * @param fieldName the field name
         * @param fieldType the data type of the field
         */
        public Builder field(String fieldName, DataType fieldType) {
            if (schema.containsKey(fieldName)) {
                throw new IllegalArgumentException("Duplicate field name " + fieldName);
            }
            // CSV only support java.sql.Timestamp/Date/Time
            DataType type;
            switch (fieldType.getLogicalType().getTypeRoot()) {
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    type = fieldType.bridgedTo(Timestamp.class);
                    break;
                case TIME_WITHOUT_TIME_ZONE:
                    type = fieldType.bridgedTo(Time.class);
                    break;
                case DATE:
                    type = fieldType.bridgedTo(Date.class);
                    break;
                default:
                    type = fieldType;
            }
            schema.put(fieldName, type);
            return this;
        }

        /**
         * Adds a field with the field name and the type information. Required. This method can be
         * called multiple times. The call order of this method defines also the order of the fields
         * in a row.
         *
         * @param fieldName the field name
         * @param fieldType the type information of the field
         * @deprecated This method will be removed in future versions as it uses the old type
         *     system. It is recommended to use {@link #field(String, DataType)} instead which uses
         *     the new type system based on {@link DataTypes}. Please make sure to use either the
         *     old or the new type system consistently to avoid unintended behavior. See the website
         *     documentation for more information.
         */
        @Deprecated
        public Builder field(String fieldName, TypeInformation<?> fieldType) {
            return field(fieldName, TypeConversions.fromLegacyInfoToDataType(fieldType));
        }

        /**
         * Sets a quote character for String values, null by default.
         *
         * @param quote the quote character
         */
        public Builder quoteCharacter(Character quote) {
            this.quoteCharacter = quote;
            return this;
        }

        /**
         * Sets a prefix to indicate comments, null by default.
         *
         * @param prefix the prefix to indicate comments
         */
        public Builder commentPrefix(String prefix) {
            this.commentPrefix = prefix;
            return this;
        }

        /** Ignore the first line. Not skip the first line by default. */
        public Builder ignoreFirstLine() {
            this.isIgnoreFirstLine = true;
            return this;
        }

        /** Skip records with parse error instead to fail. Throw an exception by default. */
        public Builder ignoreParseErrors() {
            this.lenient = true;
            return this;
        }

        /** Treat empty column as null, false by default. */
        public Builder emptyColumnAsNull() {
            this.emptyColumnAsNull = true;
            return this;
        }

        /**
         * Apply the current values and constructs a newly-created CsvTableSource.
         *
         * @return a newly-created CsvTableSource
         */
        public CsvTableSource build() {
            if (path == null) {
                throw new IllegalArgumentException("Path must be defined.");
            }
            if (schema.isEmpty()) {
                throw new IllegalArgumentException("Fields can not be empty.");
            }

            return new CsvTableSource(
                    new CsvInputFormatConfig(
                            path,
                            schema.keySet().toArray(new String[0]),
                            schema.values().toArray(new DataType[0]),
                            IntStream.range(0, schema.values().size()).toArray(),
                            fieldDelim,
                            lineDelim,
                            quoteCharacter,
                            isIgnoreFirstLine,
                            commentPrefix,
                            lenient,
                            emptyColumnAsNull));
        }
    }

    // ------------------------------------------------------------------------------------
    // private utilities
    // ------------------------------------------------------------------------------------

    /** LookupFunction to support lookup in CsvTableSource. */
    @Internal
    public static class CsvLookupFunction extends TableFunction<Row> {
        private static final long serialVersionUID = 1L;

        private final CsvInputFormatConfig config;

        private final List<Integer> sourceKeys = new ArrayList<>();
        private final List<Integer> targetKeys = new ArrayList<>();
        private final Map<Object, List<Row>> dataMap = new HashMap<>();

        CsvLookupFunction(CsvInputFormatConfig config, String[] lookupKeys) {
            this.config = config;

            List<String> fields = Arrays.asList(config.getSelectedFieldNames());
            for (int i = 0; i < lookupKeys.length; i++) {
                sourceKeys.add(i);
                int targetIdx = fields.indexOf(lookupKeys[i]);
                assert targetIdx != -1;
                targetKeys.add(targetIdx);
            }
        }

        @Override
        public TypeInformation<Row> getResultType() {
            return new RowTypeInfo(config.getSelectedFieldTypes(), config.getSelectedFieldNames());
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            TypeInformation<Row> rowType = getResultType();

            RowCsvInputFormat inputFormat = config.createInputFormat();
            FileInputSplit[] inputSplits = inputFormat.createInputSplits(1);
            for (FileInputSplit split : inputSplits) {
                inputFormat.open(split);
                Row row = new Row(rowType.getArity());
                while (true) {
                    Row r = inputFormat.nextRecord(row);
                    if (r == null) {
                        break;
                    } else {
                        Object key = getTargetKey(r);
                        List<Row> rows = dataMap.computeIfAbsent(key, k -> new ArrayList<>());
                        rows.add(Row.copy(r));
                    }
                }
                inputFormat.close();
            }
        }

        public void eval(Object... values) {
            Object srcKey = getSourceKey(Row.of(values));
            if (dataMap.containsKey(srcKey)) {
                for (Row row1 : dataMap.get(srcKey)) {
                    collect(row1);
                }
            }
        }

        private Object getSourceKey(Row source) {
            return getKey(source, sourceKeys);
        }

        private Object getTargetKey(Row target) {
            return getKey(target, targetKeys);
        }

        private Object getKey(Row input, List<Integer> keys) {
            if (keys.size() == 1) {
                int keyIdx = keys.get(0);
                if (input.getField(keyIdx) != null) {
                    return input.getField(keyIdx);
                }
                return null;
            } else {
                Row key = new Row(keys.size());
                for (int i = 0; i < keys.size(); i++) {
                    int keyIdx = keys.get(i);
                    key.setField(i, input.getField(keyIdx));
                }
                return key;
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    private static class CsvInputFormatConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String path;
        private final String[] fieldNames;
        private final DataType[] fieldTypes;
        private final int[] selectedFields;

        private final String fieldDelim;
        private final String lineDelim;
        private final Character quoteCharacter;
        private final boolean ignoreFirstLine;
        private final String ignoreComments;
        private final boolean lenient;
        private final boolean emptyColumnAsNull;

        CsvInputFormatConfig(
                String path,
                String[] fieldNames,
                DataType[] fieldTypes,
                int[] selectedFields,
                String fieldDelim,
                String lineDelim,
                Character quoteCharacter,
                boolean ignoreFirstLine,
                String ignoreComments,
                boolean lenient,
                boolean emptyColumnAsNull) {

            this.path = path;
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
            this.selectedFields = selectedFields;
            this.fieldDelim = fieldDelim;
            this.lineDelim = lineDelim;
            this.quoteCharacter = quoteCharacter;
            this.ignoreFirstLine = ignoreFirstLine;
            this.ignoreComments = ignoreComments;
            this.lenient = lenient;
            this.emptyColumnAsNull = emptyColumnAsNull;
        }

        String[] getSelectedFieldNames() {
            String[] selectedFieldNames = new String[selectedFields.length];
            for (int i = 0; i < selectedFields.length; i++) {
                selectedFieldNames[i] = fieldNames[selectedFields[i]];
            }
            return selectedFieldNames;
        }

        DataType[] getSelectedFieldDataTypes() {
            DataType[] selectedFieldTypes = new DataType[selectedFields.length];
            for (int i = 0; i < selectedFields.length; i++) {
                selectedFieldTypes[i] = fieldTypes[selectedFields[i]];
            }
            return selectedFieldTypes;
        }

        TypeInformation<?>[] getSelectedFieldTypes() {
            return Arrays.stream(getSelectedFieldDataTypes())
                    .map(TypeConversions::fromDataTypeToLegacyInfo)
                    .toArray(TypeInformation[]::new);
        }

        RowCsvInputFormat createInputFormat() {
            RowCsvInputFormat inputFormat =
                    new RowCsvInputFormat(
                            new Path(path),
                            getSelectedFieldTypes(),
                            lineDelim,
                            fieldDelim,
                            selectedFields,
                            emptyColumnAsNull);
            inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine);
            inputFormat.setCommentPrefix(ignoreComments);
            inputFormat.setLenient(lenient);
            if (quoteCharacter != null) {
                inputFormat.enableQuotedStringParsing(quoteCharacter);
            }
            return inputFormat;
        }

        CsvInputFormatConfig select(int[] fields) {
            return new CsvInputFormatConfig(
                    path,
                    fieldNames,
                    fieldTypes,
                    fields,
                    fieldDelim,
                    lineDelim,
                    quoteCharacter,
                    ignoreFirstLine,
                    ignoreComments,
                    lenient,
                    emptyColumnAsNull);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CsvInputFormatConfig that = (CsvInputFormatConfig) o;
            return ignoreFirstLine == that.ignoreFirstLine
                    && lenient == that.lenient
                    && Objects.equals(path, that.path)
                    && Arrays.equals(fieldNames, that.fieldNames)
                    && Arrays.equals(fieldTypes, that.fieldTypes)
                    && Arrays.equals(selectedFields, that.selectedFields)
                    && Objects.equals(fieldDelim, that.fieldDelim)
                    && Objects.equals(lineDelim, that.lineDelim)
                    && Objects.equals(quoteCharacter, that.quoteCharacter)
                    && Objects.equals(ignoreComments, that.ignoreComments)
                    && Objects.equals(emptyColumnAsNull, that.emptyColumnAsNull);
        }

        @Override
        public int hashCode() {
            int result =
                    Objects.hash(
                            path,
                            fieldDelim,
                            lineDelim,
                            quoteCharacter,
                            ignoreFirstLine,
                            ignoreComments,
                            lenient,
                            emptyColumnAsNull);
            result = 31 * result + Arrays.hashCode(fieldNames);
            result = 31 * result + Arrays.hashCode(fieldTypes);
            result = 31 * result + Arrays.hashCode(selectedFields);
            return result;
        }
    }
}
