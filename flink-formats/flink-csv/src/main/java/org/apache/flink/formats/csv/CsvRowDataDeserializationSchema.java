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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Deserialization schema from CSV to Flink Table & SQL internal data structures.
 *
 * <p>Deserializes a <code>byte[]</code> message as a {@link JsonNode} and converts it to {@link
 * RowData}.
 *
 * <p>Failure during deserialization are forwarded as wrapped {@link IOException}s.
 */
@Internal
public final class CsvRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** Type information describing the result type. */
    private final TypeInformation<RowData> resultTypeInfo;

    /** Runtime instance that performs the actual work. */
    private final CsvToRowDataConverters.CsvToRowDataConverter runtimeConverter;

    /** Schema describing the input CSV data. */
    private final CsvSchema csvSchema;

    /** Object reader used to read rows. It is configured by {@link CsvSchema}. */
    private transient ObjectReader objectReader;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    private CsvRowDataDeserializationSchema(
            TypeInformation<RowData> resultTypeInfo,
            CsvSchema csvSchema,
            CsvToRowDataConverters.CsvToRowDataConverter runtimeConverter,
            boolean ignoreParseErrors) {
        this.resultTypeInfo = resultTypeInfo;
        this.runtimeConverter = runtimeConverter;
        this.csvSchema = csvSchema;
        this.ignoreParseErrors = ignoreParseErrors;
    }

    @Override
    public void open(InitializationContext context) {
        this.objectReader =
                JacksonMapperFactory.createCsvMapper().readerFor(JsonNode.class).with(csvSchema);
    }

    /** A builder for creating a {@link CsvRowDataDeserializationSchema}. */
    @Internal
    public static class Builder {

        private final RowType rowResultType;
        private final TypeInformation<RowData> resultTypeInfo;
        private CsvSchema csvSchema;
        private boolean ignoreParseErrors;

        /**
         * Creates a CSV deserialization schema for the given {@link TypeInformation} with optional
         * parameters.
         *
         * @param rowReadType The {@link RowType} used for reading CSV rows.
         * @param rowResultType The {@link RowType} of the produced results. It can be different
         *     from the {@code rowReadType} if the underlying converter supports the discrepancy
         *     (for instance for filtering/projection pushdown).
         * @param resultTypeInfo The result type info.
         */
        public Builder(
                RowType rowReadType,
                RowType rowResultType,
                TypeInformation<RowData> resultTypeInfo) {
            Preconditions.checkNotNull(rowReadType, "RowType must not be null.");
            Preconditions.checkNotNull(rowResultType, "RowType must not be null.");
            Preconditions.checkNotNull(resultTypeInfo, "Result type information must not be null.");
            this.rowResultType = rowResultType;
            this.resultTypeInfo = resultTypeInfo;
            this.csvSchema = CsvRowSchemaConverter.convert(rowReadType);
        }

        /**
         * Creates a CSV deserialization schema for the given {@link TypeInformation} with optional
         * parameters.
         */
        public Builder(RowType rowType, TypeInformation<RowData> resultTypeInfo) {
            Preconditions.checkNotNull(resultTypeInfo, "Result type information must not be null.");
            this.rowResultType = rowType;
            this.resultTypeInfo = resultTypeInfo;
            this.csvSchema = CsvRowSchemaConverter.convert(rowType);
        }

        public Builder setFieldDelimiter(char delimiter) {
            this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(delimiter).build();
            return this;
        }

        public Builder setAllowComments(boolean allowComments) {
            this.csvSchema = this.csvSchema.rebuild().setAllowComments(allowComments).build();
            return this;
        }

        public Builder setArrayElementDelimiter(String delimiter) {
            Preconditions.checkNotNull(delimiter, "Array element delimiter must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
            return this;
        }

        public Builder disableQuoteCharacter() {
            this.csvSchema = this.csvSchema.rebuild().disableQuoteChar().build();
            return this;
        }

        public Builder setQuoteCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
            return this;
        }

        public Builder setEscapeCharacter(char c) {
            this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
            return this;
        }

        public Builder setNullLiteral(String nullLiteral) {
            Preconditions.checkNotNull(nullLiteral, "Null literal must not be null.");
            this.csvSchema = this.csvSchema.rebuild().setNullValue(nullLiteral).build();
            return this;
        }

        public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
            this.ignoreParseErrors = ignoreParseErrors;
            return this;
        }

        public CsvRowDataDeserializationSchema build() {
            CsvToRowDataConverters.CsvToRowDataConverter runtimeConverter =
                    new CsvToRowDataConverters(ignoreParseErrors)
                            .createRowConverter(rowResultType, true);
            return new CsvRowDataDeserializationSchema(
                    resultTypeInfo, csvSchema, runtimeConverter, ignoreParseErrors);
        }
    }

    @Override
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            final JsonNode root = objectReader.readValue(message);
            return (RowData) runtimeConverter.convert(root);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    String.format("Failed to deserialize CSV row '%s'.", new String(message)), t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        final CsvRowDataDeserializationSchema that = (CsvRowDataDeserializationSchema) o;
        final CsvSchema otherSchema = that.csvSchema;

        return resultTypeInfo.equals(that.resultTypeInfo)
                && ignoreParseErrors == that.ignoreParseErrors
                && csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator()
                && csvSchema.allowsComments() == otherSchema.allowsComments()
                && csvSchema
                        .getArrayElementSeparator()
                        .equals(otherSchema.getArrayElementSeparator())
                && csvSchema.getQuoteChar() == otherSchema.getQuoteChar()
                && csvSchema.getEscapeChar() == otherSchema.getEscapeChar()
                && Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                resultTypeInfo,
                ignoreParseErrors,
                csvSchema.getColumnSeparator(),
                csvSchema.allowsComments(),
                csvSchema.getArrayElementSeparator(),
                csvSchema.getQuoteChar(),
                csvSchema.getEscapeChar(),
                Arrays.hashCode(csvSchema.getNullValue()));
    }
}
