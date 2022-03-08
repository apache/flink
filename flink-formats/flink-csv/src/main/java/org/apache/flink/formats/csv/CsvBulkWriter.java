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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.common.Converter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A simple {@link BulkWriter} implementation based on Jackson CSV transformations. */
class CsvBulkWriter<T, R, C> implements BulkWriter<T> {

    private final FSDataOutputStream stream;
    private final Converter<T, R, C> converter;
    @Nullable private final C converterContext;
    private final ObjectWriter csvWriter;

    CsvBulkWriter(
            CsvMapper mapper,
            CsvSchema schema,
            Converter<T, R, C> converter,
            @Nullable C converterContext,
            FSDataOutputStream stream) {
        checkNotNull(mapper);
        checkNotNull(schema);

        this.converter = checkNotNull(converter);
        this.stream = checkNotNull(stream);
        this.converterContext = converterContext;
        this.csvWriter = mapper.writer(schema);

        // Prevent Jackson's writeValue() method calls from closing the stream.
        mapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    }

    /**
     * Builds a writer with Jackson schema and a type converter.
     *
     * @param mapper The specialized mapper for producing CSV.
     * @param schema The schema that defined the mapping properties.
     * @param converter The type converter that converts incoming elements of type {@code <T>} into
     *     elements of type JsonNode.
     * @param stream The output stream.
     * @param <T> The type of the elements accepted by this writer.
     * @param <C> The type of the converter context.
     * @param <R> The type of the elements produced by this writer.
     */
    static <T, R, C> CsvBulkWriter<T, R, C> forSchema(
            CsvMapper mapper,
            CsvSchema schema,
            Converter<T, R, C> converter,
            @Nullable C converterContext,
            FSDataOutputStream stream) {
        return new CsvBulkWriter<>(mapper, schema, converter, converterContext, stream);
    }

    /**
     * Builds a writer based on a POJO class definition.
     *
     * @param pojoClass The class of the POJO.
     * @param stream The output stream.
     * @param <T> The type of the elements accepted by this writer.
     */
    static <T> CsvBulkWriter<T, T, Void> forPojo(Class<T> pojoClass, FSDataOutputStream stream) {
        final Converter<T, T, Void> converter = (value, context) -> value;
        final CsvMapper csvMapper = new CsvMapper();
        final CsvSchema schema = csvMapper.schemaFor(pojoClass).withoutQuoteChar();
        return new CsvBulkWriter<>(csvMapper, schema, converter, null, stream);
    }

    @Override
    public void addElement(T element) throws IOException {
        final R r = converter.convert(element, converterContext);
        csvWriter.writeValue(stream, r);
    }

    @Override
    public void flush() throws IOException {
        stream.flush();
    }

    @Override
    public void finish() throws IOException {
        stream.sync();
    }
}
