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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.formats.common.Converter;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code StreamFormat} for reading CSV files.
 *
 * <p>The following example shows how to create a {@code CsvReaderFormat} where the schema for CSV
 * parsing is automatically derived based on the fields of a POJO class.
 *
 * <pre>{@code
 * CsvReaderFormat<SomePojo> csvFormat = CsvReaderFormat.forPojo(SomePojo.class);
 * FileSource<SomePojo> source =
 *         FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(filesPath)).build();
 * }</pre>
 *
 * <i> Note: you might need to add {@code @JsonPropertyOrder({field1, field2, ...})} annotation from
 * the {@code Jackson} library to your class definition with the fields order exactly matching those
 * of the CSV file columns).</i>
 *
 * <p>If you need more fine-grained control over the CSV schema or the parsing options, use the more
 * low-level {@code forSchema} static factory method based on the {@code Jackson} library utilities:
 *
 * <pre>{@code
 * Function<CsvMapper, CsvSchema> schemaGenerator =
 *          mapper -> mapper.schemaFor(SomePojo.class)
 *                          .withColumnSeparator('|');
 * CsvReaderFormat<SomePojo> csvFormat =
 *          CsvReaderFormat.forSchema(() -> new CsvMapper(), schemaGenerator, TypeInformation.of(SomePojo.class));
 * FileSource<SomePojo> source =
 *         FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(filesPath)).build();
 * }</pre>
 *
 * @param <T> The type of the returned elements.
 */
@PublicEvolving
public class CsvReaderFormat<T> extends SimpleStreamFormat<T> {

    private static final long serialVersionUID = 1L;

    private final SerializableSupplier<CsvMapper> mapperFactory;
    private final SerializableFunction<CsvMapper, CsvSchema> schemaGenerator;
    private final Class<Object> rootType;
    private final Converter<Object, T, Void> converter;
    private final TypeInformation<T> typeInformation;
    private boolean ignoreParseErrors;

    @SuppressWarnings("unchecked")
    <R> CsvReaderFormat(
            SerializableSupplier<CsvMapper> mapperFactory,
            SerializableFunction<CsvMapper, CsvSchema> schemaGenerator,
            Class<R> rootType,
            Converter<R, T, Void> converter,
            TypeInformation<T> typeInformation,
            boolean ignoreParseErrors) {
        this.mapperFactory = checkNotNull(mapperFactory);
        this.schemaGenerator = checkNotNull(schemaGenerator);
        this.rootType = (Class<Object>) checkNotNull(rootType);
        this.typeInformation = checkNotNull(typeInformation);
        this.converter = (Converter<Object, T, Void>) checkNotNull(converter);
        this.ignoreParseErrors = ignoreParseErrors;
    }

    /**
     * Builds a new {@code CsvReaderFormat} using a {@code CsvSchema}.
     *
     * @param schema The Jackson CSV schema configured for parsing specific CSV files.
     * @param typeInformation The Flink type descriptor of the returned elements.
     * @param <T> The type of the returned elements.
     */
    public static <T> CsvReaderFormat<T> forSchema(
            CsvSchema schema, TypeInformation<T> typeInformation) {
        return forSchema(JacksonMapperFactory::createCsvMapper, ignored -> schema, typeInformation);
    }

    /**
     * @deprecated This method is limited to serializable {@link CsvMapper CsvMappers}, preventing
     *     the usage of certain Jackson modules (like the {@link
     *     org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
     *     Java 8 Date/Time Serializers}). Use {@link #forSchema(Supplier, Function,
     *     TypeInformation)} instead.
     */
    @Deprecated
    public static <T> CsvReaderFormat<T> forSchema(
            CsvMapper mapper, CsvSchema schema, TypeInformation<T> typeInformation) {
        return new CsvReaderFormat<>(
                () -> mapper,
                ignored -> schema,
                typeInformation.getTypeClass(),
                (value, context) -> value,
                typeInformation,
                false);
    }

    /**
     * Builds a new {@code CsvReaderFormat} using a {@code CsvSchema} generator and {@code
     * CsvMapper} factory.
     *
     * @param mapperFactory The factory creating the {@code CsvMapper}.
     * @param schemaGenerator A generator that creates and configures the Jackson CSV schema for
     *     parsing specific CSV files, from a mapper created by the mapper factory.
     * @param typeInformation The Flink type descriptor of the returned elements.
     * @param <T> The type of the returned elements.
     */
    public static <T> CsvReaderFormat<T> forSchema(
            SerializableSupplier<CsvMapper> mapperFactory,
            SerializableFunction<CsvMapper, CsvSchema> schemaGenerator,
            TypeInformation<T> typeInformation) {
        return new CsvReaderFormat<>(
                mapperFactory,
                schemaGenerator,
                typeInformation.getTypeClass(),
                (value, context) -> value,
                typeInformation,
                false);
    }

    /**
     * Builds a new {@code CsvReaderFormat} for reading CSV files mapped to the provided POJO class
     * definition. Produced reader uses default mapper and schema settings, use {@code forSchema} if
     * you need customizations.
     *
     * @param pojoType The type class of the POJO.
     * @param <T> The type of the returned elements.
     */
    public static <T> CsvReaderFormat<T> forPojo(Class<T> pojoType) {
        return forSchema(
                JacksonMapperFactory::createCsvMapper,
                mapper -> mapper.schemaFor(pojoType).withoutQuoteChar(),
                TypeInformation.of(pojoType));
    }

    /**
     * Returns a new {@code CsvReaderFormat} configured to ignore all parsing errors. All the other
     * options directly carried over from the subject of the method call.
     */
    public CsvReaderFormat<T> withIgnoreParseErrors() {
        return new CsvReaderFormat<>(
                this.mapperFactory,
                this.schemaGenerator,
                this.rootType,
                this.converter,
                this.typeInformation,
                true);
    }

    @Override
    public StreamFormat.Reader<T> createReader(Configuration config, FSDataInputStream stream)
            throws IOException {
        final CsvMapper csvMapper = mapperFactory.get();
        return new Reader<>(
                csvMapper
                        .readerFor(rootType)
                        .with(schemaGenerator.apply(csvMapper))
                        .readValues(stream),
                converter,
                ignoreParseErrors);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInformation;
    }

    // ------------------------------------------------------------------------

    /** The actual reader for the {@code CsvFormat}. */
    private static final class Reader<R, T> implements StreamFormat.Reader<T> {
        private final MappingIterator<R> iterator;
        private final Converter<R, T, Void> converter;
        private final boolean ignoreParseErrors;

        public Reader(
                MappingIterator<R> iterator,
                Converter<R, T, Void> converter,
                boolean ignoreParseErrors) {
            this.iterator = checkNotNull(iterator);
            this.converter = checkNotNull(converter);
            this.ignoreParseErrors = ignoreParseErrors;
        }

        @Nullable
        @Override
        public T read() throws IOException {
            while (iterator.hasNext()) {
                try {
                    R nextElement = iterator.next();
                    return converter.convert(nextElement, null);
                } catch (Throwable t) {
                    if (!ignoreParseErrors) {
                        throw t;
                    }
                }
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }
    }
}
