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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroToVariantDataConverters.AvroToVariantDataConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.variant.Variant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Deserialization schema from Avro bytes to {@link RowData} with a Variant physical column and
 * optional metadata columns appended at the end. Wraps a {@link
 * RegistryWriterAvroDeserializationSchema} that produces {@link GenericRecord}, then converts to
 * Variant and assembles the output row.
 *
 * <p>Converter cache performance will be better when combined with CachedSchemaProvider's that
 * return same Schema object reference in case of matching schemas.
 */
@Internal
public class AvroVariantDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(AvroVariantDeserializationSchema.class);

    private final RegistryWriterAvroDeserializationSchema innerDeserializationSchema;
    private final boolean includeSchemaMetadata;
    private final int maxCacheSize;
    private final TypeInformation<RowData> typeInfo;

    transient Map<Schema, AvroToVariantDataConverter> converterCache;

    public AvroVariantDeserializationSchema(
            RegistryWriterAvroDeserializationSchema innerDeserializationSchema,
            boolean includeSchemaMetadata,
            int maxCacheSize,
            TypeInformation<RowData> typeInfo) {
        this.innerDeserializationSchema = Objects.requireNonNull(innerDeserializationSchema);
        this.includeSchemaMetadata = includeSchemaMetadata;
        this.maxCacheSize = maxCacheSize;
        this.typeInfo = Objects.requireNonNull(typeInfo);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.innerDeserializationSchema.open(context);
        this.converterCache =
                new LinkedHashMap<>(maxCacheSize) {
                    @Override
                    protected boolean removeEldestEntry(
                            Map.Entry<Schema, AvroToVariantDataConverter> eldest) {
                        if (size() > maxCacheSize) {
                            LOG.info(
                                    "Evicting schema converter from cache, size exceeded {}",
                                    maxCacheSize);
                            return true;
                        }
                        return false;
                    }
                };
    }

    @Override
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }

        GenericRecord record = innerDeserializationSchema.deserialize(message);
        if (record == null) {
            return null;
        }

        GenericRowData row = new GenericRowData(includeSchemaMetadata ? 2 : 1);
        Schema writerSchema = record.getSchema();
        AvroToVariantDataConverter converter = getOrCreateConverter(writerSchema);
        Variant variant = converter.convert(record);
        row.setField(0, variant);

        if (includeSchemaMetadata) {
            row.setField(1, StringData.fromString(writerSchema.toString()));
        }

        return row;
    }

    private AvroToVariantDataConverter getOrCreateConverter(Schema writerSchema) {
        return converterCache.computeIfAbsent(
                writerSchema, AvroToVariantDataConverters::createVariantConverter);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvroVariantDeserializationSchema that = (AvroVariantDeserializationSchema) o;
        return includeSchemaMetadata == that.includeSchemaMetadata
                && maxCacheSize == that.maxCacheSize
                && innerDeserializationSchema.equals(that.innerDeserializationSchema)
                && typeInfo.equals(that.typeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                includeSchemaMetadata, maxCacheSize, innerDeserializationSchema, typeInfo);
    }
}
