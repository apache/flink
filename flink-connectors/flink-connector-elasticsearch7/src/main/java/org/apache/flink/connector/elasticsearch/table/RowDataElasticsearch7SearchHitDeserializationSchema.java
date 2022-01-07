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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.elasticsearch.source.reader.Elasticsearch7SearchHitDeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.search.SearchHit;

import java.io.IOException;

/** A {@link Elasticsearch7SearchHitDeserializationSchema} that deserializes to {@link RowData}. */
public class RowDataElasticsearch7SearchHitDeserializationSchema
        implements Elasticsearch7SearchHitDeserializationSchema<RowData> {

    /** TypeInformation of the produced {@link RowData}. */
    private final TypeInformation<RowData> producedTypeInformation;

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of Flink SQL internal data
     * structures.
     */
    private final JsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    RowDataElasticsearch7SearchHitDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> producedTypeInformation,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        this.producedTypeInformation = producedTypeInformation;
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;

        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }

        this.runtimeConverter =
                new JsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                        .createConverter(rowType);

        boolean hasDecimalType =
                LogicalTypeChecks.hasNested(rowType, t -> t instanceof DecimalType);
        if (hasDecimalType) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        // do nothing here
    }

    @Override
    public void deserialize(SearchHit record, Collector<RowData> out) throws IOException {
        if (record == null) {
            out.collect(null);
            return;
        }
        try {
            out.collect(convertToRowData(deserializeToJsonNode(record)));
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                out.collect(null);
                return;
            }
            throw new IOException(
                    String.format("Failed to deserialize JSON for SearchHit '%s'.", record));
        }
    }

    public TypeInformation<RowData> getProducedType() {
        return producedTypeInformation;
    }

    private JsonNode deserializeToJsonNode(SearchHit searchHit) throws IOException {
        return objectMapper.readTree(searchHit.getSourceAsString());
    }

    private RowData convertToRowData(JsonNode document) {
        return (RowData) runtimeConverter.convert(document);
    }
}
