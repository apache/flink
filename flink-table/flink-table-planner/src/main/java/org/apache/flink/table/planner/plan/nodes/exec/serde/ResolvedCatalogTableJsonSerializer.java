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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

class ResolvedCatalogTableJsonSerializer extends StdSerializer<ResolvedCatalogTable> {
    private static final long serialVersionUID = 1L;

    static final String SERIALIZE_OPTIONS = "serialize_options";

    public static final String RESOLVED_SCHEMA = "schema";
    public static final String PARTITION_KEYS = "partitionKeys";
    public static final String OPTIONS = "options";
    public static final String COMMENT = "comment";

    public ResolvedCatalogTableJsonSerializer() {
        super(ResolvedCatalogTable.class);
    }

    @Override
    public void serialize(
            ResolvedCatalogTable resolvedCatalogTable,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        boolean serializeOptions =
                serializerProvider.getAttribute(SERIALIZE_OPTIONS) == null
                        || (boolean) serializerProvider.getAttribute(SERIALIZE_OPTIONS);

        serialize(resolvedCatalogTable, serializeOptions, jsonGenerator, serializerProvider);
    }

    static void serialize(
            ResolvedCatalogTable resolvedCatalogTable,
            boolean serializeOptions,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        // This should never happen anyway, but we keep this assertion for sanity check
        assert resolvedCatalogTable.getTableKind() == CatalogBaseTable.TableKind.TABLE;

        jsonGenerator.writeStartObject();

        if (resolvedCatalogTable.getOrigin() instanceof ExternalCatalogTable) {
            throw new ValidationException(
                    "Cannot serialize the table as it's an external inline table. "
                            + "This might be caused by a usage of "
                            + "StreamTableEnvironment#fromDataStream or TableResult#collect, "
                            + "which are not supported by the persisted plan.");
        }

        serializerProvider.defaultSerializeField(
                RESOLVED_SCHEMA, resolvedCatalogTable.getResolvedSchema(), jsonGenerator);
        jsonGenerator.writeObjectField(PARTITION_KEYS, resolvedCatalogTable.getPartitionKeys());

        if (serializeOptions) {
            if (!resolvedCatalogTable.getComment().isEmpty()) {
                jsonGenerator.writeObjectField(COMMENT, resolvedCatalogTable.getComment());
            }
            jsonGenerator.writeObjectField(OPTIONS, resolvedCatalogTable.getOptions());
        }

        jsonGenerator.writeEndObject();
    }
}
