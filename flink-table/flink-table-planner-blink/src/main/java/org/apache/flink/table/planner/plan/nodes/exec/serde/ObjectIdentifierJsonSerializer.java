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

import org.apache.flink.table.catalog.ObjectIdentifier;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/** JSON serializer for {@link ObjectIdentifier}. */
public class ObjectIdentifierJsonSerializer extends StdSerializer<ObjectIdentifier> {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_CATALOG_NAME = "catalogName";
    public static final String FIELD_NAME_DATABASE_NAME = "databaseName";
    public static final String FIELD_NAME_TABLE_NAME = "tableName";

    public ObjectIdentifierJsonSerializer() {
        super(ObjectIdentifier.class);
    }

    @Override
    public void serialize(
            ObjectIdentifier objectIdentifier,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_CATALOG_NAME, objectIdentifier.getCatalogName());
        jsonGenerator.writeStringField(
                FIELD_NAME_DATABASE_NAME, objectIdentifier.getDatabaseName());
        jsonGenerator.writeStringField(FIELD_NAME_TABLE_NAME, objectIdentifier.getObjectName());
        jsonGenerator.writeEndObject();
    }
}
