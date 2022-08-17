/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.table.serialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.connector.mongodb.table.converter.BsonToRowDataConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.bson.BsonDocument;

/** Deserializer that {@link BsonDocument} to flink internal {@link RowData}. */
@Internal
public class MongoRowDataDeserializationSchema implements MongoDeserializationSchema<RowData> {

    /** Type information describing the result type. */
    private final TypeInformation<RowData> typeInfo;

    /** Runtime instance that performs the actual work. */
    private final BsonToRowDataConverters.BsonToRowDataConverter runtimeConverter;

    public MongoRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> typeInfo) {
        this.typeInfo = typeInfo;
        this.runtimeConverter = BsonToRowDataConverters.createNullableConverter(rowType);
    }

    @Override
    public RowData deserialize(BsonDocument document) {
        return (RowData) runtimeConverter.convert(document);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }
}
