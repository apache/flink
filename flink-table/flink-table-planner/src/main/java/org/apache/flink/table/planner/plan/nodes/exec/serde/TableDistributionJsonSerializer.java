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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.TableDistribution;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.serializeOptionalField;

/**
 * JSON serializer for {@link TableDistribution}.
 *
 * @see TableDistributionJsonDeserializer for the reverse operation
 */
@Internal
final class TableDistributionJsonSerializer extends StdSerializer<TableDistribution> {
    private static final long serialVersionUID = 1L;

    static final String KIND = "kind";
    static final String BUCKET_COUNT = "bucketCount";
    static final String BUCKET_KEYS = "bucketKeys";

    TableDistributionJsonSerializer() {
        super(TableDistribution.class);
    }

    @Override
    public void serialize(
            TableDistribution distribution,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        serializerProvider.defaultSerializeField(KIND, distribution.getKind(), jsonGenerator);
        serializeOptionalField(
                jsonGenerator, BUCKET_COUNT, distribution.getBucketCount(), serializerProvider);
        serializerProvider.defaultSerializeField(
                BUCKET_KEYS, distribution.getBucketKeys(), jsonGenerator);

        jsonGenerator.writeEndObject();
    }
}
