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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.DistributionType;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.HashDistribution;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.RequiredDistribution;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/** JSON serializer for {@link RequiredDistribution}. */
public class RequiredDistributionJsonSerializer extends StdSerializer<RequiredDistribution> {
    private static final long serialVersionUID = 1L;

    public RequiredDistributionJsonSerializer() {
        super(RequiredDistribution.class);
    }

    @Override
    public void serialize(
            RequiredDistribution requiredDistribution,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        DistributionType type = requiredDistribution.getType();
        jsonGenerator.writeStringField("type", type.name());
        switch (type) {
            case ANY:
            case SINGLETON:
            case BROADCAST:
            case UNKNOWN:
                // do nothing, type name is enough
                break;
            case HASH:
                HashDistribution hashDistribution = (HashDistribution) requiredDistribution;
                jsonGenerator.writeFieldName("keys");
                jsonGenerator.writeArray(
                        hashDistribution.getKeys(),
                        0, // offset
                        hashDistribution.getKeys().length);
                break;
            default:
                throw new TableException("Unsupported distribution type: " + type);
        }
        jsonGenerator.writeEndObject();
    }
}
