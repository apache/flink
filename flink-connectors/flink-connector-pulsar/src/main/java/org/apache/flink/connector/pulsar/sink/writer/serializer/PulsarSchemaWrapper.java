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

package org.apache.flink.connector.pulsar.sink.writer.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;

import org.apache.pulsar.client.api.Schema;

/** Wrap the Pulsar's Schema into PulsarSerializationSchema. */
@Internal
public class PulsarSchemaWrapper<IN> implements PulsarSerializationSchema<IN> {
    private static final long serialVersionUID = -2567052498398184194L;

    private final PulsarSchema<IN> pulsarSchema;

    public PulsarSchemaWrapper(PulsarSchema<IN> pulsarSchema) {
        this.pulsarSchema = pulsarSchema;
    }

    @Override
    public byte[] serialize(IN element, PulsarSinkContext sinkContext) {
        return schema().encode(element);
    }

    @Override
    public Schema<IN> schema() {
        return pulsarSchema.getPulsarSchema();
    }
}
