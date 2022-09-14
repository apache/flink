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

package org.apache.flink.connector.upserttest.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.File;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink Sink to upsert test data into a file. This Sink is intended for testing internal
 * functionality and is **not** production-ready.
 *
 * <p>Please note that the UpsertTestSink needs to run with a parallelism of 1 to function
 * correctly. There is currently no support for using multiple writers at once.
 *
 * @param <IN> type of records written to the file
 * @see UpsertTestSinkBuilder on how to construct an UpsertTestSink
 */
@PublicEvolving
public class UpsertTestSink<IN> implements Sink<IN> {

    private final File outputFile;
    private final SerializationSchema<IN> keySerializationSchema;
    private final SerializationSchema<IN> valueSerializationSchema;

    UpsertTestSink(
            File outputFile,
            SerializationSchema<IN> keySerializationSchema,
            SerializationSchema<IN> valueSerializationSchema) {
        this.outputFile = checkNotNull(outputFile);
        this.keySerializationSchema = checkNotNull(keySerializationSchema);
        this.valueSerializationSchema = checkNotNull(valueSerializationSchema);
    }

    /**
     * Create a {@link UpsertTestSinkBuilder} to construct a new {@link UpsertTestSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link UpsertTestSinkBuilder}
     */
    public static <IN> UpsertTestSinkBuilder<IN> builder() {
        return new UpsertTestSinkBuilder<>();
    }

    @Internal
    @Override
    public SinkWriter<IN> createWriter(InitContext context) {
        try {
            keySerializationSchema.open(context.asSerializationSchemaInitializationContext());
            valueSerializationSchema.open(context.asSerializationSchemaInitializationContext());
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to initialize schema.", e);
        }

        return new UpsertTestSinkWriter<>(
                outputFile, keySerializationSchema, valueSerializationSchema);
    }
}
