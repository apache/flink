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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.File;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder to construct {@link UpsertTestSink}.
 *
 * <p>The following example shows the minimum setup to create a UpsertTestSink that writes {@code
 * Tuple2<String, String>} values to a file.
 *
 * <pre>{@code
 * UpsertTestSink<Tuple2<String, String>> sink = UpsertTestSink
 *     .<Tuple2<String, String>>builder
 *     .setOutputFile(MY_OUTPUT_FILE)
 *     .setKeySerializationSchema(MY_KEY_SERIALIZER)
 *     .setValueSerializationSchema(MY_VALUE_SERIALIZER)
 *     .build();
 * }</pre>
 *
 * @param <IN> type of the records written to the file
 */
@PublicEvolving
public class UpsertTestSinkBuilder<IN> {

    private File outputFile;
    private SerializationSchema<IN> keySerializationSchema;
    private SerializationSchema<IN> valueSerializationSchema;

    /**
     * Sets the output {@link File} to write to.
     *
     * @param outputFile
     * @return {@link UpsertTestSinkBuilder}
     */
    public UpsertTestSinkBuilder<IN> setOutputFile(File outputFile) {
        this.outputFile = checkNotNull(outputFile);
        return this;
    }

    /**
     * Sets the key {@link SerializationSchema} that transforms incoming records to byte[].
     *
     * @param keySerializationSchema
     * @return {@link UpsertTestSinkBuilder}
     */
    public UpsertTestSinkBuilder<IN> setKeySerializationSchema(
            SerializationSchema<IN> keySerializationSchema) {
        this.keySerializationSchema = checkNotNull(keySerializationSchema);
        return this;
    }

    /**
     * Sets the value {@link SerializationSchema} that transforms incoming records to byte[].
     *
     * @param valueSerializationSchema
     * @return {@link UpsertTestSinkBuilder}
     */
    public UpsertTestSinkBuilder<IN> setValueSerializationSchema(
            SerializationSchema<IN> valueSerializationSchema) {
        this.valueSerializationSchema = checkNotNull(valueSerializationSchema);
        return this;
    }

    /**
     * Constructs the {@link UpsertTestSink} with the configured properties.
     *
     * @return {@link UpsertTestSink}
     */
    public UpsertTestSink<IN> build() {
        checkNotNull(outputFile);
        checkNotNull(keySerializationSchema);
        checkNotNull(valueSerializationSchema);
        return new UpsertTestSink<>(outputFile, keySerializationSchema, valueSerializationSchema);
    }
}
