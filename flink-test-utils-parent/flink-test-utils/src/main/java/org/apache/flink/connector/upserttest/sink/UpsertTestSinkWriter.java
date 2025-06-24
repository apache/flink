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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for writing records into a file in an upsert-fashion. On every
 * checkpoint each key-value pair currently in the map is written to the file.
 *
 * @param <IN> The type of the input elements.
 */
class UpsertTestSinkWriter<IN> implements SinkWriter<IN> {

    private final SerializationSchema<IN> keySerializationSchema;
    private final SerializationSchema<IN> valueSerializationSchema;
    private final Map<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper> records =
            new HashMap<>();
    private final BufferedOutputStream bufferedOutputStream;

    UpsertTestSinkWriter(
            File outputFile,
            SerializationSchema<IN> keySerializationSchema,
            SerializationSchema<IN> valueSerializationSchema) {
        this.keySerializationSchema = checkNotNull(keySerializationSchema);
        this.valueSerializationSchema = checkNotNull(valueSerializationSchema);
        checkNotNull(outputFile);
        try {
            Files.createDirectories(outputFile.toPath().getParent());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not parent directories for path: " + outputFile);
        }
        try {
            this.bufferedOutputStream =
                    new BufferedOutputStream(new FileOutputStream(outputFile, true));
        } catch (FileNotFoundException e) {
            throw new FlinkRuntimeException("Could not find file", e);
        }
    }

    @Override
    public void write(IN element, Context context) {
        byte[] key = keySerializationSchema.serialize(element);
        byte[] value = valueSerializationSchema.serialize(element);
        records.put(new ImmutableByteArrayWrapper(key), new ImmutableByteArrayWrapper(value));
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        UpsertTestFileUtil.writeRecords(bufferedOutputStream, records);
        records.clear();
    }

    @Override
    public void close() throws Exception {
        flush(true);
        bufferedOutputStream.close();
    }
}
