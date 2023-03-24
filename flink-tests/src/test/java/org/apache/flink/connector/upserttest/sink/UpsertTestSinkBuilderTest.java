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
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link UpsertTestSinkBuilder}. */
@ExtendWith(TestLoggerExtension.class)
class UpsertTestSinkBuilderTest {

    @TempDir static File tempFile;

    @Test
    void testValidBuilder() {
        SerializationSchema<String> dummySerializationSchema = element -> null;
        UpsertTestSinkBuilder<String> builder =
                new UpsertTestSinkBuilder<String>()
                        .setOutputFile(tempFile)
                        .setKeySerializationSchema(dummySerializationSchema)
                        .setValueSerializationSchema(dummySerializationSchema);

        assertThatNoException().isThrownBy(builder::build);
    }

    @Test
    void testThrowIfFileNotSet() {
        SerializationSchema<String> dummySerializationSchema = element -> null;
        UpsertTestSinkBuilder<String> builder =
                new UpsertTestSinkBuilder<String>()
                        .setKeySerializationSchema(dummySerializationSchema)
                        .setValueSerializationSchema(dummySerializationSchema);

        assertThatThrownBy(builder::build).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowIfKeySerializerNotSet() {
        SerializationSchema<String> dummySerializationSchema = element -> null;
        UpsertTestSinkBuilder<String> builder =
                new UpsertTestSinkBuilder<String>()
                        .setOutputFile(tempFile)
                        .setValueSerializationSchema(dummySerializationSchema);

        assertThatThrownBy(builder::build).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testThrowIfValueSerializerNotSet() {
        SerializationSchema<String> dummySerializationSchema = element -> null;
        UpsertTestSinkBuilder<String> builder =
                new UpsertTestSinkBuilder<String>()
                        .setOutputFile(tempFile)
                        .setValueSerializationSchema(dummySerializationSchema);

        assertThatThrownBy(builder::build).isInstanceOf(NullPointerException.class);
    }
}
