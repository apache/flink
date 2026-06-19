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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnumSerializerCompatibilityTest {

    @TempDir private static Path tempDir;
    private static final String ENUM_NAME = "EnumSerializerUpgradeTestEnum";

    private static final String ENUM_A = "public enum " + ENUM_NAME + " { A, B, C }";
    private static final String ENUM_B = "public enum " + ENUM_NAME + " { A, B, C, D }";
    private static final String ENUM_C = "public enum " + ENUM_NAME + " { A, C }";
    private static final String ENUM_D = "public enum " + ENUM_NAME + " { A, C, B }";

    /** Check that identical enums don't require migration */
    @Test
    void checkIndenticalEnums() throws Exception {
        assertThat(checkCompatibility(ENUM_A, ENUM_A).isCompatibleAsIs()).isTrue();
    }

    /** Check that appending fields to the enum does not require migration */
    @Test
    void checkAppendedField() throws Exception {
        assertThat(checkCompatibility(ENUM_A, ENUM_B).isCompatibleWithReconfiguredSerializer())
                .isTrue();
    }

    /** Check that removing enum fields makes the snapshot incompatible */
    @Test
    void removingFieldShouldBeIncompatible() throws Exception {
        assertThatThrownBy(() -> checkCompatibility(ENUM_B, ENUM_A))
                .isInstanceOf(IllegalStateException.class);
    }

    /** Check that changing the enum field order don't require migration */
    @Test
    void checkDifferentFieldOrder() throws Exception {
        assertThat(checkCompatibility(ENUM_A, ENUM_D).isCompatibleWithReconfiguredSerializer())
                .isTrue();
    }

    @SuppressWarnings("unchecked")
    private static TypeSerializerSchemaCompatibility checkCompatibility(
            String enumSourceA, String enumSourceB) throws IOException, ClassNotFoundException {

        ClassLoader classLoader =
                ClassLoaderUtils.compileAndLoadJava(
                        TempDirUtils.newFolder(tempDir), ENUM_NAME + ".java", enumSourceA);

        EnumSerializer enumSerializer = new EnumSerializer(classLoader.loadClass(ENUM_NAME));

        TypeSerializerSnapshot snapshot = enumSerializer.snapshotConfiguration();
        byte[] snapshotBytes;
        try (ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper outputViewStreamWrapper =
                        new DataOutputViewStreamWrapper(outBuffer)) {

            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    outputViewStreamWrapper, snapshot);
            snapshotBytes = outBuffer.toByteArray();
        }

        ClassLoader classLoader2 =
                ClassLoaderUtils.compileAndLoadJava(
                        TempDirUtils.newFolder(tempDir), ENUM_NAME + ".java", enumSourceB);

        TypeSerializerSnapshot restoredSnapshot;
        try (ByteArrayInputStream inBuffer = new ByteArrayInputStream(snapshotBytes);
                DataInputViewStreamWrapper inputViewStreamWrapper =
                        new DataInputViewStreamWrapper(inBuffer)) {

            restoredSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            inputViewStreamWrapper, classLoader2);
        }

        EnumSerializer enumSerializer2 = new EnumSerializer(classLoader2.loadClass(ENUM_NAME));
        return enumSerializer2.snapshotConfiguration().resolveSchemaCompatibility(restoredSnapshot);
    }
}
