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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Abstract test base for type information. */
@ExtendWith(TestLoggerExtension.class)
public abstract class TypeInformationTestBase<T extends TypeInformation<?>> {

    protected abstract T[] getTestData();

    @Test
    void testHashcodeAndEquals() throws Exception {
        final T[] testData = getTestData();
        final TypeInformation<?> unrelatedTypeInfo = new UnrelatedTypeInfo();

        for (T typeInfo : testData) {
            // check for implemented hashCode and equals
            if (typeInfo.getClass().getMethod("hashCode").getDeclaringClass() == Object.class) {
                throw new AssertionError(
                        "Type information does not implement own hashCode method: "
                                + typeInfo.getClass().getCanonicalName());
            }
            if (typeInfo.getClass().getMethod("equals", Object.class).getDeclaringClass()
                    == Object.class) {
                throw new AssertionError(
                        "Type information does not implement own equals method: "
                                + typeInfo.getClass().getCanonicalName());
            }

            // compare among test data
            for (T otherTypeInfo : testData) {
                // test equality
                if (typeInfo == otherTypeInfo) {
                    assertThat(typeInfo.hashCode())
                            .as("hashCode() returns inconsistent results.")
                            .isEqualTo(otherTypeInfo.hashCode());
                    assertThat(typeInfo)
                            .as("equals() is false for same object.")
                            .isEqualTo(otherTypeInfo);
                }
                // test inequality
                else {
                    assertThat(typeInfo)
                            .as("equals() returned true for different objects.")
                            .isNotEqualTo(otherTypeInfo);
                }
            }

            // compare with unrelated type
            assertThat(typeInfo.canEqual(unrelatedTypeInfo))
                    .as("Type information allows to compare with unrelated type.")
                    .isFalse();
            assertThat(typeInfo).isNotEqualTo(unrelatedTypeInfo);
        }
    }

    @Test
    public void testSerialization() {
        final T[] testData = getTestData();

        for (T typeInfo : testData) {
            final byte[] serialized;
            try {
                serialized = InstantiationUtil.serializeObject(typeInfo);
            } catch (IOException e) {
                throw new AssertionError("Could not serialize type information: " + typeInfo, e);
            }
            final T deserialized;
            try {
                deserialized =
                        InstantiationUtil.deserializeObject(
                                serialized, getClass().getClassLoader());
            } catch (IOException | ClassNotFoundException e) {
                throw new AssertionError("Could not deserialize type information: " + typeInfo, e);
            }
            if (typeInfo.hashCode() != deserialized.hashCode() || !typeInfo.equals(deserialized)) {
                throw new AssertionError(
                        "Deserialized type information differs from original one.");
            }
        }
    }

    @Test
    public void testGetTotalFields() {
        final T[] testData = getTestData();
        for (T typeInfo : testData) {
            assertThat(typeInfo.getTotalFields())
                    .as("Number of total fields must be at least 1")
                    .isGreaterThan(0);
        }
    }

    private static class UnrelatedTypeInfo extends TypeInformation<Object> {

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 0;
        }

        @Override
        public int getTotalFields() {
            return 0;
        }

        @Override
        public Class<Object> getTypeClass() {
            return null;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<Object> createSerializer(ExecutionConfig config) {
            return null;
        }

        @Override
        public String toString() {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean canEqual(Object obj) {
            return false;
        }
    }
}
