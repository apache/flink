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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TypeSerializerSnapshot} */
class TypeSerializerSnapshotTest {

    @Test
    void testIllegalSchemaCompatibility() {
        TypeSerializerSnapshot<Integer> illegalSnapshot =
                new NotCompletedTypeSerializerSnapshot() {};

        // Should throw UnsupportedOperationException if both two methods are not implemented
        assertThatThrownBy(
                        () ->
                                illegalSnapshot.resolveSchemaCompatibility(
                                        new NotCompletedTypeSerializer()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                illegalSnapshot.resolveSchemaCompatibility(
                                        new NotCompletedTypeSerializer().snapshotConfiguration()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testNewSchemaCompatibility() {
        TypeSerializerSnapshot<Integer> legalSnapshot =
                new NotCompletedTypeSerializerSnapshot() {
                    @Override
                    public TypeSerializerSchemaCompatibility<Integer> resolveSchemaCompatibility(
                            TypeSerializerSnapshot<Integer> oldSerializerSnapshot) {
                        return TypeSerializerSchemaCompatibility.compatibleAsIs();
                    }
                };

        // The result of resolving schema compatibility should always be determined by legalSnapshot
        assertThat(
                        new NotCompletedTypeSerializerSnapshot()
                                .resolveSchemaCompatibility(
                                        new NotCompletedTypeSerializer() {
                                            @Override
                                            public TypeSerializerSnapshot<Integer>
                                                    snapshotConfiguration() {
                                                return legalSnapshot;
                                            }
                                        })
                                .isCompatibleAsIs())
                .isTrue();
        assertThat(
                        legalSnapshot
                                .resolveSchemaCompatibility(
                                        new NotCompletedTypeSerializerSnapshot() {})
                                .isCompatibleAsIs())
                .isTrue();
    }

    @Test
    void testOldSchemaCompatibility() {
        TypeSerializerSnapshot<Integer> legalSnapshot =
                new NotCompletedTypeSerializerSnapshot() {

                    @Override
                    public TypeSerializerSchemaCompatibility<Integer> resolveSchemaCompatibility(
                            TypeSerializer<Integer> newSerializer) {
                        return TypeSerializerSchemaCompatibility.compatibleAsIs();
                    }
                };

        // The result of resolving schema compatibility should always be determined by legalSnapshot
        assertThat(
                        legalSnapshot
                                .resolveSchemaCompatibility(new NotCompletedTypeSerializer())
                                .isCompatibleAsIs())
                .isTrue();
        assertThat(
                        new NotCompletedTypeSerializerSnapshot()
                                .resolveSchemaCompatibility(legalSnapshot)
                                .isCompatibleAsIs())
                .isTrue();
    }

    @Test
    void testNestedSchemaCompatibility() {
        TypeSerializerSnapshot<Integer> innerSnapshot =
                new NotCompletedTypeSerializerSnapshot() {
                    @Override
                    public TypeSerializerSchemaCompatibility<Integer> resolveSchemaCompatibility(
                            TypeSerializerSnapshot<Integer> oldSerializerSnapshot) {
                        return TypeSerializerSchemaCompatibility.compatibleAsIs();
                    }
                };

        TypeSerializerSnapshot<Integer> outerSnapshot =
                new NotCompletedTypeSerializerSnapshot() {
                    @Override
                    public TypeSerializerSchemaCompatibility<Integer> resolveSchemaCompatibility(
                            TypeSerializer<Integer> newSerializer) {
                        return innerSnapshot.resolveSchemaCompatibility(
                                innerSnapshot.restoreSerializer());
                    }
                };

        // The result of resolving schema compatibility should be determined by the new method of
        // innerSnapshot
        assertThat(outerSnapshot.resolveSchemaCompatibility(outerSnapshot).isCompatibleAsIs())
                .isTrue();
    }

    private static class NotCompletedTypeSerializer extends TypeSerializer<Integer> {

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<Integer> duplicate() {
            return this;
        }

        @Override
        public Integer createInstance() {
            return 0;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 1;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) {
            // do nothing
        }

        @Override
        public Integer deserialize(DataInputView source) {
            return 0;
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) {
            return reuse;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) {
            // do nothing
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
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return new NotCompletedTypeSerializerSnapshot() {
                @Override
                public TypeSerializer<Integer> restoreSerializer() {
                    return NotCompletedTypeSerializer.this;
                }
            };
        }
    }

    private static class NotCompletedTypeSerializerSnapshot
            implements TypeSerializerSnapshot<Integer> {

        @Override
        public int getCurrentVersion() {
            return 0;
        }

        @Override
        public void writeSnapshot(DataOutputView out) {
            // do nothing
        }

        @Override
        public void readSnapshot(
                int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
            // do nothing
        }

        @Override
        public TypeSerializer<Integer> restoreSerializer() {
            return new NotCompletedTypeSerializer() {
                @Override
                public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
                    return NotCompletedTypeSerializerSnapshot.this;
                }
            };
        }
    }
}
