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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.testutils.statemigration.TestType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test suit for {@link StateSerializerProvider}. */
class StateSerializerProviderTest {

    // --------------------------------------------------------------------------------
    //  Tests for #currentSchemaSerializer()
    // --------------------------------------------------------------------------------

    @Test
    void testCurrentSchemaSerializerForEagerlyRegisteredStateSerializerProvider() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V1TestTypeSerializer());
        assertThat(testProvider.currentSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    @Test
    void testCurrentSchemaSerializerForLazilyRegisteredStateSerializerProvider() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());
        assertThat(testProvider.currentSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    // --------------------------------------------------------------------------------
    //  Tests for #previousSchemaSerializer()
    // --------------------------------------------------------------------------------

    @Test
    void testPreviousSchemaSerializerForEagerlyRegisteredStateSerializerProvider() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V1TestTypeSerializer());

        // this should fail with an exception
        assertThatThrownBy(testProvider::previousSchemaSerializer)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testPreviousSchemaSerializerForLazilyRegisteredStateSerializerProvider() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());
        assertThat(testProvider.previousSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    @Test
    void testLazyInstantiationOfPreviousSchemaSerializer() {
        // create the provider with an exception throwing snapshot;
        // this would throw an exception if the restore serializer was eagerly accessed
        StateSerializerProvider<String> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        new ExceptionThrowingSerializerSnapshot());

        // if we fail here, that means the restore serializer was indeed lazily accessed
        assertThatThrownBy(testProvider::previousSchemaSerializer)
                .withFailMessage("expected to fail when accessing the restore serializer.")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // --------------------------------------------------------------------------------
    //  Tests for #registerNewSerializerForRestoredState(TypeSerializer)
    // --------------------------------------------------------------------------------

    @Test
    void testRegisterNewSerializerWithEagerlyRegisteredStateSerializerProviderShouldFail() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V1TestTypeSerializer());
        assertThatThrownBy(
                        () ->
                                testProvider.registerNewSerializerForRestoredState(
                                        new TestType.V2TestTypeSerializer()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRegisterNewSerializerTwiceWithLazilyRegisteredStateSerializerProviderShouldFail() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        testProvider.registerNewSerializerForRestoredState(new TestType.V2TestTypeSerializer());

        // second registration should fail
        assertThatThrownBy(
                        () ->
                                testProvider.registerNewSerializerForRestoredState(
                                        new TestType.V2TestTypeSerializer()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testLazilyRegisterNewCompatibleAsIsSerializer() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        // register compatible serializer for state
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.registerNewSerializerForRestoredState(
                        new TestType.V1TestTypeSerializer());
        assertThat(schemaCompatibility.isCompatibleAsIs()).isTrue();

        assertThat(testProvider.currentSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
        assertThat(testProvider.previousSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    @Test
    void testLazilyRegisterNewCompatibleAfterMigrationSerializer() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        // register serializer that requires migration for state
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.registerNewSerializerForRestoredState(
                        new TestType.V2TestTypeSerializer());
        assertThat(schemaCompatibility.isCompatibleAfterMigration()).isTrue();

        assertThat(testProvider.currentSchemaSerializer())
                .isInstanceOf(TestType.V2TestTypeSerializer.class);
        assertThat(testProvider.previousSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    @Test
    void testLazilyRegisterNewSerializerRequiringReconfiguration() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        // register serializer that requires reconfiguration, and verify that
        // the resulting current schema serializer is the reconfigured one
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.registerNewSerializerForRestoredState(
                        new TestType.ReconfigurationRequiringTestTypeSerializer());
        assertThat(schemaCompatibility.isCompatibleWithReconfiguredSerializer()).isTrue();
        assertThat(testProvider.currentSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    @Test
    void testLazilyRegisterIncompatibleSerializer() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        // register serializer that requires migration for state
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.registerNewSerializerForRestoredState(
                        new TestType.IncompatibleTestTypeSerializer());
        assertThat(schemaCompatibility.isIncompatible()).isTrue();

        // a serializer for the current schema will no longer be accessible
        assertThatThrownBy(testProvider::currentSchemaSerializer)
                .isInstanceOf(IllegalStateException.class);
    }

    // --------------------------------------------------------------------------------
    //  Tests for #setPreviousSerializerSnapshotForRestoredState(TypeSerializerSnapshot)
    // --------------------------------------------------------------------------------

    @Test
    void testSetSerializerSnapshotWithLazilyRegisteredSerializerProviderShouldFail() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        assertThatThrownBy(
                        () ->
                                testProvider.setPreviousSerializerSnapshotForRestoredState(
                                        serializer.snapshotConfiguration()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSetSerializerSnapshotTwiceWithEagerlyRegisteredSerializerProviderShouldFail() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(serializer);

        testProvider.setPreviousSerializerSnapshotForRestoredState(
                serializer.snapshotConfiguration());

        // second registration should fail
        assertThatThrownBy(
                        () ->
                                testProvider.setPreviousSerializerSnapshotForRestoredState(
                                        serializer.snapshotConfiguration()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testEagerlyRegisterNewCompatibleAsIsSerializer() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V1TestTypeSerializer());

        // set previous serializer snapshot for state, which should let the new serializer be
        // considered compatible as is
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.setPreviousSerializerSnapshotForRestoredState(
                        new TestType.V1TestTypeSerializer().snapshotConfiguration());
        assertThat(schemaCompatibility.isCompatibleAsIs()).isTrue();

        assertThat(testProvider.currentSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
        assertThat(testProvider.previousSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    @Test
    void testEagerlyRegisterCompatibleAfterMigrationSerializer() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V2TestTypeSerializer());

        // set previous serializer snapshot for state, which should let the new serializer be
        // considered compatible after migration
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.setPreviousSerializerSnapshotForRestoredState(
                        new TestType.V1TestTypeSerializer().snapshotConfiguration());
        assertThat(schemaCompatibility.isCompatibleAfterMigration()).isTrue();

        assertThat(testProvider.currentSchemaSerializer())
                .isInstanceOf(TestType.V2TestTypeSerializer.class);
        assertThat(testProvider.previousSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    @Test
    void testEagerlyRegisterNewSerializerRequiringReconfiguration() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.ReconfigurationRequiringTestTypeSerializer());

        // set previous serializer snapshot, which should let the new serializer be considered to
        // require reconfiguration,
        // and verify that the resulting current schema serializer is the reconfigured one
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.setPreviousSerializerSnapshotForRestoredState(
                        new TestType.V1TestTypeSerializer().snapshotConfiguration());
        assertThat(schemaCompatibility.isCompatibleWithReconfiguredSerializer()).isTrue();
        assertThat(testProvider.currentSchemaSerializer())
                .isInstanceOf(TestType.V1TestTypeSerializer.class);
    }

    @Test
    void testEagerlyRegisterIncompatibleSerializer() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.IncompatibleTestTypeSerializer());

        // set previous serializer snapshot for state, which should let the new serializer be
        // considered incompatible
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.setPreviousSerializerSnapshotForRestoredState(
                        new TestType.V1TestTypeSerializer().snapshotConfiguration());
        assertThat(schemaCompatibility.isIncompatible()).isTrue();

        // a serializer for the current schema will no longer be accessible
        assertThatThrownBy(testProvider::currentSchemaSerializer)
                .isInstanceOf(IllegalStateException.class);
    }

    // --------------------------------------------------------------------------------
    //  Utilities
    // --------------------------------------------------------------------------------

    public static class ExceptionThrowingSerializerSnapshot
            implements TypeSerializerSnapshot<String> {

        @Override
        public TypeSerializer<String> restoreSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeSerializerSchemaCompatibility<String> resolveSchemaCompatibility(
                TypeSerializer<String> newSerializer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getCurrentVersion() {
            throw new UnsupportedOperationException();
        }
    }
}
