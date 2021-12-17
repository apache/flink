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

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test suit for {@link StateSerializerProvider}. */
public class StateSerializerProviderTest {

    // --------------------------------------------------------------------------------
    //  Tests for #currentSchemaSerializer()
    // --------------------------------------------------------------------------------

    @Test
    public void testCurrentSchemaSerializerForEagerlyRegisteredStateSerializerProvider() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V1TestTypeSerializer());
        assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
    }

    @Test
    public void testCurrentSchemaSerializerForLazilyRegisteredStateSerializerProvider() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());
        assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
    }

    // --------------------------------------------------------------------------------
    //  Tests for #previousSchemaSerializer()
    // --------------------------------------------------------------------------------

    @Test(expected = UnsupportedOperationException.class)
    public void testPreviousSchemaSerializerForEagerlyRegisteredStateSerializerProvider() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V1TestTypeSerializer());

        // this should fail with an exception
        testProvider.previousSchemaSerializer();
    }

    @Test
    public void testPreviousSchemaSerializerForLazilyRegisteredStateSerializerProvider() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());
        assertTrue(
                testProvider.previousSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
    }

    @Test
    public void testLazyInstantiationOfPreviousSchemaSerializer() {
        // create the provider with an exception throwing snapshot;
        // this would throw an exception if the restore serializer was eagerly accessed
        StateSerializerProvider<String> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        new ExceptionThrowingSerializerSnapshot());

        try {
            // if we fail here, that means the restore serializer was indeed lazily accessed
            testProvider.previousSchemaSerializer();
            fail("expected to fail when accessing the restore serializer.");
        } catch (Exception expected) {
            // success
        }
    }

    // --------------------------------------------------------------------------------
    //  Tests for #registerNewSerializerForRestoredState(TypeSerializer)
    // --------------------------------------------------------------------------------

    @Test(expected = UnsupportedOperationException.class)
    public void testRegisterNewSerializerWithEagerlyRegisteredStateSerializerProviderShouldFail() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V1TestTypeSerializer());
        testProvider.registerNewSerializerForRestoredState(new TestType.V2TestTypeSerializer());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void
            testRegisterNewSerializerTwiceWithLazilyRegisteredStateSerializerProviderShouldFail() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        testProvider.registerNewSerializerForRestoredState(new TestType.V2TestTypeSerializer());

        // second registration should fail
        testProvider.registerNewSerializerForRestoredState(new TestType.V2TestTypeSerializer());
    }

    @Test
    public void testLazilyRegisterNewCompatibleAsIsSerializer() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        // register compatible serializer for state
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.registerNewSerializerForRestoredState(
                        new TestType.V1TestTypeSerializer());
        assertTrue(schemaCompatibility.isCompatibleAsIs());

        assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
        assertTrue(
                testProvider.previousSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
    }

    @Test
    public void testLazilyRegisterNewCompatibleAfterMigrationSerializer() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        // register serializer that requires migration for state
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.registerNewSerializerForRestoredState(
                        new TestType.V2TestTypeSerializer());
        assertTrue(schemaCompatibility.isCompatibleAfterMigration());

        assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V2TestTypeSerializer);
        assertTrue(
                testProvider.previousSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
    }

    @Test
    public void testLazilyRegisterNewSerializerRequiringReconfiguration() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        // register serializer that requires reconfiguration, and verify that
        // the resulting current schema serializer is the reconfigured one
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.registerNewSerializerForRestoredState(
                        new TestType.ReconfigurationRequiringTestTypeSerializer());
        assertTrue(schemaCompatibility.isCompatibleWithReconfiguredSerializer());
        assertTrue(
                testProvider.currentSchemaSerializer().getClass()
                        == TestType.V1TestTypeSerializer.class);
    }

    @Test
    public void testLazilyRegisterIncompatibleSerializer() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        // register serializer that requires migration for state
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.registerNewSerializerForRestoredState(
                        new TestType.IncompatibleTestTypeSerializer());
        assertTrue(schemaCompatibility.isIncompatible());

        try {
            // a serializer for the current schema will no longer be accessible
            testProvider.currentSchemaSerializer();

            fail();
        } catch (Exception excepted) {
            // success
        }
    }

    // --------------------------------------------------------------------------------
    //  Tests for #setPreviousSerializerSnapshotForRestoredState(TypeSerializerSnapshot)
    // --------------------------------------------------------------------------------

    @Test(expected = UnsupportedOperationException.class)
    public void testSetSerializerSnapshotWithLazilyRegisteredSerializerProviderShouldFail() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        serializer.snapshotConfiguration());

        testProvider.setPreviousSerializerSnapshotForRestoredState(
                serializer.snapshotConfiguration());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetSerializerSnapshotTwiceWithEagerlyRegisteredSerializerProviderShouldFail() {
        TestType.V1TestTypeSerializer serializer = new TestType.V1TestTypeSerializer();
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(serializer);

        testProvider.setPreviousSerializerSnapshotForRestoredState(
                serializer.snapshotConfiguration());

        // second registration should fail
        testProvider.setPreviousSerializerSnapshotForRestoredState(
                serializer.snapshotConfiguration());
    }

    @Test
    public void testEagerlyRegisterNewCompatibleAsIsSerializer() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V1TestTypeSerializer());

        // set previous serializer snapshot for state, which should let the new serializer be
        // considered compatible as is
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.setPreviousSerializerSnapshotForRestoredState(
                        new TestType.V1TestTypeSerializer().snapshotConfiguration());
        assertTrue(schemaCompatibility.isCompatibleAsIs());

        assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
        assertTrue(
                testProvider.previousSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
    }

    @Test
    public void testEagerlyRegisterCompatibleAfterMigrationSerializer() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.V2TestTypeSerializer());

        // set previous serializer snapshot for state, which should let the new serializer be
        // considered compatible after migration
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.setPreviousSerializerSnapshotForRestoredState(
                        new TestType.V1TestTypeSerializer().snapshotConfiguration());
        assertTrue(schemaCompatibility.isCompatibleAfterMigration());

        assertTrue(testProvider.currentSchemaSerializer() instanceof TestType.V2TestTypeSerializer);
        assertTrue(
                testProvider.previousSchemaSerializer() instanceof TestType.V1TestTypeSerializer);
    }

    @Test
    public void testEagerlyRegisterNewSerializerRequiringReconfiguration() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.ReconfigurationRequiringTestTypeSerializer());

        // set previous serializer snapshot, which should let the new serializer be considered to
        // require reconfiguration,
        // and verify that the resulting current schema serializer is the reconfigured one
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.setPreviousSerializerSnapshotForRestoredState(
                        new TestType.V1TestTypeSerializer().snapshotConfiguration());
        assertTrue(schemaCompatibility.isCompatibleWithReconfiguredSerializer());
        assertTrue(
                testProvider.currentSchemaSerializer().getClass()
                        == TestType.V1TestTypeSerializer.class);
    }

    @Test
    public void testEagerlyRegisterIncompatibleSerializer() {
        StateSerializerProvider<TestType> testProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(
                        new TestType.IncompatibleTestTypeSerializer());

        // set previous serializer snapshot for state, which should let the new serializer be
        // considered incompatible
        TypeSerializerSchemaCompatibility<TestType> schemaCompatibility =
                testProvider.setPreviousSerializerSnapshotForRestoredState(
                        new TestType.V1TestTypeSerializer().snapshotConfiguration());
        assertTrue(schemaCompatibility.isIncompatible());

        try {
            // a serializer for the current schema will no longer be accessible
            testProvider.currentSchemaSerializer();

            fail();
        } catch (Exception excepted) {
            // success
        }
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
