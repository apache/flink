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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests related to configuration snapshotting and reconfiguring for the {@link KryoSerializer}. */
public class KryoSerializerCompatibilityTest {

    /** Verifies that reconfiguration result is INCOMPATIBLE if data type has changed. */
    @Test
    void testMigrationStrategyWithDifferentKryoType() throws Exception {
        KryoSerializer<TestClassA> kryoSerializerForA =
                new KryoSerializer<>(TestClassA.class, new SerializerConfigImpl());

        // snapshot configuration and serialize to bytes
        TypeSerializerSnapshot kryoSerializerConfigSnapshot =
                kryoSerializerForA.snapshotConfiguration();
        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), kryoSerializerConfigSnapshot);
            serializedConfig = out.toByteArray();
        }

        KryoSerializer<TestClassB> kryoSerializerForB =
                new KryoSerializer<>(TestClassB.class, new SerializerConfigImpl());

        // read configuration again from bytes
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            kryoSerializerConfigSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        @SuppressWarnings("unchecked")
        TypeSerializerSchemaCompatibility<TestClassB> compatResult =
                kryoSerializerForB
                        .snapshotConfiguration()
                        .resolveSchemaCompatibility(kryoSerializerConfigSnapshot);
        assertThat(compatResult.isIncompatible()).isTrue();
    }

    @Test
    void testMigrationOfTypeWithAvroType() throws Exception {

        /*
        When Avro sees the schema "{"type" : "array", "items" : "boolean"}" it will create a field
        of type List<Integer> but the actual type will be GenericData.Array<Integer>. The
        KryoSerializer registers a special Serializer for this type that simply deserializes
        as ArrayList because Kryo cannot handle GenericData.Array well. Before Flink 1.4 Avro
        was always in the classpath but after 1.4 it's only present if the flink-avro jar is
        included. This test verifies that we can still deserialize data written pre-1.4.
        */
        class FakeAvroClass {
            public final List<Integer> array;

            FakeAvroClass(List<Integer> array) {
                this.array = array;
            }
        }

        /*
        // This has to be executed on a pre-1.4 branch to generate the binary blob
        {
        	ExecutionConfig executionConfig = new ExecutionConfig();
        	KryoSerializer<FakeAvroClass> kryoSerializer =
        		new KryoSerializer<>(FakeAvroClass.class, executionConfig);

        	try (
        		FileOutputStream f = new FileOutputStream(
        			"src/test/resources/type-with-avro-serialized-using-kryo");
        		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(f)) {


        		GenericData.Array<Integer> array =
        			new GenericData.Array<>(10, Schema.createArray(Schema.create(Schema.Type.INT)));

        		array.add(10);
        		array.add(20);
        		array.add(30);

        		FakeAvroClass myTestClass = new FakeAvroClass(array);

        		kryoSerializer.serialize(myTestClass, outputView);
        	}
        }
        */

        SerializerConfigImpl serializerConfigImpl = new SerializerConfigImpl();
        KryoSerializer<FakeAvroClass> kryoSerializer =
                new KryoSerializer<>(FakeAvroClass.class, serializerConfigImpl);

        assertThatThrownBy(
                        () -> {
                            try (FileInputStream f =
                                            new FileInputStream(
                                                    "src/test/resources/type-with-avro-serialized-using-kryo");
                                    DataInputViewStreamWrapper inputView =
                                            new DataInputViewStreamWrapper(f)) {
                                kryoSerializer.deserialize(inputView);
                            }
                        })
                .hasMessageContaining("Could not find required Avro dependency");
    }

    @Test
    void testMigrationWithTypeDevoidOfAvroTypes() throws Exception {

        class FakeClass {
            public final List<Integer> array;

            FakeClass(List<Integer> array) {
                this.array = array;
            }
        }

        {
            SerializerConfigImpl serializerConfigImpl = new SerializerConfigImpl();
            KryoSerializer<FakeClass> kryoSerializer =
                    new KryoSerializer<>(FakeClass.class, serializerConfigImpl);

            try (FileInputStream f =
                            new FileInputStream(
                                    "src/test/resources/type-without-avro-serialized-using-kryo");
                    DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(f)) {

                FakeClass myTestClass = kryoSerializer.deserialize(inputView);
                assertThat(myTestClass.array).hasSize(3).containsExactly(10, 20, 30);
            }
        }
    }

    /**
     * Tests that after reconfiguration, registration ids are reconfigured to remain the same as the
     * preceding KryoSerializer.
     */
    @Test
    void testMigrationStrategyForDifferentRegistrationOrder() throws Exception {

        SerializerConfigImpl serializerConfigImpl = new SerializerConfigImpl();
        serializerConfigImpl.registerKryoType(TestClassA.class);
        serializerConfigImpl.registerKryoType(TestClassB.class);

        KryoSerializer<TestClass> kryoSerializer =
                new KryoSerializer<>(TestClass.class, serializerConfigImpl);

        // get original registration ids
        int testClassId = kryoSerializer.getKryo().getRegistration(TestClass.class).getId();
        int testClassAId = kryoSerializer.getKryo().getRegistration(TestClassA.class).getId();
        int testClassBId = kryoSerializer.getKryo().getRegistration(TestClassB.class).getId();

        // snapshot configuration and serialize to bytes
        TypeSerializerSnapshot kryoSerializerConfigSnapshot =
                kryoSerializer.snapshotConfiguration();
        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), kryoSerializerConfigSnapshot);
            serializedConfig = out.toByteArray();
        }

        // use new config and instantiate new KryoSerializer
        serializerConfigImpl = new SerializerConfigImpl();
        serializerConfigImpl.registerKryoType(TestClassB.class); // test with B registered before A
        serializerConfigImpl.registerKryoType(TestClassA.class);

        kryoSerializer = new KryoSerializer<>(TestClass.class, serializerConfigImpl);

        // read configuration from bytes
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            kryoSerializerConfigSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        // reconfigure - check reconfiguration result and that registration id remains the same
        @SuppressWarnings("unchecked")
        TypeSerializerSchemaCompatibility<TestClass> compatResult =
                kryoSerializer
                        .snapshotConfiguration()
                        .resolveSchemaCompatibility(kryoSerializerConfigSnapshot);
        assertThat(compatResult.isCompatibleWithReconfiguredSerializer()).isTrue();

        kryoSerializer = (KryoSerializer<TestClass>) compatResult.getReconfiguredSerializer();
        assertThat(kryoSerializer.getKryo().getRegistration(TestClass.class).getId())
                .isEqualTo(testClassId);
        assertThat(kryoSerializer.getKryo().getRegistration(TestClassA.class).getId())
                .isEqualTo(testClassAId);
        assertThat(kryoSerializer.getKryo().getRegistration(TestClassB.class).getId())
                .isEqualTo(testClassBId);
    }

    private static class TestClass {}

    private static class TestClassA {}

    private static class TestClassB {}

    private static class TestClassBSerializer extends Serializer {
        @Override
        public void write(Kryo kryo, Output output, Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object read(Kryo kryo, Input input, Class aClass) {
            throw new UnsupportedOperationException();
        }
    }
}
