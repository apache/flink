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

package org.apache.flink.table.api.runtime.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FlinkScalaKryoInstantiator}.
 *
 * <p>Verifies that the Kryo instance created by {@link FlinkScalaKryoInstantiator} uses {@link
 * DefaultInstantiatorStrategy} as the primary strategy (which invokes no-arg constructors via
 * reflection), with {@link StdInstantiatorStrategy} as a fallback (which uses Objenesis to bypass
 * constructors).
 *
 * <p>This is critical because using a pure {@link StdInstantiatorStrategy} bypasses all
 * constructors, leaving fields uninitialized (null) for classes that rely on constructor
 * initialization (e.g., Iceberg's SerializableByteBufferMap).
 */
class FlinkScalaKryoInstantiatorTest {

    private Kryo kryo;

    @BeforeEach
    void setUp() {
        FlinkScalaKryoInstantiator instantiator = new FlinkScalaKryoInstantiator();
        kryo = instantiator.newKryo();
    }

    /** Verifies that the primary InstantiatorStrategy is DefaultInstantiatorStrategy. */
    @Test
    void testInstantiatorStrategyIsDefaultWithFallback() {
        assertThat(kryo.getInstantiatorStrategy()).isInstanceOf(DefaultInstantiatorStrategy.class);

        DefaultInstantiatorStrategy strategy =
                (DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy();

        assertThat(strategy.getFallbackInstantiatorStrategy())
                .isInstanceOf(StdInstantiatorStrategy.class);
    }

    /**
     * Verifies that a class with a no-arg constructor that initializes fields is properly
     * instantiated by Kryo (constructor is called, fields are initialized).
     *
     * <p>This test simulates the real-world scenario where Iceberg's SerializableByteBufferMap
     * initializes its internal 'wrapped' map in the constructor. Without the fix, the pure
     * StdInstantiatorStrategy would bypass the constructor, leaving 'wrapped' as null, causing NPE
     * during deserialization when MapSerializer tries to call map.put().
     */
    @Test
    void testClassWithNoArgConstructorIsProperlyInitialized() {
        // Verify that newInstance invokes the constructor
        PojoWithConstructorInit instance = kryo.newInstance(PojoWithConstructorInit.class);
        assertThat(instance).isNotNull();
        assertThat(instance.getInternalMap())
                .as(
                        "The internal map should be initialized by the constructor, "
                                + "not null (which would happen if the constructor was bypassed)")
                .isNotNull();
        assertThat(instance.getInitFlag())
                .as("The init flag should be set to true by the constructor")
                .isTrue();
    }

    /**
     * Verifies that Kryo serialization round-trip works correctly for a class with a Map field
     * initialized in the constructor.
     *
     * <p>This is the end-to-end test that reproduces the exact failure scenario: serialize an
     * object with map entries, then deserialize it. Without the fix, MapSerializer.read() would
     * call map.put() on a null map, causing NPE.
     */
    @Test
    void testSerializationRoundTripWithMapField() {
        PojoWithConstructorInit original = new PojoWithConstructorInit();
        original.getInternalMap().put("key1", "value1");
        original.getInternalMap().put("key2", "value2");

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, original);
        output.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Input input = new Input(bais);
        Object deserialized = kryo.readClassAndObject(input);
        input.close();

        assertThat(deserialized).isInstanceOf(PojoWithConstructorInit.class);
        PojoWithConstructorInit result = (PojoWithConstructorInit) deserialized;
        assertThat(result.getInternalMap()).as("Deserialized map should not be null").isNotNull();
        assertThat(result.getInternalMap())
                .as("Deserialized map should contain the same entries")
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2")
                .hasSize(2);
    }

    /**
     * Verifies that classes without a no-arg constructor can still be instantiated via the
     * Objenesis fallback strategy.
     */
    @Test
    void testClassWithoutNoArgConstructorUsesObjenesisFallback() {
        PojoWithoutNoArgConstructor instance = kryo.newInstance(PojoWithoutNoArgConstructor.class);
        // Objenesis bypasses the constructor, so the object is created but fields are
        // uninitialized.
        // This is expected and correct behavior for classes without no-arg constructors.
        assertThat(instance).isNotNull();
    }

    /**
     * Verifies that the KryoSerializer (which loads FlinkScalaKryoInstantiator via reflection when
     * it's on the classpath) produces a Kryo instance with the correct InstantiatorStrategy.
     */
    @Test
    void testKryoSerializerUsesCorrectStrategy() {
        // When FlinkScalaKryoInstantiator is on the classpath (which it is in this test module),
        // KryoSerializer.getKryoInstance() will use it to create the Kryo instance.
        org.apache.flink.api.common.serialization.SerializerConfigImpl config =
                new org.apache.flink.api.common.serialization.SerializerConfigImpl();
        org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<PojoWithConstructorInit>
                kryoSerializer =
                        new org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<>(
                                PojoWithConstructorInit.class, config);
        Kryo kryoFromSerializer = kryoSerializer.getKryo();

        assertThat(kryoFromSerializer.getInstantiatorStrategy())
                .isInstanceOf(DefaultInstantiatorStrategy.class);

        DefaultInstantiatorStrategy strategy =
                (DefaultInstantiatorStrategy) kryoFromSerializer.getInstantiatorStrategy();
        assertThat(strategy.getFallbackInstantiatorStrategy())
                .isInstanceOf(StdInstantiatorStrategy.class);
    }

    // -------------------------------------------------------------------------
    // Test helper classes
    // -------------------------------------------------------------------------

    /**
     * A simple POJO that initializes an internal Map in its no-arg constructor. This simulates
     * classes like Iceberg's SerializableByteBufferMap.
     */
    public static class PojoWithConstructorInit implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Map<String, String> internalMap;
        private final boolean initFlag;

        public PojoWithConstructorInit() {
            this.internalMap = new HashMap<>();
            this.initFlag = true;
        }

        public Map<String, String> getInternalMap() {
            return internalMap;
        }

        public boolean getInitFlag() {
            return initFlag;
        }
    }

    /**
     * A class without a no-arg constructor. Kryo should fall back to Objenesis
     * (StdInstantiatorStrategy) for this class.
     */
    public static class PojoWithoutNoArgConstructor implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String value;

        public PojoWithoutNoArgConstructor(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
