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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Animal;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Cat;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Dog;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Parrot;

import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.hasSameCompatibilityAs;
import static org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer;
import static org.hamcrest.Matchers.is;

/** Tests migrations for {@link KryoSerializerSnapshot}. */
@SuppressWarnings("WeakerAccess")
class KryoSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "kryo-type-serializer-empty-config",
                        flinkVersion,
                        KryoTypeSerializerEmptyConfigSetup.class,
                        KryoTypeSerializerEmptyConfigVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "kryo-type-serializer-unrelated-config-after-restore",
                        flinkVersion,
                        KryoTypeSerializerEmptyConfigSetup.class,
                        KryoTypeSerializerWithUnrelatedConfigVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "kryo-type-serializer-changed-registration-order",
                        flinkVersion,
                        KryoTypeSerializerChangedRegistrationOrderSetup.class,
                        KryoTypeSerializerChangedRegistrationOrderVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "kryo-custom-type-serializer-changed-registration-order",
                        flinkVersion,
                        KryoCustomTypeSerializerChangedRegistrationOrderSetup.class,
                        KryoCustomTypeSerializerChangedRegistrationOrderVerifier.class));

        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "kryo-type-serializer-empty-config"
    // ----------------------------------------------------------------------------------------------

    public static final class KryoTypeSerializerEmptyConfigSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Animal> {

        @Override
        public TypeSerializer<Animal> createPriorSerializer() {
            return new KryoSerializer<>(Animal.class, new SerializerConfigImpl());
        }

        @Override
        public Animal createTestData() {
            return new Dog("Hasso");
        }
    }

    public static final class KryoTypeSerializerEmptyConfigVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Animal> {

        @Override
        public TypeSerializer<Animal> createUpgradedSerializer() {
            return new KryoSerializer<>(Animal.class, new SerializerConfigImpl());
        }

        @Override
        public Matcher<Animal> testDataMatcher() {
            return is(new Dog("Hasso"));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Animal>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "kryo-type-serializer-empty-config-then-some-config"
    // ----------------------------------------------------------------------------------------------

    public static final class KryoTypeSerializerWithUnrelatedConfigVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Animal> {

        @Override
        public TypeSerializer<Animal> createUpgradedSerializer() {
            SerializerConfigImpl serializerConfigImpl = new SerializerConfigImpl();
            serializerConfigImpl.registerKryoType(DummyClassOne.class);
            serializerConfigImpl.registerTypeWithKryoSerializer(
                    DummyClassTwo.class, DefaultSerializers.StringSerializer.class);

            return new KryoSerializer<>(Animal.class, serializerConfigImpl);
        }

        @Override
        public Matcher<Animal> testDataMatcher() {
            return is(new Dog("Hasso"));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Animal>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return hasSameCompatibilityAs(
                    compatibleWithReconfiguredSerializer(
                            new KryoSerializer<>(Animal.class, new SerializerConfigImpl())));
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "kryo-type-serializer-changed-registration-order"
    // ----------------------------------------------------------------------------------------------

    public static final class KryoTypeSerializerChangedRegistrationOrderSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Animal> {

        @Override
        public TypeSerializer<Animal> createPriorSerializer() {
            SerializerConfigImpl serializerConfigImpl = new SerializerConfigImpl();
            serializerConfigImpl.registerKryoType(Dog.class);
            serializerConfigImpl.registerKryoType(Cat.class);
            serializerConfigImpl.registerKryoType(Parrot.class);

            return new KryoSerializer<>(Animal.class, serializerConfigImpl);
        }

        @Override
        public Animal createTestData() {
            return new Dog("Hasso");
        }
    }

    public static final class KryoTypeSerializerChangedRegistrationOrderVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Animal> {

        @Override
        public TypeSerializer<Animal> createUpgradedSerializer() {
            SerializerConfigImpl serializerConfigImpl = new SerializerConfigImpl();
            serializerConfigImpl.registerKryoType(DummyClassOne.class);
            serializerConfigImpl.registerKryoType(Dog.class);
            serializerConfigImpl.registerKryoType(DummyClassTwo.class);
            serializerConfigImpl.registerKryoType(Cat.class);
            serializerConfigImpl.registerKryoType(Parrot.class);

            return new KryoSerializer<>(Animal.class, serializerConfigImpl);
        }

        @Override
        public Matcher<Animal> testDataMatcher() {
            return is(new Dog("Hasso"));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Animal>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return hasSameCompatibilityAs(
                    compatibleWithReconfiguredSerializer(
                            new KryoSerializer<>(Animal.class, new SerializerConfigImpl())));
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "kryo-custom-type-serializer-changed-registration-order"
    // ----------------------------------------------------------------------------------------------

    public static final class KryoCustomTypeSerializerChangedRegistrationOrderSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Animal> {

        @Override
        public TypeSerializer<Animal> createPriorSerializer() {
            SerializerConfigImpl serializerConfigImpl = new SerializerConfigImpl();
            serializerConfigImpl.registerTypeWithKryoSerializer(
                    Dog.class, KryoPojosForMigrationTests.DogKryoSerializer.class);
            serializerConfigImpl.registerKryoType(Cat.class);
            serializerConfigImpl.registerTypeWithKryoSerializer(
                    Parrot.class, KryoPojosForMigrationTests.ParrotKryoSerializer.class);

            return new KryoSerializer<>(Animal.class, serializerConfigImpl);
        }

        @Override
        public Animal createTestData() {
            return new Dog("Hasso");
        }
    }

    public static final class KryoCustomTypeSerializerChangedRegistrationOrderVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Animal> {

        @Override
        public TypeSerializer<Animal> createUpgradedSerializer() {
            SerializerConfigImpl serializerConfigImpl = new SerializerConfigImpl();
            serializerConfigImpl.registerKryoType(DummyClassOne.class);
            serializerConfigImpl.registerTypeWithKryoSerializer(
                    Dog.class, KryoPojosForMigrationTests.DogV2KryoSerializer.class);
            serializerConfigImpl.registerKryoType(DummyClassTwo.class);
            serializerConfigImpl.registerKryoType(Cat.class);
            serializerConfigImpl.registerTypeWithKryoSerializer(
                    Parrot.class, KryoPojosForMigrationTests.ParrotKryoSerializer.class);

            return new KryoSerializer<>(Animal.class, serializerConfigImpl);
        }

        @Override
        public Matcher<Animal> testDataMatcher() {
            return is(new Dog("Hasso"));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Animal>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return hasSameCompatibilityAs(
                    compatibleWithReconfiguredSerializer(
                            new KryoSerializer<>(Animal.class, new SerializerConfigImpl())));
        }
    }

    /** Dummy class to be registered in the tests. */
    public static final class DummyClassOne {}

    /** Dummy class to be registered in the tests. */
    public static final class DummyClassTwo {}
}
