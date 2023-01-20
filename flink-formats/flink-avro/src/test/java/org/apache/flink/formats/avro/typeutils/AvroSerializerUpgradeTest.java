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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.formats.avro.generated.Address;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/** Tests based on {@link TypeSerializerUpgradeTestBase} for the {@link AvroSerializer}. */
class AvroSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "generic-avro-serializer",
                        flinkVersion,
                        GenericAvroSerializerSetup.class,
                        GenericAvroSerializerVerifier.class));

        testSpecifications.add(
                new TestSpecification<>(
                        "specific-avro-serializer",
                        flinkVersion,
                        SpecificAvroSerializerSetup.class,
                        SpecificAvroSerializerVerifier.class));

        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "generic-avro-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class GenericAvroSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<GenericRecord> {

        @Override
        public TypeSerializer<GenericRecord> createPriorSerializer() {
            return new AvroSerializer<>(GenericRecord.class, Address.getClassSchema());
        }

        @Override
        public GenericRecord createTestData() {
            GenericData.Record record = new GenericData.Record(Address.getClassSchema());
            record.put("num", 239);
            record.put("street", "Baker Street");
            record.put("city", "London");
            record.put("state", "London");
            record.put("zip", "NW1 6XE");
            return record;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class GenericAvroSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<GenericRecord> {

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public TypeSerializer<GenericRecord> createUpgradedSerializer() {
            return new AvroSerializer(GenericRecord.class, Address.getClassSchema());
        }

        @Override
        public Matcher<GenericRecord> testDataMatcher() {
            GenericData.Record record = new GenericData.Record(Address.getClassSchema());
            record.put("num", 239);
            record.put("street", "Baker Street");
            record.put("city", "London");
            record.put("state", "London");
            record.put("zip", "NW1 6XE");
            return is(record);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<GenericRecord>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "specific-avro-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class SpecificAvroSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Address> {

        @Override
        public TypeSerializer<Address> createPriorSerializer() {
            @SuppressWarnings({"unchecked", "rawtypes"})
            AvroSerializer<Address> avroSerializer = new AvroSerializer(Address.class);
            return avroSerializer;
        }

        @Override
        public Address createTestData() {
            Address addr = new Address();
            addr.setNum(239);
            addr.setStreet("Baker Street");
            addr.setCity("London");
            addr.setState("London");
            addr.setZip("NW1 6XE");
            return addr;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class SpecificAvroSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Address> {

        @Override
        public TypeSerializer<Address> createUpgradedSerializer() {
            @SuppressWarnings({"unchecked", "rawtypes"})
            AvroSerializer<Address> avroSerializer = new AvroSerializer(Address.class);
            return avroSerializer;
        }

        @Override
        public Matcher<Address> testDataMatcher() {
            Address addr = new Address();
            addr.setNum(239);
            addr.setStreet("Baker Street");
            addr.setCity("London");
            addr.setState("London");
            addr.setZip("NW1 6XE");
            return is(addr);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Address>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
