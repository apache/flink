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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import org.assertj.core.api.Condition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/** State migration test for {@link RowSerializer}. */
class ValueSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<
                ValueSerializerUpgradeTest.NameValue, ValueSerializerUpgradeTest.NameValue> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "value-serializer",
                        flinkVersion,
                        ValueSerializerSetup.class,
                        ValueSerializerVerifier.class));

        return testSpecifications;
    }

    public static final class ValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<NameValue> {
        @Override
        public TypeSerializer<NameValue> createPriorSerializer() {
            return new ValueSerializer<>(NameValue.class);
        }

        @Override
        public NameValue createTestData() {
            NameValue value = new NameValue();
            value.setName("klion26");
            return value;
        }
    }

    public static final class ValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<NameValue> {
        @Override
        public TypeSerializer<NameValue> createUpgradedSerializer() {
            return new ValueSerializer<>(NameValue.class);
        }

        @Override
        public Condition<NameValue> testDataCondition() {
            NameValue value = new NameValue();
            value.setName("klion26");
            return new Condition<>(value::equals, "value is klion26");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<NameValue>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    /** A dummy class used for this test. */
    public static final class NameValue implements Value {

        public static final long serialVersionUID = 2277251654485371327L;

        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeUTF(name);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            name = in.readUTF();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof NameValue)) {
                return false;
            }

            NameValue other = (NameValue) obj;
            return Objects.equals(this.name, other.name);
        }
    }
}
