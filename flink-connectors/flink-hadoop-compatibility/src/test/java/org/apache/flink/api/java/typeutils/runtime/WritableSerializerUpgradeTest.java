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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.runtime.WritableSerializerUpgradeTest.WritableName;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.apache.hadoop.io.Writable;
import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link WritableSerializer}. */
@RunWith(Parameterized.class)
public class WritableSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<WritableName, WritableName> {

    public WritableSerializerUpgradeTest(
            TestSpecification<WritableName, WritableName> testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            "writeable-serializer",
                            migrationVersion,
                            WritableSerializerSetup.class,
                            WritableSerializerVerifier.class));
        }
        return testSpecifications;
    }

    /** A dummy class that is used in this test. */
    public static final class WritableName implements Writable {

        public static final long serialVersionUID = 1L;

        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(name);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            name = in.readUTF();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (!(obj instanceof WritableName)) {
                return false;
            }

            WritableName other = (WritableName) obj;
            return Objects.equals(name, other.name);
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "writeable-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class WritableSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<WritableName> {
        @Override
        public TypeSerializer<WritableName> createPriorSerializer() {
            return new WritableSerializer<>(WritableName.class);
        }

        @Override
        public WritableName createTestData() {
            WritableName writable = new WritableName();
            writable.setName("flink");
            return writable;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class WritableSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<WritableName> {
        @Override
        public TypeSerializer<WritableName> createUpgradedSerializer() {
            return new WritableSerializer<>(WritableName.class);
        }

        @Override
        public Matcher<WritableName> testDataMatcher() {
            WritableName writable = new WritableName();
            writable.setName("flink");
            return is(writable);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<WritableName>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
