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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link RowSerializer}. */
@VisibleForTesting
public class RowSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Row, Row> {

    @Override
    public Collection<FlinkVersion> getMigrationVersions() {
        // for RowSerializer we also test against 1.10 and newer because we have snapshots
        // for this which go beyond what we have for the usual subclasses of
        // TypeSerializerUpgradeTestBase
        List<FlinkVersion> testVersions = new ArrayList<>();
        testVersions.add(FlinkVersion.v1_10);
        testVersions.addAll(super.getMigrationVersions());
        return testVersions;
    }

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        return Collections.singletonList(
                new TestSpecification<>(
                        "row-serializer",
                        flinkVersion,
                        RowSerializerSetup.class,
                        RowSerializerVerifier.class));
    }

    public static TypeSerializer<Row> createRowSerializer() {
        // in older branches, this writes in old format WITHOUT row kind;
        // in newer branches >= 1.11, this writes in new format WITH row kind
        final RowTypeInfo rowTypeInfo =
                new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        return rowTypeInfo.createSerializer(new ExecutionConfig());
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "row-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class RowSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Row> {
        @Override
        public TypeSerializer<Row> createPriorSerializer() {
            return createRowSerializer();
        }

        @Override
        public Row createTestData() {
            Row row = new Row(4);
            row.setField(0, null);
            row.setField(1, 42L);
            row.setField(2, "My string.");
            row.setField(3, null);
            return row;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class RowSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Row> {
        @Override
        public TypeSerializer<Row> createUpgradedSerializer() {
            return createRowSerializer();
        }

        @Override
        public Matcher<Row> testDataMatcher() {
            Row row = new Row(RowKind.INSERT, 4);
            row.setField(0, null);
            row.setField(1, 42L);
            row.setField(2, "My string.");
            row.setField(3, null);
            return is(row);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Row>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            if (version.isNewerVersionThan(FlinkVersion.v1_10)) {
                return TypeSerializerMatchers.isCompatibleAsIs();
            }
            return TypeSerializerMatchers.isCompatibleAfterMigration();
        }
    }
}
