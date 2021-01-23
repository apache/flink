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

package org.apache.flink.table.dataview;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link MapViewSerializer}. */
@RunWith(Parameterized.class)
public class MapViewSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<MapView<Integer, String>, MapView<Integer, String>> {

    private static final String SPEC_NAME = "map-view-serializer";

    public MapViewSerializerUpgradeTest(
            TestSpecification<MapView<Integer, String>, MapView<Integer, String>>
                    testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            SPEC_NAME,
                            migrationVersion,
                            MapViewSerializerSetup.class,
                            MapViewSerializerVerifier.class));
        }
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "map-view-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class MapViewSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<MapView<Integer, String>> {
        @Override
        public TypeSerializer<MapView<Integer, String>> createPriorSerializer() {
            return new MapViewSerializer<>(
                    new MapSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE));
        }

        @Override
        public MapView<Integer, String> createTestData() {
            return mockTestData();
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class MapViewSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<MapView<Integer, String>> {
        @Override
        public TypeSerializer<MapView<Integer, String>> createUpgradedSerializer() {
            return new MapViewSerializer<>(
                    new MapSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE));
        }

        @Override
        public Matcher<MapView<Integer, String>> testDataMatcher() {
            return is(mockTestData());
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<MapView<Integer, String>>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    public static MapView<Integer, String> mockTestData() {
        MapView<Integer, String> view =
                new MapView<>(TypeInformation.of(Integer.class), TypeInformation.of(String.class));
        try {
            view.put(1, "1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return view;
    }
}
