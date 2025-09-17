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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.ClassRelocator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.BitmapSerializer.BitmapSerializerSnapshot;
import org.apache.flink.test.util.MigrationTest;
import org.apache.flink.types.bitmap.Bitmap;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Disabled;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link BitmapSerializerSnapshot}. The test is
 * disabled because bitmap is introduced in Flink 2.3. We should restore the test when there is a
 * Flink 2.4 which should test compatibility with Flink 2.3.
 */
@Disabled
class BitmapSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Bitmap, Bitmap> {

    private static final String SPEC_NAME = "bitmap-serializer";

    @Override
    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion currentVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        currentVersion,
                        BitmapSerializerSetup.class,
                        BitmapSerializerVerifier.class));

        return testSpecifications;
    }

    @Override
    public Collection<FlinkVersion> getMigrationVersions() {
        return FlinkVersion.rangeOf(
                FlinkVersion.v2_3, MigrationTest.getMostRecentlyPublishedVersion());
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "bitmap-serializer"
    // ----------------------------------------------------------------------------------------------

    /** This class is only public to work with {@link ClassRelocator}. */
    public static final class BitmapSerializerSetup implements PreUpgradeSetup<Bitmap> {
        @Override
        public TypeSerializer<Bitmap> createPriorSerializer() {
            return BitmapSerializer.INSTANCE;
        }

        @Override
        public Bitmap createTestData() {
            Bitmap bitmap = Bitmap.empty();
            bitmap.add(1);
            bitmap.add(Integer.MAX_VALUE);
            return bitmap;
        }
    }

    /** This class is only public to work with {@link ClassRelocator}. */
    public static final class BitmapSerializerVerifier implements UpgradeVerifier<Bitmap> {
        @Override
        public TypeSerializer<Bitmap> createUpgradedSerializer() {
            return BitmapSerializer.INSTANCE;
        }

        @Override
        public Condition<Bitmap> testDataCondition() {
            Bitmap bitmap = Bitmap.empty();
            bitmap.add(1);
            bitmap.add(Integer.MAX_VALUE);
            return new Condition<>(bitmap::equals, "value is " + bitmap);
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Bitmap>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
