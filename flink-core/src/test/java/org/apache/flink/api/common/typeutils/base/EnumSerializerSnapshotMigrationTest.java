/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer;
import static org.apache.flink.api.common.typeutils.base.TestEnum.BAR;
import static org.apache.flink.api.common.typeutils.base.TestEnum.EMMA;
import static org.apache.flink.api.common.typeutils.base.TestEnum.FOO;
import static org.apache.flink.api.common.typeutils.base.TestEnum.NATHANIEL;
import static org.apache.flink.api.common.typeutils.base.TestEnum.PAULA;
import static org.apache.flink.api.common.typeutils.base.TestEnum.PETER;

/**
 * Migration tests for {@link EnumSerializer}.
 */
@RunWith(Parameterized.class)
public class EnumSerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<TestEnum> {
	private static final String SPEC_NAME = "enum-serializer";

	public EnumSerializerSnapshotMigrationTest(TestSpecification<TestEnum> enumSerializer) {
		super(enumSerializer);
	}

	private static TestEnum[] previousEnumValues = {FOO, BAR, PETER, NATHANIEL, EMMA, PAULA};

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.addWithCompatibilityMatcher(
				SPEC_NAME,
				EnumSerializer.class,
				EnumSerializer.EnumSerializerSnapshot.class,
				() -> new EnumSerializer(TestEnum.class),
				isCompatibleWithReconfiguredSerializer(enumSerializerWith(previousEnumValues))
		);

		return testSpecifications.get();
	}

	private static Matcher<? extends TypeSerializer<TestEnum>> enumSerializerWith(final TestEnum[] expectedEnumValues) {
		return new TypeSafeMatcher<EnumSerializer<TestEnum>>() {

			@Override
			protected boolean matchesSafely(EnumSerializer<TestEnum> reconfiguredSerialized) {
				return Arrays.equals(reconfiguredSerialized.getValues(), expectedEnumValues);
			}

			@Override
			public void describeTo(Description description) {
				description
					.appendText("EnumSerializer with values ")
					.appendValueList("{", ", ", "}", expectedEnumValues);
			}
		};
	}
}

