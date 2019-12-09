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

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer.NullableSerializerSnapshot;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * {@link NullableSerializer} migration test.
 */
@RunWith(Parameterized.class)
public class NullableSerializerMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Long> {

	public NullableSerializerMigrationTest(TestSpecification<Long> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"nullable-padded-serializer",
			NullableSerializer.class,
			NullableSerializerSnapshot.class,
			() -> NullableSerializer.wrap(LongSerializer.INSTANCE, true),
			NULL_OR_LONG);

		testSpecifications.add(
			"nullable-not-padded-serializer",
			NullableSerializer.class,
			NullableSerializerSnapshot.class,
			() -> NullableSerializer.wrap(LongSerializer.INSTANCE, false),
			NULL_OR_LONG);

		return testSpecifications.get();
	}

	@SuppressWarnings("unchecked")
	private static final Matcher<Long> NULL_OR_LONG = new NullableMatcher();

	private static final class NullableMatcher extends BaseMatcher<Long> {

		@Override
		public boolean matches(Object item) {
			return item == null || item instanceof Long;
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("a null or a long");
		}
	}

}
