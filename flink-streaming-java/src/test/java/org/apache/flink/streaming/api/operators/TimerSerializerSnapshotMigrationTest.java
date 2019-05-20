/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Migration test for {@link TimerSerializer}.
 */
@RunWith(Parameterized.class)
public class TimerSerializerSnapshotMigrationTest
	extends TypeSerializerSnapshotMigrationTestBase<TimerHeapInternalTimer<String, Integer>> {

	public TimerSerializerSnapshotMigrationTest(
		TestSpecification<TimerHeapInternalTimer<String, Integer>> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"timer-serializer",
			TimerSerializer.class,
			TimerSerializerSnapshot.class,
			TimerSerializerSnapshotMigrationTest::stringIntTimerSerializerSupplier);

		return testSpecifications.get();
	}

	private static TypeSerializer<TimerHeapInternalTimer<String, Integer>> stringIntTimerSerializerSupplier() {
		return new TimerSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE);
	}
}
