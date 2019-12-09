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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.UnionSerializer;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.UnionSerializerSnapshot;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * State migration tests for {@link UnionSerializer}.
 */
@RunWith(Parameterized.class)
public class UnionSerializerMigrationTest extends TypeSerializerSnapshotMigrationTestBase<TaggedUnion<String, Long>> {

	public UnionSerializerMigrationTest(TestSpecification<TaggedUnion<String, Long>> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"union-serializer",
			UnionSerializer.class,
			UnionSerializerSnapshot.class,
			UnionSerializerMigrationTest::stringLongRowSupplier);

		return testSpecifications.get();
	}

	private static TypeSerializer<TaggedUnion<String, Long>> stringLongRowSupplier() {
		return new UnionSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE);
	}

}



