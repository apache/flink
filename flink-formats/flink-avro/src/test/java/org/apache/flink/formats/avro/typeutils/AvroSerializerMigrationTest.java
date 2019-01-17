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

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.apache.avro.generic.GenericRecord;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Tests migrations for {@link AvroSerializerSnapshot}.
 */
@RunWith(Parameterized.class)
public class AvroSerializerMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Address> {

	private static final String DATA_FILE_FORMAT = "flink-%s-avro-type-serializer-address-data";
	private static final String SPECIFIC_SNAPSHOT_FILE_FORMAT = "flink-%s-avro-type-serializer-address-snapshot";
	private static final String GENERIC_SNAPSHOT_FILE_FORMAT = "flink-%s-avro-generic-type-serializer-address-snapshot";

	public AvroSerializerMigrationTest(TestSpecification<Address> testSpec) {
		super(testSpec);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"generic-avro-serializer",
			AvroSerializer.class,
			AvroSerializerSnapshot.class,
			() -> new AvroSerializer(GenericRecord.class, Address.getClassSchema()),
			testVersion -> String.format(GENERIC_SNAPSHOT_FILE_FORMAT, testVersion),
			testVersion -> String.format(DATA_FILE_FORMAT, testVersion),
			10);
		testSpecifications.add(
			"specific-avro-serializer",
			AvroSerializer.class,
			AvroSerializerSnapshot.class,
			() -> new AvroSerializer<>(Address.class),
			testVersion -> String.format(SPECIFIC_SNAPSHOT_FILE_FORMAT, testVersion),
			testVersion -> String.format(DATA_FILE_FORMAT, testVersion),
			10);

		return testSpecifications.get();
	}

}
