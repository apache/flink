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

import org.apache.avro.generic.GenericRecord;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Tests migrations for {@link AvroSerializerSnapshot}.
 */
@RunWith(Parameterized.class)
public class AvroSerializerMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	private static final String DATA = "flink-1.6-avro-type-serializer-address-data";
	private static final String SPECIFIC_SNAPSHOT = "flink-1.6-avro-type-serializer-address-snapshot";
	private static final String GENERIC_SNAPSHOT = "flink-1.6-avro-generic-type-serializer-address-snapshot";

	public AvroSerializerMigrationTest(TestSpecification<Object> testSpec) {
		super(testSpec);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<Object[]> testSpecifications() {

		final TestSpecification<Address> genericCase = TestSpecification.<Address>builder("1.6-generic", AvroSerializer.class, AvroSerializerSnapshot.class)
			.withSerializerProvider(() -> new AvroSerializer(GenericRecord.class, Address.getClassSchema()))
			.withSnapshotDataLocation(GENERIC_SNAPSHOT)
			.withTestData(DATA, 10);

		final TestSpecification<Address> specificCase = TestSpecification.<Address>builder("1.6-specific", AvroSerializer.class, AvroSerializerSnapshot.class)
			.withSerializerProvider(() -> new AvroSerializer<>(Address.class))
			.withSnapshotDataLocation(SPECIFIC_SNAPSHOT)
			.withTestData(DATA, 10);

		return Arrays.asList(
			new Object[]{genericCase},
			new Object[]{specificCase}
		);
	}

}
