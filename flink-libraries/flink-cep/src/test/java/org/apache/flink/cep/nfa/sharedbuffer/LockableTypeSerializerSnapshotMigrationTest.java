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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

/**
 * Migration test for the {@link LockableTypeSerializerSnapshot}.
 */
public class LockableTypeSerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Lockable<String>> {

	private static final String DATA = "flink-1.6-lockable-type-serializer-data";
	private static final String SNAPSHOT = "flink-1.6-lockable-type-serializer-snapshot";

	public LockableTypeSerializerSnapshotMigrationTest() {
		super(
			TestSpecification.<Lockable<String>>builder(
					"1.6-lockable-type-serializer",
					Lockable.LockableTypeSerializer.class,
					LockableTypeSerializerSnapshot.class)
				.withSerializerProvider(() -> new Lockable.LockableTypeSerializer<>(StringSerializer.INSTANCE))
				.withSnapshotDataLocation(SNAPSHOT)
				.withTestData(DATA, 10)
		);
	}
}
