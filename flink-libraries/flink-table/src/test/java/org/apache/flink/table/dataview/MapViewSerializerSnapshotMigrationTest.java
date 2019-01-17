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

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.testutils.migration.MigrationVersion;

/**
 * Migration test for the {@link MapViewSerializerSnapshot}.
 */
public class MapViewSerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<MapView<Integer, String>> {

	private static final String DATA = "flink-1.6-map-view-serializer-data";
	private static final String SNAPSHOT = "flink-1.6-map-view-serializer-snapshot";

	public MapViewSerializerSnapshotMigrationTest() {
		super(
			TestSpecification.<MapView<Integer, String>>builder(
					"1.6-map-view-serializer",
					MapViewSerializer.class,
					MapViewSerializerSnapshot.class,
					MigrationVersion.v1_6)
				.withNewSerializerProvider(() -> new MapViewSerializer<>(
					new MapSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE)))
				.withSnapshotDataLocation(SNAPSHOT)
				.withTestData(DATA, 10)
		);
	}
}
