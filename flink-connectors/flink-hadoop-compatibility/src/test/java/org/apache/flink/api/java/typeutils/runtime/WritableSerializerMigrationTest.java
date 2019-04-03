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
import org.apache.flink.api.java.typeutils.runtime.WritableSerializer.WritableSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.WritableSerializerMigrationTest.WritableName;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.apache.hadoop.io.Writable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

/**
 * State migration test for {@link RowSerializer}.
 */
@RunWith(Parameterized.class)
public class WritableSerializerMigrationTest extends TypeSerializerSnapshotMigrationTestBase<WritableName> {

	public WritableSerializerMigrationTest(TestSpecification<WritableName> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"writeable-serializer",
			WritableSerializer.class,
			WritableSerializerSnapshot.class,
			() -> new WritableSerializer<>(WritableName.class));

		return testSpecifications.get();
	}

	/**
	 * A dummy class that is used in this test.
	 */
	public static final class WritableName implements Writable {

		public static final long serialVersionUID = 1L;

		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(name);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			name = in.readUTF();
		}
	}

}
