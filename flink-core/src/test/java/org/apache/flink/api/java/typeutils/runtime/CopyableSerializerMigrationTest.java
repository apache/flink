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
import org.apache.flink.api.java.typeutils.runtime.CopyableSerializerMigrationTest.SimpleCopyable;
import org.apache.flink.api.java.typeutils.runtime.CopyableValueSerializer.CopyableValueSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.types.CopyableValue;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

/**
 * {@link CopyableValueSerializer} migration test.
 */
@RunWith(Parameterized.class)
public class CopyableSerializerMigrationTest extends TypeSerializerSnapshotMigrationTestBase<SimpleCopyable> {

	public CopyableSerializerMigrationTest(TestSpecification<SimpleCopyable> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"copyable-value-serializer",
			CopyableValueSerializer.class,
			CopyableValueSerializerSnapshot.class,
			() -> new CopyableValueSerializer<>(SimpleCopyable.class));

		return testSpecifications.get();
	}

	/**
	 * A simple copyable value for migration tests.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class SimpleCopyable implements CopyableValue<SimpleCopyable> {

		public static final long serialVersionUID = 1;

		private long value;

		public SimpleCopyable() {
		}

		public SimpleCopyable(long value) {
			this.value = value;
		}

		@Override
		public int getBinaryLength() {
			return 8;
		}

		@Override
		public void copyTo(SimpleCopyable target) {
			target.value = this.value;
		}

		@Override
		public SimpleCopyable copy() {
			return new SimpleCopyable(value);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeLong(source.readLong());
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeLong(value);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			value = in.readLong();
		}
	}
}
