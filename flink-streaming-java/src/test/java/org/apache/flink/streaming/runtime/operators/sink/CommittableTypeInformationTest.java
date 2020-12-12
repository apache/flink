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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.typeutils.TypeInformationTestBase;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * Test for {@link CommittableTypeInformation}.
 */
public class CommittableTypeInformationTest
		extends TypeInformationTestBase<CommittableTypeInformation<CommittableTypeInformationTest.Dummy>> {

	@SuppressWarnings("unchecked")
	@Override
	protected CommittableTypeInformation<Dummy>[] getTestData() {
		return new CommittableTypeInformation[]{
				new CommittableTypeInformation<>(Dummy.class, SimpleVersionedDummySerializer::new)};
	}

	static class Dummy {}

	static class SimpleVersionedDummySerializer implements SimpleVersionedSerializer<Dummy> {

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(Dummy obj) throws IOException {
			return new byte[0];
		}

		@Override
		public Dummy deserialize(int version, byte[] serialized) throws IOException {
			return new Dummy();
		}

		@Override
		public int hashCode() {
			return 1;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof SimpleVersionedDummySerializer;
		}
	}
}
