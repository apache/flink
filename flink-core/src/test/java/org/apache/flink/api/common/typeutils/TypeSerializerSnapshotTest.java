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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.Test;

import java.io.IOException;
import java.io.ObjectInputStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests the backwards compatibility of the TypeSerializerConfigSnapshot.
 */
@SuppressWarnings({"serial", "deprecation"})
public class TypeSerializerSnapshotTest {

	@Test
	public void testSerializeConfigWhenSerializerMissing() throws Exception {
		TestSerializer ser = new TestSerializer();
		TypeSerializerConfigSnapshot<Object> snap = (TypeSerializerConfigSnapshot<Object>) ser.snapshotConfiguration();

		try {
			TypeSerializerSnapshot.writeVersionedSnapshot(new DataOutputSerializer(64), snap);
			fail("exception expected");
		}
		catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testSerializerDeserializationFailure() throws Exception {
		TestSerializer ser = new TestSerializer();
		TypeSerializerConfigSnapshot<Object> snap = (TypeSerializerConfigSnapshot<Object>) ser.snapshotConfiguration();
		snap.setPriorSerializer(ser);

		DataOutputSerializer out = new DataOutputSerializer(64);

		TypeSerializerSnapshot.writeVersionedSnapshot(out, snap);
		TypeSerializerSnapshot<Object> readBack = TypeSerializerSnapshot.readVersionedSnapshot(
				new DataInputDeserializer(out.getCopyOfBuffer()), getClass().getClassLoader());

		assertNotNull(readBack);

		try {
			readBack.restoreSerializer();
			fail("expected exception");
		}
		catch (IllegalStateException e) {
			// expected
		}

		((TypeSerializerConfigSnapshot<Object>) readBack).setPriorSerializer(
				new UnloadableDummyTypeSerializer<>(new byte[0]));
		try {
			readBack.restoreSerializer();
			fail("expected exception");
		}
		catch (IllegalStateException e) {
			// expected
		}
	}

	// ------------------------------------------------------------------------

	private static final class TestSerializer extends TypeSerializer<Object> {

		private final boolean compatible;

		TestSerializer() {
			this(true);
		}

		TestSerializer(boolean compatible) {
			this.compatible = compatible;
		}

		@Override
		public boolean isImmutableType() {
			throw new UnsupportedOperationException();
		}

		@Override
		public TypeSerializer<Object> duplicate() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object createInstance() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object copy(Object from) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object copy(Object from, Object reuse) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getLength() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void serialize(Object record, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object deserialize(DataInputView source) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object deserialize(Object reuse, DataInputView source) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof TestSerializer;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			throw new IOException("cannot deserialize");
		}

		@Override
		public TypeSerializerSnapshot<Object> snapshotConfiguration() {
			return new TestSerializerConfigSnapshot();
		}
	}

	public static class TestSerializerConfigSnapshot extends TypeSerializerConfigSnapshot<Object> {

		@Override
		public int getVersion() {
			return 0;
		}
	}
}
