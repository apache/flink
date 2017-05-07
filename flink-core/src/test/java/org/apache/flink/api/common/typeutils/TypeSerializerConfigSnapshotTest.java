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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests related to {@link TypeSerializerConfigSnapshot}.
 */
public class TypeSerializerConfigSnapshotTest {

	/**
	 * Verifies that reading and writing configuration snapshots work correctly.
	 */
	@Test
	public void testSerializeConfigurationSnapshots() throws Exception {
		TestConfigSnapshot configSnapshot1 = new TestConfigSnapshot(1, "foo");
		TestConfigSnapshot configSnapshot2 = new TestConfigSnapshot(2, "bar");

		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerUtil.writeSerializerConfigSnapshots(
				new DataOutputViewStreamWrapper(out),
				configSnapshot1,
				configSnapshot2);

			serializedConfig = out.toByteArray();
		}

		TypeSerializerConfigSnapshot[] restoredConfigs;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			restoredConfigs = TypeSerializerUtil.readSerializerConfigSnapshots(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		assertEquals(2, restoredConfigs.length);
		assertEquals(configSnapshot1, restoredConfigs[0]);
		assertEquals(configSnapshot2, restoredConfigs[1]);
	}

	/**
	 * Verifies that deserializing config snapshots fail if the config class could not be found.
	 */
	@Test
	public void testFailsWhenConfigurationSnapshotClassNotFound() throws Exception {
		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerUtil.writeSerializerConfigSnapshot(
				new DataOutputViewStreamWrapper(out), new TestConfigSnapshot(123, "foobar"));
			serializedConfig = out.toByteArray();
		}

		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			// read using a dummy classloader
			TypeSerializerUtil.readSerializerConfigSnapshot(
				new DataInputViewStreamWrapper(in), new URLClassLoader(new URL[0], null));
			fail("Expected a ClassNotFoundException wrapped in IOException");
		} catch (IOException expected) {
			// test passes
		}
	}

	/**
	 * Tests that serializing and then deserializing the special marker config
	 * {@link ForwardCompatibleSerializationFormatConfig#INSTANCE} always
	 * restores the singleton instance.
	 */
	@Test
	public void testSerializeForwardCompatibleMarkerConfig() throws Exception {
		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerUtil.writeSerializerConfigSnapshot(
				new DataOutputViewStreamWrapper(out), ForwardCompatibleSerializationFormatConfig.INSTANCE);
			serializedConfig = out.toByteArray();
		}

		TypeSerializerConfigSnapshot restoredConfig;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			restoredConfig = TypeSerializerUtil.readSerializerConfigSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		// reference equality to the singleton instance
		assertTrue(restoredConfig == ForwardCompatibleSerializationFormatConfig.INSTANCE);
	}

	/**
	 * Verifies that the actual reconfigure method is never invoked if the
	 * provided configuration snapshot is the special singleton marker config
	 * {@link ForwardCompatibleSerializationFormatConfig#INSTANCE}.
	 */
	@Test
	public void testMigrationStrategyWithForwardCompatibleMarkerConfig() {
		TypeSerializer<?> mockSerializer = spy(TypeSerializer.class);

		mockSerializer.getMigrationStrategyFor(ForwardCompatibleSerializationFormatConfig.INSTANCE);
		verify(mockSerializer, never()).getMigrationStrategy(any(TypeSerializerConfigSnapshot.class));

		// make sure that is actually is called if its not the special marker
		TypeSerializerConfigSnapshot nonForwardCompatibleConfig = new TestConfigSnapshot(123, "foobar");
		mockSerializer.getMigrationStrategyFor(nonForwardCompatibleConfig);
		verify(mockSerializer, times(1)).getMigrationStrategy(nonForwardCompatibleConfig);
	}

	public static class TestConfigSnapshot extends TypeSerializerConfigSnapshot {

		static final int VERSION = 1;

		private int val;
		private String msg;

		public TestConfigSnapshot() {}

		public TestConfigSnapshot(int val, String msg) {
			this.val = val;
			this.msg = msg;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);
			out.writeInt(val);
			out.writeUTF(msg);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);
			val = in.readInt();
			msg = in.readUTF();
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}

			if (obj == null) {
				return false;
			}

			if (obj instanceof TestConfigSnapshot) {
				return val == ((TestConfigSnapshot) obj).val && msg.equals(((TestConfigSnapshot) obj).msg);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return 31 * val + msg.hashCode();
		}
	}
}
