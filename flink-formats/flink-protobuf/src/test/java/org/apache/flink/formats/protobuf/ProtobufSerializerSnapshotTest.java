/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.protobuf.generated.UserProtobuf;
import org.apache.flink.formats.protobuf.typeutils.ProtobufSerializer;
import org.apache.flink.formats.protobuf.typeutils.ProtobufSerializerSnapshot;

import com.google.protobuf.Message;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.formats.protobuf.utils.SerializerSchemaCompatibilityUtils.isCompatibleAsIs;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ProtobufSerializerSnapshot}.
 */
public class ProtobufSerializerSnapshotTest {

	@Test
	public void aProtobufSnapshotIsCompatibleWithItsOriginatingSerializer() {
		ExecutionConfig executionConfig = new ExecutionConfig();
		ProtobufSerializer<UserProtobuf.User> serializer =
			new ProtobufSerializer<>(UserProtobuf.User.class, executionConfig);

		TypeSerializerSnapshot<UserProtobuf.User> snapshot = serializer.snapshotConfiguration();

		assertThat(snapshot.resolveSchemaCompatibility(serializer), isCompatibleAsIs());
	}

	@Test
	public void aProtobufnapshotIsCompatibleAfterARoundTrip() throws IOException {
		ExecutionConfig executionConfig = new ExecutionConfig();
		ProtobufSerializer<UserProtobuf.User> serializer =
			new ProtobufSerializer<>(UserProtobuf.User.class, executionConfig);

		ProtobufSerializerSnapshot<UserProtobuf.User> restored = roundTrip(serializer.snapshotConfiguration());

		assertThat(restored.resolveSchemaCompatibility(serializer), isCompatibleAsIs());
	}

	// ---------------------------------------------------------------------------------------------------------------
	// Utils
	// ---------------------------------------------------------------------------------------------------------------

	/**
	 * Serialize an (avro)TypeSerializerSnapshot and deserialize it.
	 */
	private static <T extends Message> ProtobufSerializerSnapshot<T> roundTrip(TypeSerializerSnapshot<T> original) throws IOException {
		// writeSnapshot();
		DataOutputSerializer out = new DataOutputSerializer(1024);
		original.writeSnapshot(out);

		// init
		ProtobufSerializerSnapshot<T> restored = new ProtobufSerializerSnapshot<>();

		// readSnapshot();
		DataInputView in = new DataInputDeserializer(out.wrapAsByteBuffer());
		restored.readSnapshot(restored.getCurrentVersion(), in, original.getClass().getClassLoader());

		return restored;
	}

}
