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

package org.apache.flink.connectors.hive;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.FileSourceSplitSerializer;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * SerDe for {@link HiveSourceSplit}.
 */
public class HiveSourceSplitSerializer implements SimpleVersionedSerializer<HiveSourceSplit> {

	private static final int VERSION = 1;

	public static final HiveSourceSplitSerializer INSTANCE = new HiveSourceSplitSerializer();

	private HiveSourceSplitSerializer() {
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public byte[] serialize(HiveSourceSplit split) throws IOException {
		checkArgument(split.getClass() == HiveSourceSplit.class, "Cannot serialize subclasses of HiveSourceSplit");

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
			serialize(outputStream, split);
		}
		return byteArrayOutputStream.toByteArray();
	}

	@Override
	public HiveSourceSplit deserialize(int version, byte[] serialized) throws IOException {
		if (version == 1) {
			try (ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
				return deserializeV1(inputStream);
			}
		} else {
			throw new IOException("Unknown version: " + version);
		}
	}

	private void serialize(ObjectOutputStream outputStream, HiveSourceSplit split) throws IOException {
		byte[] superBytes = FileSourceSplitSerializer.INSTANCE.serialize(new FileSourceSplit(
				split.splitId(), split.path(), split.offset(), split.length(), split.hostnames(), split.getReaderPosition().orElse(null)));
		outputStream.writeInt(superBytes.length);
		outputStream.write(superBytes);
		outputStream.writeObject(split.getHiveTablePartition());
	}

	private HiveSourceSplit deserializeV1(ObjectInputStream inputStream) throws IOException {
		try {
			int superLen = inputStream.readInt();
			byte[] superBytes = new byte[superLen];
			inputStream.readFully(superBytes);
			FileSourceSplit superSplit = FileSourceSplitSerializer.INSTANCE.deserialize(FileSourceSplitSerializer.INSTANCE.getVersion(), superBytes);
			HiveTablePartition hiveTablePartition = (HiveTablePartition) inputStream.readObject();
			return new HiveSourceSplit(
					superSplit.splitId(),
					superSplit.path(),
					superSplit.offset(),
					superSplit.length(),
					superSplit.hostnames(),
					superSplit.getReaderPosition().orElse(null),
					hiveTablePartition);
		} catch (ClassNotFoundException e) {
			throw new IOException("Failed to deserialize HiveSourceSplit", e);
		}
	}
}
