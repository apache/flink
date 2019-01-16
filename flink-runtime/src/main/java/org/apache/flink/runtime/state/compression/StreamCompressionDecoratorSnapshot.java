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

package org.apache.flink.runtime.state.compression;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * A {@code StreamCompressionDecoratorSnapshot} is a point-in-time view of a {@link StreamCompressionDecorator}'s configuration.
 *
 * <p>This class borrows the design ideas from {@link TypeSerializerSnapshot}. The {@code StreamCompressionDecoratorSnapshot} object would be stored
 * when {@link KeyedBackendSerializationProxy#write(DataOutputView)} and restored when {@link KeyedBackendSerializationProxy#read(DataInputView)}.
 * After {@code StreamCompressionDecoratorSnapshot} object restored, it would then call {@link #getStreamCompressionDecorator()} to create the
 * {@link StreamCompressionDecorator} so that previously compressed data could be read back to {@link AbstractKeyedStateBackend}.
 */
@Public
public interface StreamCompressionDecoratorSnapshot {

	/**
	 * Returns the version of the current snapshot's written binary format.
	 *
	 * @return the version of the current snapshot's written binary format.
	 */
	int getCurrentVersion();

	/**
	 *  Returns the compression decorator according to the compression type.
	 */
	StreamCompressionDecorator getStreamCompressionDecorator();

	/**
	 * Writes the compression type snapshot to the provided {@link DataOutputView}.
	 *
	 * @param out the {@link DataOutputView} to write the snapshot to.
	 *
	 * @throws IOException Thrown if the snapshot data could not be written.
	 */
	void writeSnapshot(DataOutputView out) throws IOException;

	/**
	 * Reads the compression decorator snapshot from the provided {@link DataInputView}.
	 *
	 * @param readVersion version of the compression decorator snapshot's written binary format.
	 * @param in the {@link DataInputView} to read the snapshot from.
	 * @param userCodeClassLoader the user code classloader.
	 *
	 * * @throws IOException Thrown if the snapshot data could be read or parsed.
	 */
	void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException;

	// ------------------------------------------------------------------------
	//  read / write utilities
	// ------------------------------------------------------------------------

	/**
	 * Writes the given snapshot to the out stream. One should always use this method to write
	 * snapshots out, rather than directly calling {@link #writeSnapshot(DataOutputView)}.
	 *
	 * <p>The snapshot written with this method can be read via {@link #readVersionedSnapshot(DataInputView, ClassLoader)}.
	 */
	static void writeVersionedSnapshot(DataOutputView out, StreamCompressionDecoratorSnapshot snapshot) throws IOException {
		out.writeUTF(snapshot.getClass().getName());
		out.writeInt(snapshot.getCurrentVersion());
		snapshot.writeSnapshot(out);
	}


	/**
	 * Reads a snapshot from the stream, performing resolving
	 *
	 * <p>This method reads snapshots written by {@link #writeVersionedSnapshot(DataOutputView, StreamCompressionDecoratorSnapshot)}.
	 */
	static StreamCompressionDecoratorSnapshot readVersionedSnapshot(DataInputView in, ClassLoader cl) throws IOException {
		Class<StreamCompressionDecoratorSnapshot> clazz = InstantiationUtil.resolveClassByName(in, cl, StreamCompressionDecoratorSnapshot.class);
		final StreamCompressionDecoratorSnapshot snapshot = InstantiationUtil.instantiate(clazz);

		final int version = in.readInt();
		snapshot.readSnapshot(version, in, cl);

		return snapshot;
	}
}
