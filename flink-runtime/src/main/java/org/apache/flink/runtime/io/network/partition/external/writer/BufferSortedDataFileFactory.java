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

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.SerializerManager;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.operators.sort.SortedDataFile;
import org.apache.flink.runtime.operators.sort.SortedDataFileFactory;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Factory to create BufferSortedDataFile.
 */
public class BufferSortedDataFileFactory<T> implements SortedDataFileFactory<T> {
	private final String partitionDataRootPath;
	private final TypeSerializer<T> serialize;
	private final IOManager ioManager;
	private final SerializerManager<SerializationDelegate<T>> serializerManager;

	private final Counter numBytesOut;
	private final Counter numBuffersOut;

	private int nextFileId;

	public BufferSortedDataFileFactory(String partitionDataRootPath, TypeSerializer<T> serialize, IOManager ioManager,
		SerializerManager<SerializationDelegate<T>> serializerManager, Counter numBytesOut, Counter numBuffersOut) {
		this.partitionDataRootPath = partitionDataRootPath;
		this.serialize = serialize;
		this.ioManager = ioManager;
		this.serializerManager = serializerManager;

		this.numBytesOut = numBytesOut;
		this.numBuffersOut = numBuffersOut;
	}

	@Override
	public SortedDataFile<T> createFile(List<MemorySegment> writeMemory) throws IOException {
		int fileId = nextFileId++;
		String path = ExternalBlockShuffleUtils.generateSpillPath(partitionDataRootPath, fileId);

		return new BufferSortedDataFile<T>(ioManager.createChannel(new File(path)), fileId, serialize, ioManager,
			writeMemory, serializerManager, numBytesOut, numBuffersOut);
	}
}
