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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import java.io.IOException;
import java.util.List;

/**
 * Factory for creating BlockSortedDataFile.
 */
public class BlockSortedDataFileFactory<T> implements SortedDataFileFactory<T> {
	private FileIOChannel.Enumerator enumerator;
	private TypeSerializer<T> serialize;
	private IOManager ioManager;

	public BlockSortedDataFileFactory(FileIOChannel.Enumerator enumerator, TypeSerializer<T> serialize, IOManager ioManager) {
		this.enumerator = enumerator;
		this.serialize = serialize;
		this.ioManager = ioManager;
	}

	@Override
	public SortedDataFile<T> createFile(List<MemorySegment> writeMemory) throws IOException {
		return new BlockSortedDataFile<>(enumerator.next(), serialize, ioManager, writeMemory);
	}
}
