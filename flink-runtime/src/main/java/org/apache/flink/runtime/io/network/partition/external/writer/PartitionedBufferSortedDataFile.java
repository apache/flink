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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelBackendMutableObjectIterator;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;

import java.io.IOException;
import java.util.List;

/**
 * A partitioned sorted data file backend by a BufferSortedDataFile.
 */
public class PartitionedBufferSortedDataFile<T> implements PartitionedSortedDataFile<T> {
	private final BufferSortedDataFile<T> backendFile;
	private final PartitionIndexGenerator partitionIndexGenerator;

	private int currentPartition;
	private int numRecordWritten;

	public PartitionedBufferSortedDataFile(int numberOfSubpartitions, BufferSortedDataFile<T> backendFile) {
		this.backendFile = backendFile;
		partitionIndexGenerator = new PartitionIndexGenerator(numberOfSubpartitions);
	}

	@Override
	public FileIOChannel getWriteChannel() {
		return backendFile.getWriteChannel();
	}

	@Override
	public FileIOChannel.ID getChannelID() {
		return backendFile.getChannelID();
	}

	@Override
	public void writeRecord(Tuple2<Integer, T> record) throws IOException {
		if (record.f0 != currentPartition) {
			backendFile.flush();
			currentPartition = record.f0;
		}

		partitionIndexGenerator.updatePartitionIndexBeforeWriting(
			record.f0, backendFile.getBytesWritten(), numRecordWritten);

		backendFile.writeRecord(record.f1);
		numRecordWritten++;
	}

	@Override
	public void copyRecord(DataInputView serializedRecord) throws IOException {
		int partitionIndex = serializedRecord.readInt();

		if (partitionIndex != currentPartition) {
			backendFile.flush();
			currentPartition = partitionIndex;
		}

		partitionIndexGenerator.updatePartitionIndexBeforeWriting(
			partitionIndex, backendFile.getBytesWritten(), numRecordWritten);

		backendFile.copyRecord(serializedRecord);

		numRecordWritten++;
	}

	@Override
	public long getBytesWritten() throws IOException {
		return backendFile.getBytesWritten();
	}

	@Override
	public void finishWriting() throws IOException {
		backendFile.finishWriting();
		partitionIndexGenerator.finishWriting(backendFile.getBytesWritten(), numRecordWritten);
	}

	@Override
	public ChannelBackendMutableObjectIterator<Tuple2<Integer, T>> createReader(List<MemorySegment> readMemory) throws IOException {
		ChannelBackendMutableObjectIterator<T> recordIterator = backendFile.createReader(readMemory);
		return new PartitionedRecordsIterator<>(recordIterator, partitionIndexGenerator.getPartitionIndices());
	}

	public List<PartitionIndex> getPartitionIndexList() {
		return partitionIndexGenerator.getPartitionIndices();
	}

	public int getFileId() {
		return backendFile.getFileId();
	}

	private static class PartitionedRecordsIterator<T> implements ChannelBackendMutableObjectIterator<Tuple2<Integer, T>> {
		private final ChannelBackendMutableObjectIterator<T> recordIterator;
		private final List<PartitionIndex> partitionIndices;
		private long numReadRecords;

		private int currentPartition;
		private long currentPartitionRemainRecords;

		public PartitionedRecordsIterator(ChannelBackendMutableObjectIterator<T> recordIterator, List<PartitionIndex> partitionIndices) {
			this.recordIterator = recordIterator;
			this.partitionIndices = partitionIndices;
		}

		@Override
		public Tuple2<Integer, T> next(Tuple2<Integer, T> reuse) throws IOException {
			T rec = recordIterator.next(reuse.f1);

			if (rec == null) {
				return null;
			}

			reuse.f0 = currentPartition;

			currentPartitionRemainRecords--;
			while(currentPartitionRemainRecords == 0) {
				currentPartition++;
				currentPartitionRemainRecords = partitionIndices.get(currentPartition).getNumRecords();
			}

			return reuse;
		}

		@Override
		public Tuple2<Integer, T> next() throws IOException {
			T rec = recordIterator.next();

			if (rec == null) {
				return null;
			}

			Tuple2<Integer, T> result = new Tuple2<>(currentPartition, rec);

			currentPartitionRemainRecords--;
			while(currentPartitionRemainRecords == 0) {
				currentPartition++;
				currentPartitionRemainRecords = partitionIndices.get(currentPartition).getNumRecords();
			}

			return result;
		}

		@Override
		public FileIOChannel getReaderChannel() {
			return recordIterator.getReaderChannel();
		}
	}
}
