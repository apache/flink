/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.	See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.	You may obtain a copy of the License at
*
*		http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.compression.BlockCompressionFactory;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Key-Value style record merger for sort.
 */
public class BinaryKVExternalMerger extends AbstractBinaryExternalMerger<Tuple2<BinaryRow, BinaryRow>> {

	private final BinaryRowSerializer keySerializer;
	private final BinaryRowSerializer valueSerializer;
	private final RecordComparator comparator;

	public BinaryKVExternalMerger(
			IOManager ioManager,
			int pageSize,
			int maxFanIn,
			SpillChannelManager channelManager,
			BinaryRowSerializer keySerializer,
			BinaryRowSerializer valueSerializer,
			RecordComparator comparator,
			boolean compressionEnable,
			BlockCompressionFactory compressionCodecFactory,
			int compressionBlockSize) {
		super(ioManager, pageSize, maxFanIn, channelManager, compressionEnable, compressionCodecFactory, compressionBlockSize);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.comparator = comparator;
	}

	@Override
	protected List<Tuple2<BinaryRow, BinaryRow>> mergeReusedEntries(int size) {
		ArrayList<Tuple2<BinaryRow, BinaryRow>> reused = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			reused.add(
					new Tuple2<>(keySerializer.createInstance(), valueSerializer.createInstance()));
		}
		return reused;
	}

	@Override
	protected MutableObjectIterator<Tuple2<BinaryRow, BinaryRow>> channelReaderInputViewIterator(
			AbstractChannelReaderInputView inView) {
		return new ChannelReaderKVInputViewIterator<>(
				inView, null, keySerializer.duplicate(), valueSerializer.duplicate());
	}

	@Override
	protected Comparator<Tuple2<BinaryRow, BinaryRow>> mergeComparator() {
		return (o1, o2) -> comparator.compare(o1.f0, o2.f0);
	}

	@Override
	protected void writeMergingOutput(
			MutableObjectIterator<Tuple2<BinaryRow, BinaryRow>> mergeIterator,
			AbstractPagedOutputView output) throws IOException {
		// read the merged stream and write the data back
		Tuple2<BinaryRow, BinaryRow> kv = new Tuple2<>(
				keySerializer.createInstance(), valueSerializer.createInstance());
		while ((kv = mergeIterator.next(kv)) != null) {
			keySerializer.serialize(kv.f0, output);
			valueSerializer.serialize(kv.f1, output);
		}
	}
}
