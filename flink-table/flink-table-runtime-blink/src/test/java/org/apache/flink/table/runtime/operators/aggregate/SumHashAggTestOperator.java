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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.runtime.operators.sort.BufferedKVExternalSorter;
import org.apache.flink.table.runtime.operators.sort.IntNormalizedKeyComputer;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;

/**
 * Test for Hash aggregation of (select f0, sum(f1) from T).
 */
public class SumHashAggTestOperator extends AbstractStreamOperator<BaseRow>
		implements OneInputStreamOperator<BaseRow, BaseRow> {

	private final long memorySize;
	private final LogicalType[] keyTypes = new LogicalType[] {new IntType()};
	private final LogicalType[] aggBufferTypes = new LogicalType[] {new IntType(), new BigIntType()};

	private transient BinaryRow currentKey;
	private transient BinaryRowWriter currentKeyWriter;

	private transient BufferedKVExternalSorter sorter;
	private transient BytesHashMap aggregateMap;

	private transient BinaryRow emptyAggBuffer;

	public SumHashAggTestOperator(long memorySize) throws Exception {
		this.memorySize = memorySize;
	}

	@Override
	public void open() throws Exception {
		super.open();
		aggregateMap = new BytesHashMap(
				getOwner(), getMemoryManager(), memorySize,
				keyTypes, aggBufferTypes);

		currentKey = new BinaryRow(1);
		currentKeyWriter = new BinaryRowWriter(currentKey);
		emptyAggBuffer = new BinaryRow(1);

		// for null value
		BinaryRowWriter emptyAggBufferWriter = new BinaryRowWriter(emptyAggBuffer);
		emptyAggBufferWriter.reset();
		emptyAggBufferWriter.setNullAt(0);
		emptyAggBufferWriter.complete();
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow in1 = element.getValue();

		// project key from input
		currentKeyWriter.reset();
		if (in1.isNullAt(0)) {
			currentKeyWriter.setNullAt(0);
		} else {
			currentKeyWriter.writeInt(0, in1.getInt(0));
		}
		currentKeyWriter.complete();

		// look up output buffer using current group key
		BytesHashMap.LookupInfo lookupInfo = aggregateMap.lookup(currentKey);
		BinaryRow currentAggBuffer = lookupInfo.getValue();

		if (!lookupInfo.isFound()) {

			// append empty agg buffer into aggregate map for current group key
			try {
				currentAggBuffer = aggregateMap.append(lookupInfo, emptyAggBuffer);
			} catch (EOFException exp) {
				// hash map out of memory, spill to external sorter
				if (sorter == null) {
					sorter = new BufferedKVExternalSorter(
							getIOManager(),
							new BinaryRowSerializer(keyTypes.length),
							new BinaryRowSerializer(aggBufferTypes.length),
							new IntNormalizedKeyComputer(), new IntRecordComparator(),
							getMemoryManager().getPageSize(),
							getConf());
				}
				// sort and spill
				sorter.sortAndSpill(
						aggregateMap.getRecordAreaMemorySegments(),
						aggregateMap.getNumElements(),
						new BytesHashMapSpillMemorySegmentPool(aggregateMap.getBucketAreaMemorySegments()));

				// retry append
				// reset aggregate map retry append
				aggregateMap.reset();
				lookupInfo = aggregateMap.lookup(currentKey);
				try {
					currentAggBuffer = aggregateMap.append(lookupInfo, emptyAggBuffer);
				} catch (EOFException e) {
					throw new OutOfMemoryError("BytesHashMap Out of Memory.");
				}
			}
		}

		if (!in1.isNullAt(1)) {
			long sumInput = in1.getLong(1);
			if (currentAggBuffer.isNullAt(0)) {
				currentAggBuffer.setLong(0, sumInput);
			} else {
				currentAggBuffer.setLong(0, sumInput + currentAggBuffer.getLong(0));
			}
		}
	}

	public void endInput() throws Exception {

		StreamRecord<BaseRow> outElement = new StreamRecord<>(null);
		JoinedRow hashAggOutput = new JoinedRow();
		GenericRow aggValueOutput = new GenericRow(1);

		if (sorter == null) {
			// no spilling, output by iterating aggregate map.
			MutableObjectIterator<BytesHashMap.Entry> iter = aggregateMap.getEntryIterator();

			BinaryRow reuseAggMapKey = new BinaryRow(1);
			BinaryRow reuseAggBuffer = new BinaryRow(1);
			BytesHashMap.Entry reuseAggMapEntry = new BytesHashMap.Entry(reuseAggMapKey, reuseAggBuffer);

			while (iter.next(reuseAggMapEntry) != null) {
				// set result and output
				aggValueOutput.setField(0, reuseAggBuffer.isNullAt(0) ? null : reuseAggBuffer.getLong(0));
				hashAggOutput.replace(reuseAggMapKey, aggValueOutput);
				getOutput().collect(outElement.replace(hashAggOutput));
			}
		} else {
			// spill last part of input' aggregation output buffer
			sorter.sortAndSpill(
					aggregateMap.getRecordAreaMemorySegments(),
					aggregateMap.getNumElements(),
					new BytesHashMapSpillMemorySegmentPool(aggregateMap.getBucketAreaMemorySegments()));

			// only release non-data memory in advance.
			aggregateMap.free(true);

			// fall back to sort based aggregation
			BinaryRow lastKey = null;
			JoinedRow fallbackInput = new JoinedRow();
			boolean aggSumIsNull = false;
			long aggSum = -1;

			// free hash map memory, but not release back to memory manager
			MutableObjectIterator<Tuple2<BinaryRow, BinaryRow>> iterator = sorter.getKVIterator();
			Tuple2<BinaryRow, BinaryRow> kv;
			while ((kv = iterator.next()) != null) {
				BinaryRow key = kv.f0;
				BinaryRow value = kv.f1;
				// prepare input
				fallbackInput.replace(key, value);
				if (lastKey == null) {
					// found first key group
					lastKey = key.copy();
					aggSumIsNull = true;
					aggSum = -1L;
				} else if (key.getSizeInBytes() != lastKey.getSizeInBytes() ||
						!(BinaryRowUtil.byteArrayEquals(
								key.getSegments()[0].getArray(),
								lastKey.getSegments()[0].getArray(),
								key.getSizeInBytes()))) {

					// output current group aggregate result
					aggValueOutput.setField(0, aggSumIsNull ? null : aggSum);
					hashAggOutput.replace(lastKey, aggValueOutput);
					getOutput().collect(outElement.replace(hashAggOutput));

					// found new group
					lastKey = key.copy();
					aggSumIsNull = true;
					aggSum = -1L;
				}

				if (!fallbackInput.isNullAt(1)) {
					long sumInput = fallbackInput.getLong(1);
					if (aggSumIsNull) {
						aggSum = sumInput;
					} else {
						aggSum = aggSum + sumInput;
					}
					aggSumIsNull = false;
				}
			}

			// output last key group aggregate result
			aggValueOutput.setField(0, aggSumIsNull ? null : aggSum);
			hashAggOutput.replace(lastKey, aggValueOutput);
			getOutput().collect(outElement.replace(hashAggOutput));
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		aggregateMap.free();
		if (sorter != null) {
			sorter.close();
		}
	}

	Object getOwner() {
		return getContainingTask();
	}

	Collector<StreamRecord<BaseRow>> getOutput() {
		return output;
	}

	MemoryManager getMemoryManager() {
		return getContainingTask().getEnvironment().getMemoryManager();
	}

	Configuration getConf() {
		return getContainingTask().getJobConfiguration();
	}

	public IOManager getIOManager() {
		return getContainingTask().getEnvironment().getIOManager();
	}
}
