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

package org.apache.flink.batch.tests;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.io.IOException;

/**
 * InputFormat that generates a deterministic DataSet of Tuple2(String, Integer)
 * <ul>
 *     <li>String: key, can be repeated.</li>
 *     <li>Integer: uniformly distributed int between 0 and 127</li>
 * </ul>
 *
 * <p>If control path was provided, as long as this file is empty dummy elements with value equal to -1 will be emitted.
 */
public class Generator implements InputFormat<Tuple2<String, Integer>, GenericInputSplit> {

	// total number of records
	private final long numRecords;
	// total number of keys
	private final long numKeys;

	// records emitted per partition
	private long recordsPerPartition;
	// number of keys per partition
	private long keysPerPartition;

	// number of currently emitted records
	private long recordCnt;

	// id of current partition
	private int partitionId;

	private final String path;

	private boolean hasStarted = true;

	public Generator(long numKeys, int recordsPerKey) {
		this(numKeys, recordsPerKey, null);
	}

	public Generator(long numKeys, int recordsPerKey, String controlPath) {
		this.numKeys = numKeys;
		this.numRecords = numKeys * recordsPerKey;
		this.path = controlPath;
	}

	@Override
	public void configure(Configuration parameters) { }

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return null;
	}

	@Override
	public GenericInputSplit[] createInputSplits(int minNumSplits) {

		GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
		for (int i = 0; i < minNumSplits; i++) {
			splits[i] = new GenericInputSplit(i, minNumSplits);
		}
		return splits;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		this.partitionId = split.getSplitNumber();
		// total number of partitions
		int numPartitions = split.getTotalNumberOfSplits();

		// ensure even distribution of records and keys
		Preconditions.checkArgument(
			numRecords % numPartitions == 0,
			"Records cannot be evenly distributed among partitions");
		Preconditions.checkArgument(
			numKeys % numPartitions == 0,
			"Keys cannot be evenly distributed among partitions");

		this.recordsPerPartition = numRecords / numPartitions;
		this.keysPerPartition = numKeys / numPartitions;

		this.recordCnt = 0;

		if (this.path != null) {
			this.hasStarted = false;
		}
	}

	@Override
	public boolean reachedEnd() {
		return this.recordCnt >= this.recordsPerPartition;
	}

	@Override
	public Tuple2<String, Integer> nextRecord(Tuple2<String, Integer> reuse) throws IOException {

		if (!hasStarted) {
			checkExternallyIfStarted();
			return Tuple2.of("dummy", -1);
		}

		// build key from partition id and count per partition
		String key = String.format(
			"%d-%d",
			this.partitionId,
			this.recordCnt % this.keysPerPartition);
		// 128 values to filter on
		int filterVal = (int) this.recordCnt % 128;

		this.recordCnt++;

		reuse.f0 = key;
		reuse.f1 = filterVal;
		return reuse;
	}

	@SuppressWarnings("ConstantConditions")
	private void checkExternallyIfStarted() throws IOException {
		if (FileUtils.readFileUtf8(new File(path)).isEmpty()) {
			hasStarted = true;
		} else {
			//backoff a little
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void close() { }
}
