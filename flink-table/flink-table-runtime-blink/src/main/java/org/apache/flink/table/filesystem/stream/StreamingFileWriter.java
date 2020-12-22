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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Writer for emitting {@link PartitionCommitInfo} to downstream.
 */
public class StreamingFileWriter<IN> extends AbstractStreamingWriter<IN, PartitionCommitInfo> {

	private static final long serialVersionUID = 2L;

	private transient Set<String> currentNewPartitions;
	private transient TreeMap<Long, Set<String>> newPartitions;
	private transient Set<String> committablePartitions;

	public StreamingFileWriter(
			long bucketCheckInterval,
			StreamingFileSink.BucketsBuilder<IN, String, ? extends
					StreamingFileSink.BucketsBuilder<IN, String, ?>> bucketsBuilder) {
		super(bucketCheckInterval, bucketsBuilder);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		currentNewPartitions = new HashSet<>();
		newPartitions = new TreeMap<>();
		committablePartitions = new HashSet<>();
		super.initializeState(context);
	}

	@Override
	protected void partitionCreated(String partition) {
		currentNewPartitions.add(partition);
	}

	@Override
	protected void partitionInactive(String partition) {
		committablePartitions.add(partition);
	}

	@Override
	protected void onPartFileOpened(String s, Path newPath) {
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		newPartitions.put(context.getCheckpointId(), new HashSet<>(currentNewPartitions));
		currentNewPartitions.clear();
	}

	@Override
	protected void commitUpToCheckpoint(long checkpointId) throws Exception {
		super.commitUpToCheckpoint(checkpointId);

		NavigableMap<Long, Set<String>> headPartitions = this.newPartitions.headMap(checkpointId, true);
		Set<String> partitions = new HashSet<>(committablePartitions);
		committablePartitions.clear();
		headPartitions.values().forEach(partitions::addAll);
		headPartitions.clear();

		output.collect(new StreamRecord<>(new PartitionCommitInfo(
				checkpointId,
				getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks(),
				new ArrayList<>(partitions))));
	}
}
