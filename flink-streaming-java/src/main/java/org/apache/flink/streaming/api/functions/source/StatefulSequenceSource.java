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
package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * A stateful streaming source that emits each number from a given interval exactly once,
 * possibly in parallel.
 *
 * <p>For the source to be re-scalable, the range of elements to be emitted is initially (at the first execution)
 * split into {@code min(maxParallelism, totalNumberOfElements)} partitions, and for each one, we
 * store the {@code nextOffset}, i.e. the next element to be emitted, and its {@code end}. Upon rescaling, these
 * partitions can be reshuffled among the new tasks, and these will resume emitting from where their predecessors
 * left off.
 *
 * <p>Although each element will be emitted exactly-once, elements will not necessarily be emitted in ascending order,
 * even for the same task.
 */
@PublicEvolving
public class StatefulSequenceSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {
	
	private static final long serialVersionUID = 1L;

	private final long start;
	private final long end;

	private volatile boolean isRunning = true;

	private transient Map<Long, Long> endToNextOffsetMapping;
	private transient ListState<Tuple2<Long, Long>> checkpointedState;

	/**
	 * Creates a source that emits all numbers from the given interval exactly once.
	 *
	 * @param start Start of the range of numbers to emit.
	 * @param end End of the range of numbers to emit.
	 */
	public StatefulSequenceSource(long start, long end) {
		Preconditions.checkArgument(start <= end);
		this.start = start;
		this.end = end;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

		Preconditions.checkState(checkpointedState == null,
			"The " + getClass().getSimpleName() + " has already been initialized.");

		this.checkpointedState = context.getOperatorStateStore().getOperatorState(
			new ListStateDescriptor<>(
				"stateful-sequence-source-state", 
					new TupleSerializer<>(
							(Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class,
							new TypeSerializer<?>[] { LongSerializer.INSTANCE, LongSerializer.INSTANCE }
					)
			)
		);

		this.endToNextOffsetMapping = new HashMap<>();
		if (context.isRestored()) {
			for (Tuple2<Long, Long> partitionInfo: checkpointedState.get()) {
				Long prev = endToNextOffsetMapping.put(partitionInfo.f0, partitionInfo.f1);
				Preconditions.checkState(prev == null,
						getClass().getSimpleName() + " : Duplicate entry when restoring.");
			}
		} else {
			final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
			final int parallelTasks = getRuntimeContext().getNumberOfParallelSubtasks();

			final long totalElements = Math.abs(end - start + 1L);
			final int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
			final int totalPartitions = totalElements < Integer.MAX_VALUE ? Math.min(maxParallelism, (int) totalElements) : maxParallelism;

			Tuple2<Integer, Integer> localPartitionRange = getLocalRange(totalPartitions, parallelTasks, taskIdx);
			int localStartIdx = localPartitionRange.f0;
			int localEndIdx = localStartIdx + localPartitionRange.f1;

			for (int partIdx = localStartIdx; partIdx < localEndIdx; partIdx++) {
				Tuple2<Long, Long> limits = getPartitionLimits(totalElements, totalPartitions, partIdx);
				endToNextOffsetMapping.put(limits.f1, limits.f0);
			}
		}
	}

	private Tuple2<Integer, Integer> getLocalRange(int totalPartitions, int parallelTasks, int taskIdx) {
		int minPartitionSliceSize = totalPartitions / parallelTasks;
		int remainingPartitions = totalPartitions - minPartitionSliceSize * parallelTasks;

		int localRangeStartIdx = taskIdx * minPartitionSliceSize + Math.min(taskIdx, remainingPartitions);
		int localRangeSize = taskIdx < remainingPartitions ? minPartitionSliceSize + 1 : minPartitionSliceSize;

		return new Tuple2<>(localRangeStartIdx, localRangeSize);
	}

	private Tuple2<Long, Long> getPartitionLimits(long totalElements, int totalPartitions, long partitionIdx) {
		long minElementPartitionSize = totalElements / totalPartitions;
		long remainingElements = totalElements - minElementPartitionSize * totalPartitions;
		long startOffset = start;

		for (int idx = 0; idx < partitionIdx; idx++) {
			long partitionSize = idx < remainingElements ? minElementPartitionSize + 1L : minElementPartitionSize;
			startOffset += partitionSize;
		}

		long partitionSize = partitionIdx < remainingElements ? minElementPartitionSize + 1L : minElementPartitionSize;
		return new Tuple2<>(startOffset, startOffset + partitionSize);
	}

	@Override
	public void run(SourceContext<Long> ctx) throws Exception {
		for (Map.Entry<Long, Long> partition: endToNextOffsetMapping.entrySet()) {
			long endOffset = partition.getKey();
			long currentOffset = partition.getValue();

			while (isRunning && currentOffset < endOffset) {
				synchronized (ctx.getCheckpointLock()) {
					long toSend = currentOffset;
					endToNextOffsetMapping.put(endOffset, ++currentOffset);
					ctx.collect(toSend);
				}
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(checkpointedState != null,
			"The " + getClass().getSimpleName() + " state has not been properly initialized.");

		checkpointedState.clear();
		for (Map.Entry<Long, Long> entry : endToNextOffsetMapping.entrySet()) {
			checkpointedState.add(new Tuple2<>(entry.getKey(), entry.getValue()));
		}
	}
}
