/**
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


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.Configuration;

/**
 * A stateful streaming source that emits each number from a given interval exactly once,
 * possibly in parallel.
 */
public class StatefulSequenceSource extends RichParallelSourceFunction<Long> {
	private static final long serialVersionUID = 1L;

	private final long start;
	private final long end;

	private OperatorState<Long> collected;

	private volatile boolean isRunning = true;

	/**
	 * Creates a source that emits all numbers from the given interval exactly once.
	 *
	 * @param start Start of the range of numbers to emit.
	 * @param end End of the range of numbers to emit.
	 */
	public StatefulSequenceSource(long start, long end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public void run(SourceContext<Long> ctx) throws Exception {
		final Object checkpointLock = ctx.getCheckpointLock();

		RuntimeContext context = getRuntimeContext();

		final long stepSize = context.getNumberOfParallelSubtasks();
		final long congruence = start + context.getIndexOfThisSubtask();

		final long toCollect =
				((end - start + 1) % stepSize > (congruence - start)) ?
					((end - start + 1) / stepSize + 1) :
					((end - start + 1) / stepSize);
					
		Long currentCollected = collected.getState();

		while (isRunning && currentCollected < toCollect) {
			synchronized (checkpointLock) {
				ctx.collect(currentCollected * stepSize + congruence);
				collected.updateState(currentCollected + 1);
			}
			currentCollected = collected.getState();
		}
	}
	
	@Override
	public void open(Configuration conf){
		collected = getRuntimeContext().getOperatorState("collected", 0L, false);
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
