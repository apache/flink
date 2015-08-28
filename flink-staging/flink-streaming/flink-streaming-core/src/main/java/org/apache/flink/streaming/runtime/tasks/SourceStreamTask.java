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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Task for executing streaming sources.
 *
 * One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the StreamFunction that it must only modify its state or emit elements in
 * a synchronized block that locks on the checkpointLock Object. Also, the modification of the state
 * and the emission of elements must happen in the same block of code that is protected by the
 * synchronized block.
 *
 * @param <OUT> Type of the output elements of this source.
 */
public class SourceStreamTask<OUT> extends StreamTask<OUT, StreamSource<OUT>> {

	@Override
	protected void init() {
		// does not hold any resources, so no initialization needed
	}

	@Override
	protected void cleanup() {
		// does not hold any resources, so no cleanup needed
	}
	

	@Override
	protected void run() throws Exception {
		final Object checkpointLock = getCheckpointLock();
		
		final SourceOutput<StreamRecord<OUT>> output = 
				new SourceOutput<StreamRecord<OUT>>(outputHandler.getOutput(), checkpointLock);
		
		streamOperator.run(checkpointLock, output);
	}
	
	@Override
	protected void cancelTask() throws Exception {
		streamOperator.cancel();
	}

	// ------------------------------------------------------------------------
	
	// TODO:
	// does this help with anything? The losk should be already held by the source function that
	// emits. If that one does not hold the lock, then this does not help either.
	
	private static class SourceOutput<T> implements Output<T> {
		
		private final Output<T> output;
		private final Object lockObject;

		public SourceOutput(Output<T> output, Object lockObject) {
			this.output = output;
			this.lockObject = lockObject;
		}

		@Override
		public void emitWatermark(Watermark mark) {
			synchronized (lockObject) {
				output.emitWatermark(mark);
			}
		}

		@Override
		public void collect(T record) {
			synchronized (lockObject) {
				output.collect(record);
			}
		}

		@Override
		public void close() {
			output.close();
		}
	}
}
