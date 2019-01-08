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

package org.apache.flink.formats.parquet.testutils;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;

/**
 * A stream source that emits elements without allowing checkpoints and waits
 * for two more checkpoints to complete before exiting.
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class FiniteTestSource<T> implements SourceFunction<T>, CheckpointListener {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	private final Iterable<T> elements;

	private volatile boolean running = true;

	private transient int numCheckpointsComplete;

	@SafeVarargs
	public FiniteTestSource(T... elements) {
		this(Arrays.asList(elements));
	}

	public FiniteTestSource(Iterable<T> elements) {
		this.elements = elements;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		final Object lock = ctx.getCheckpointLock();
		final int checkpointToAwait;

		synchronized (lock) {
			checkpointToAwait = numCheckpointsComplete + 2;
			for (T t : elements) {
				ctx.collect(t);
			}
		}

		synchronized (lock) {
			while (running && numCheckpointsComplete < checkpointToAwait) {
				lock.wait(1);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		numCheckpointsComplete++;
	}
}
