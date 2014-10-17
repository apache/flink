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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.util.event.EventListener;

import static com.google.common.base.Preconditions.checkState;

// TODO Refactor this and the BufferFuture to a "FlinkFuture"
public class MemorySegmentFuture {

	private final Object monitor = new Object();

	private MemorySegment memorySegment;

	private EventListener<MemorySegmentFuture> listener;

	private boolean isCancelled;

	public MemorySegmentFuture() {
	}

	public MemorySegmentFuture(MemorySegment memorySegment) {
		synchronized (monitor) {
			this.memorySegment = memorySegment;
		}
	}

	public MemorySegment getMemorySegment() {
		synchronized (monitor) {
			return memorySegment;
		}
	}

	public MemorySegmentFuture waitForMemorySegment() throws InterruptedException {
		synchronized (monitor) {
			while (!isDone()) {
				monitor.wait();
			}

			return this;
		}
	}

	public MemorySegmentFuture addListener(EventListener<MemorySegmentFuture> listenerToAdd) {
		synchronized (monitor) {
			checkState(!isCancelled, "Future has already been cancelled.");
			checkState(listener == null, "Listener has already been added.");

			// Late registration
			if (memorySegment != null) {
				listenerToAdd.onEvent(this);
			}
			else {
				listener = listenerToAdd;
			}

			return this;
		}
	}

	public void cancel() {
		synchronized (monitor) {
			checkState(memorySegment == null, "Too late to cancel. Already handed a memory segment in for this future.");

			isCancelled = true;
			monitor.notifyAll();

			if (listener != null) {
				listener.onEvent(this);
			}
		}
	}

	public boolean isDone() {
		synchronized (monitor) {
			return memorySegment != null || isCancelled;
		}
	}

	public boolean isSuccess() {
		synchronized (monitor) {
			return memorySegment != null;
		}
	}

	public boolean isCancelled() {
		synchronized (monitor) {
			return isCancelled;
		}
	}

	void handInMemorySegment(MemorySegment memorySegmentToHandIn) {
		synchronized (monitor) {
			checkState(memorySegment == null, "Already handed a memory Segment in for this future.");

			memorySegment = memorySegmentToHandIn;

			if (listener != null) {
				listener.onEvent(this);
			}

			monitor.notifyAll();
		}
	}
}

