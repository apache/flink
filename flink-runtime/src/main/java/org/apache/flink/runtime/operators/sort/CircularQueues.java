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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.WrappingRuntimeException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Collection of queues that are used for the communication between the threads.
 */
final class CircularQueues<E> implements StageRunner.StageMessageDispatcher<E> {
	final BlockingQueue<CircularElement<E>> empty;
	final BlockingQueue<CircularElement<E>> sort;
	final BlockingQueue<CircularElement<E>> spill;
	/**
	 * The iterator to be returned by the sort-merger. This variable is null, while receiving and merging is still in
	 * progress and it will be set once we have &lt; merge factor sorted sub-streams that will then be streamed sorted.
	 */
	private final CompletableFuture<MutableObjectIterator<E>> iteratorFuture = new CompletableFuture<>();

	public CircularQueues() {
		this.empty = new LinkedBlockingQueue<>();
		this.sort = new LinkedBlockingQueue<>();
		this.spill = new LinkedBlockingQueue<>();
	}

	private BlockingQueue<CircularElement<E>> getQueue(StageRunner.SortStage stage) {
		switch (stage) {
			case READ:
				return empty;
			case SPILL:
				return spill;
			case SORT:
				return sort;
			default:
				throw new IllegalArgumentException();
		}
	}

	public CompletableFuture<MutableObjectIterator<E>> getIteratorFuture() {
		return iteratorFuture;
	}

	@Override
	public void send(StageRunner.SortStage stage, CircularElement<E> element) {
		getQueue(stage).add(element);
	}

	@Override
	public void sendResult(MutableObjectIterator<E> result) {
		iteratorFuture.complete(result);
	}

	@Override
	public CircularElement<E> take(StageRunner.SortStage stage) {
		try {
			return getQueue(stage).take();
		} catch (InterruptedException e) {
			throw new WrappingRuntimeException(e);
		}
	}

	@Override
	public CircularElement<E> poll(StageRunner.SortStage stage) {
		return getQueue(stage).poll();
	}
}
