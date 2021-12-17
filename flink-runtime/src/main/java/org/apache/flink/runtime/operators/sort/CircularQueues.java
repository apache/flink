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

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MutableObjectIterator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Collection of queues that are used for the communication between the threads. */
final class CircularQueues<E> implements StageRunner.StageMessageDispatcher<E> {
    private final BlockingQueue<CircularElement<E>> empty;
    private final BlockingQueue<CircularElement<E>> sort;
    private final BlockingQueue<CircularElement<E>> spill;
    /**
     * The close and take methods might be called from multiple threads (reading, sorting, spilling,
     * ...), therefore it must be volatile.
     */
    private volatile boolean isFinished = false;

    /**
     * The iterator to be returned by the sort-merger. This variable is null, while receiving and
     * merging is still in progress and it will be set once we have &lt; merge factor sorted
     * sub-streams that will then be streamed sorted.
     */
    private final CompletableFuture<MutableObjectIterator<E>> iteratorFuture =
            new CompletableFuture<>();

    public CircularQueues() {
        this.empty = new LinkedBlockingQueue<>();
        this.sort = new LinkedBlockingQueue<>();
        this.spill = new LinkedBlockingQueue<>();
    }

    private BlockingQueue<CircularElement<E>> getQueue(StageRunner.SortStage stage) {
        switch (stage) {
            case READ:
                return empty;
            case SORT:
                return sort;
            case SPILL:
                return spill;
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
    public CircularElement<E> take(StageRunner.SortStage stage) throws InterruptedException {
        while (!isFinished) {
            CircularElement<E> value = getQueue(stage).poll(1, TimeUnit.SECONDS);
            if (value != null) {
                return value;
            }
        }
        throw new FlinkRuntimeException("The sorter is closed already");
    }

    @Override
    public CircularElement<E> poll(StageRunner.SortStage stage) {
        return getQueue(stage).poll();
    }

    @Override
    public void close() {
        this.isFinished = true;
    }
}
