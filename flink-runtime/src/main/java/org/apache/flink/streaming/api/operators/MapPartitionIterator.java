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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link MapPartitionIterator} is an iterator used in the {@link MapPartitionOperator}.The task
 * main thread will add records to it. It will set itself as the input parameter of {@link
 * MapPartitionFunction} and execute the function.
 */
@Internal
public class MapPartitionIterator<IN> implements Iterator<IN> {

    /**
     * Max number of caches.
     *
     * <p>The constant defines the maximum number of caches that can be created. Its value is set to
     * 100, which is considered sufficient for most parallel jobs. Each cache is a record and
     * occupies a minimal amount of memory so the value is not excessively large.
     */
    public static final int DEFAULT_MAX_CACHE_NUM = 100;

    /** The lock to ensure consistency between task main thread and udf executor. */
    private final Lock lock = new ReentrantLock();

    /** The queue to store record caches. */
    @GuardedBy("lock")
    private final Queue<IN> cacheQueue = new LinkedList<>();

    /** The condition to indicate the cache queue is not empty. */
    private final Condition cacheNotEmpty = lock.newCondition();

    /** The condition to indicate the cache queue is not full. */
    private final Condition cacheNotFull = lock.newCondition();

    /** The condition to indicate the udf is finished. */
    private final Condition udfFinish = lock.newCondition();

    /** The task udf executor. */
    private final ExecutorService udfExecutor;

    /** The flag to represent the finished state of udf. */
    @GuardedBy("lock")
    private boolean udfFinished = false;

    /** The flag to represent the closed state of this iterator. */
    @GuardedBy("lock")
    private boolean closed = false;

    public MapPartitionIterator(Consumer<Iterator<IN>> udf) {
        this.udfExecutor =
                Executors.newSingleThreadExecutor(new ExecutorThreadFactory("TaskUDFExecutor"));
        this.udfExecutor.execute(
                () -> {
                    udf.accept(this);
                    runWithLock(
                            () -> {
                                udfFinished = true;
                                udfFinish.signalAll();
                                cacheNotFull.signalAll();
                            });
                });
    }

    @Override
    public boolean hasNext() {
        return supplyWithLock(
                () -> {
                    if (cacheQueue.size() > 0) {
                        return true;
                    } else if (closed) {
                        return false;
                    } else {
                        waitCacheNotEmpty();
                        return hasNext();
                    }
                });
    }

    @Override
    public IN next() {
        return supplyWithLock(
                () -> {
                    IN record;
                    if (cacheQueue.size() > 0) {
                        if (!closed && cacheQueue.size() == DEFAULT_MAX_CACHE_NUM) {
                            cacheNotFull.signalAll();
                        }
                        record = cacheQueue.poll();
                        return record;
                    } else {
                        if (closed) {
                            return null;
                        }
                        waitCacheNotEmpty();
                        return cacheQueue.poll();
                    }
                });
    }

    public void addRecord(IN record) {
        runWithLock(
                () -> {
                    checkState(!closed);
                    if (udfFinished) {
                        return;
                    }
                    if (cacheQueue.size() < DEFAULT_MAX_CACHE_NUM) {
                        cacheQueue.add(record);
                        if (cacheQueue.size() == 1) {
                            cacheNotEmpty.signalAll();
                        }
                    } else {
                        waitCacheNotFull();
                        addRecord(record);
                    }
                });
    }

    public void close() {
        runWithLock(
                () -> {
                    closed = true;
                    if (!udfFinished) {
                        cacheNotEmpty.signalAll();
                        waitUDFFinished();
                    }
                    udfExecutor.shutdown();
                });
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    /** Wait until the cache is not empty. */
    private void waitCacheNotEmpty() {
        try {
            cacheNotEmpty.await();
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /** Wait until the cache is not full. */
    private void waitCacheNotFull() {
        try {
            cacheNotFull.await();
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /** Wait until the UDF is finished. */
    private void waitUDFFinished() {
        try {
            udfFinish.await();
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private void runWithLock(Runnable runnable) {
        try {
            lock.lock();
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    private <ANY> ANY supplyWithLock(Supplier<ANY> supplier) {
        ANY result;
        try {
            lock.lock();
            result = supplier.get();
        } finally {
            lock.unlock();
        }
        return result;
    }
}
