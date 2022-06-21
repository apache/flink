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

package org.apache.flink.batch.connectors.cassandra;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.connectors.cassandra.utils.SinkUtils;
import org.apache.flink.util.Preconditions;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * OutputFormatBase is the common abstract class for output formats. It implements a flush mechanism
 * and has a maximum number of concurrent requests.
 *
 * @param <OUT> Type of the elements to write.
 */
public abstract class OutputFormatBase<OUT, V> extends RichOutputFormat<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(OutputFormatBase.class);

    private Semaphore semaphore;
    private Duration maxConcurrentRequestsTimeout = Duration.ofMillis(Long.MAX_VALUE);
    private int maxConcurrentRequests = Integer.MAX_VALUE;

    private transient FutureCallback<V> callback;
    private AtomicReference<Throwable> throwable;

    protected OutputFormatBase(int maxConcurrentRequests, Duration maxConcurrentRequestsTimeout) {
        Preconditions.checkArgument(
                maxConcurrentRequests > 0, "Max concurrent requests is expected to be positive");
        this.maxConcurrentRequests = maxConcurrentRequests;
        Preconditions.checkNotNull(
                maxConcurrentRequestsTimeout, "Max concurrent requests timeout cannot be null");
        Preconditions.checkArgument(
                !maxConcurrentRequestsTimeout.isNegative(),
                "Max concurrent requests timeout is expected to be positive");
        this.maxConcurrentRequestsTimeout = maxConcurrentRequestsTimeout;
    }

    /** Opens the format and initializes the flush system. */
    @Override
    public void open(int taskNumber, int numTasks) {
        throwable = new AtomicReference<>();
        this.semaphore = new Semaphore(maxConcurrentRequests);
        this.callback =
                new FutureCallback<V>() {
                    @Override
                    public void onSuccess(V ignored) {
                        semaphore.release();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        throwable.compareAndSet(null, t);
                        LOG.error("Error while writing value.", t);
                        semaphore.release();
                    }
                };
    }

    private void flush() throws IOException {
        tryAcquire(maxConcurrentRequests);
        semaphore.release(maxConcurrentRequests);
    }

    private void tryAcquire(int permits) throws IOException {
        try {
            SinkUtils.tryAcquire(
                    permits, maxConcurrentRequests, maxConcurrentRequestsTimeout, semaphore);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeRecord(OUT record) throws IOException {
        checkAsyncErrors();
        tryAcquire(1);
        final ListenableFuture<V> result;
        try {
            result = send(record);
        } catch (Throwable e) {
            semaphore.release();
            throw e;
        }
        Futures.addCallback(result, callback);
    }

    protected abstract ListenableFuture<V> send(OUT value);

    private void checkAsyncErrors() throws IOException {
        final Throwable currentError = throwable.getAndSet(null);
        if (currentError != null) {
            throw new IOException("Write record failed", currentError);
        }
    }

    /** Closes the format waiting for pending writes and reports errors. */
    @Override
    public void close() throws IOException {
        checkAsyncErrors();
        flush();
        checkAsyncErrors();
    }

    @VisibleForTesting
    int getAvailablePermits() {
        return semaphore.availablePermits();
    }

    @VisibleForTesting
    int getAcquiredPermits() {
        return maxConcurrentRequests - semaphore.availablePermits();
    }
}
