/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

import static org.apache.flink.util.IOUtils.closeAll;

/**
 * Executes {@link ChannelStateWriteRequest}s in a separate thread. Any exception occurred during
 * execution causes this thread to stop and the exception to be re-thrown on any subsequent call.
 */
@ThreadSafe
class ChannelStateWriteRequestExecutorImpl implements ChannelStateWriteRequestExecutor {

    private static final Logger LOG =
            LoggerFactory.getLogger(ChannelStateWriteRequestExecutorImpl.class);

    private final ChannelStateWriteRequestDispatcher dispatcher;
    private final BlockingDeque<ChannelStateWriteRequest> deque;
    private final Thread thread;
    private volatile Exception thrown = null;
    private volatile boolean wasClosed = false;
    private final String taskName;

    ChannelStateWriteRequestExecutorImpl(
            String taskName, ChannelStateWriteRequestDispatcher dispatcher) {
        this(taskName, dispatcher, new LinkedBlockingDeque<>());
    }

    ChannelStateWriteRequestExecutorImpl(
            String taskName,
            ChannelStateWriteRequestDispatcher dispatcher,
            BlockingDeque<ChannelStateWriteRequest> deque) {
        this.taskName = taskName;
        this.dispatcher = dispatcher;
        this.deque = deque;
        this.thread = new Thread(this::run, "Channel state writer " + taskName);
        this.thread.setDaemon(true);
    }

    @VisibleForTesting
    void run() {
        try {
            loop();
        } catch (Exception ex) {
            thrown = ex;
        } finally {
            try {
                closeAll(
                        this::cleanupRequests,
                        () ->
                                dispatcher.fail(
                                        thrown == null ? new CancellationException() : thrown));
            } catch (Exception e) {
                //noinspection NonAtomicOperationOnVolatileField
                thrown = ExceptionUtils.firstOrSuppressed(e, thrown);
            }
        }
        LOG.debug("{} loop terminated", taskName);
    }

    private void loop() throws Exception {
        while (!wasClosed) {
            try {
                dispatcher.dispatch(deque.take());
            } catch (InterruptedException e) {
                if (!wasClosed) {
                    LOG.debug(
                            taskName
                                    + " interrupted while waiting for a request (continue waiting)",
                            e);
                } else {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void cleanupRequests() throws Exception {
        Throwable cause = thrown == null ? new CancellationException() : thrown;
        List<ChannelStateWriteRequest> drained = new ArrayList<>();
        deque.drainTo(drained);
        LOG.info("{} discarding {} drained requests", taskName, drained.size());
        closeAll(
                drained.stream()
                        .<AutoCloseable>map(request -> () -> request.cancel(cause))
                        .collect(Collectors.toList()));
    }

    @Override
    public void start() throws IllegalStateException {
        this.thread.start();
    }

    @Override
    public void submit(ChannelStateWriteRequest request) throws Exception {
        submitInternal(request, () -> deque.add(request));
    }

    @Override
    public void submitPriority(ChannelStateWriteRequest request) throws Exception {
        submitInternal(request, () -> deque.addFirst(request));
    }

    private void submitInternal(ChannelStateWriteRequest request, RunnableWithException action)
            throws Exception {
        try {
            action.run();
        } catch (Exception ex) {
            request.cancel(ex);
            throw ex;
        }
        ensureRunning();
    }

    private void ensureRunning() throws Exception {
        // this check should be performed *at least after* enqueuing a request
        // checking before is not enough because (check + enqueue) is not atomic
        if (wasClosed || !thread.isAlive()) {
            cleanupRequests();
            throw ExceptionUtils.firstOrSuppressed(
                    new IllegalStateException("not running"), thrown);
        }
    }

    @Override
    public void close() throws IOException {
        wasClosed = true;
        while (thread.isAlive()) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                if (!thread.isAlive()) {
                    Thread.currentThread().interrupt();
                }
                LOG.debug(taskName + " interrupted while waiting for the writer thread to die", e);
            }
        }
        if (thrown != null) {
            throw new IOException(thrown);
        }
    }

    @VisibleForTesting
    Thread getThread() {
        return thread;
    }
}
