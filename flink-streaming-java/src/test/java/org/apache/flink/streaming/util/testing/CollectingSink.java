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

package org.apache.flink.streaming.util.testing;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** Sink for collecting output during testing. */
public class CollectingSink<T> implements Sink<T> {
    private static final long serialVersionUID = 1L;
    private static final List<BlockingQueue<Object>> queues =
            Collections.synchronizedList(new ArrayList<>());
    private static final AtomicInteger numSinks = new AtomicInteger(-1);
    private final int index;

    public CollectingSink() {
        this.index = numSinks.incrementAndGet();
        queues.add(new LinkedBlockingQueue<>());
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return new CollectingElementWriter(index);
    }

    private class CollectingElementWriter implements SinkWriter<T> {
        private final int index;

        public CollectingElementWriter(int index) {
            this.index = index;
        }

        @Override
        public void write(T element, Context context) {
            queues.get(this.index).add(element);
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {}
    }

    @SuppressWarnings("unchecked")
    public List<T> getRemainingOutput() {
        return new ArrayList<>((BlockingQueue<T>) queues.get(this.index));
    }

    public boolean isEmpty() {
        return queues.get(this.index).isEmpty();
    }

    public T poll() throws TimeoutException {
        return this.poll(Duration.ofSeconds(15L));
    }

    @SuppressWarnings("unchecked")
    public T poll(Duration duration) throws TimeoutException {
        Object element;

        try {
            element = queues.get(this.index).poll(duration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException var4) {
            throw new RuntimeException(var4);
        }

        if (element == null) {
            throw new TimeoutException();
        } else {
            return (T) element;
        }
    }

    public void close() {
        queues.get(this.index).clear();
    }
}
