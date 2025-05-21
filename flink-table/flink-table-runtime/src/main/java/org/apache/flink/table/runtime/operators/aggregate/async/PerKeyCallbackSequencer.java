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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Sequences callbacks on a per-key basis, so that different keys are allowed to pass concurrently,
 * but same keys are sequenced serially.
 *
 * @param <D> Metadata passed along to calls. Useful for passing arguments to callbacks.
 * @param <C> Context object which has a timer and mailbox available.
 */
public class PerKeyCallbackSequencer<D, C extends KeyedAsyncFunction.OpenContext>
        implements CallbackSequencer<D, C> {
    private final Callback<D, C> callback;
    private final Map<RowData, PriorityQueue<Data>> waitList;

    public PerKeyCallbackSequencer(Callback<D, C> callback) {
        this.callback = callback;
        this.waitList = new HashMap<>();
    }

    @Override
    public void callbackWhenNext(C ctx, long timestamp) throws Exception {
        callbackWhenNext(ctx, timestamp, null);
    }

    @Override
    public void callbackWhenNext(C ctx, long timestamp, D metadata) throws Exception {
        PriorityQueue<Data> result =
                waitList.compute(
                        ctx.currentKey(),
                        (k, v) -> {
                            if (v == null) {
                                v = new PriorityQueue<>();
                            }
                            v.add(new Data(timestamp, metadata));
                            return v;
                        });
        if (result.size() == 1) {
            runNextWaiter(ctx, result);
        }
    }

    @Override
    public void notifyNextWaiter(C ctx) throws Exception {
        PriorityQueue<Data> pq = waitList.get(ctx.currentKey());
        pq.poll();
        if (!pq.isEmpty()) {
            runNextWaiter(ctx, pq);
        } else {
            waitList.remove(ctx.currentKey());
        }
    }

    private void runNextWaiter(C ctx, PriorityQueue<Data> pq) throws Exception {
        if (!pq.isEmpty()) {
            RowData key = ctx.currentKey();
            Data data = pq.peek();

            ctx.setCurrentKey(key);
            callback.callback(data.timestamp, data.data, ctx);
        }
    }

    private class Data implements Comparable<Data> {
        private final long timestamp;
        private final D data;

        public Data(long timestamp, D data) {
            this.timestamp = timestamp;
            this.data = data;
        }

        @Override
        public int compareTo(PerKeyCallbackSequencer<D, C>.Data o) {
            return Long.compare(timestamp, o.timestamp);
        }
    }
}
