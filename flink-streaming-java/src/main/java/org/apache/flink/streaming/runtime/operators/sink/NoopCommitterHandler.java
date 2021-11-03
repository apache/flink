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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Sink;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/** Swallows all committables and emits nothing. */
@Internal
public enum NoopCommitterHandler implements CommitterHandler<Object> {
    INSTANCE;

    @SuppressWarnings("unchecked")
    public static <CommT> CommitterHandler<CommT> getInstance() {
        return (CommitterHandler<CommT>) NoopCommitterHandler.INSTANCE;
    }

    @Override
    public Collection<CommittableWrapper<Object>> processCommittables(
            Collection<CommittableWrapper<Object>> committables) {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean needsRetry() {
        return false;
    }

    @Override
    public Collection<CommittableWrapper<Object>> retry() throws IOException, InterruptedException {
        return Collections.emptyList();
    }

    /** The serializable factory of the handler. */
    public static class Factory<CommT>
            implements CommitterHandler.Factory<Sink<?, CommT, ?, ?>, CommT> {
        @Override
        public CommitterHandler<CommT> create(Sink<?, CommT, ?, ?> commTSink) throws IOException {
            return getInstance();
        }
    }
}
