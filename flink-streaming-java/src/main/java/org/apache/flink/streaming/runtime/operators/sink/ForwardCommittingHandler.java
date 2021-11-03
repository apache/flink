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
import java.util.List;

/**
 * This committer handler simply forwards all committables downstream. It's used in {@link
 * SinkOperator} without committers but with downstream operators (in streaming, only global
 * committer on sink; in batch, committer or global committer present).
 *
 * @param <CommT> The input and output type of the {@link Committer}.
 */
@Internal
public class ForwardCommittingHandler<CommT> extends AbstractCommitterHandler<CommT, Void> {
    public ForwardCommittingHandler() {}

    @Override
    List<Void> commitInternal(List<Void> committables) {
        throw new UnsupportedOperationException("This handler should never commit");
    }

    @Override
    public Collection<CommittableWrapper<CommT>> processCommittables(
            Collection<CommittableWrapper<CommT>> committables) {
        return committables;
    }

    /** The serializable factory of the handler. */
    public static class Factory<CommT>
            implements CommitterHandler.Factory<Sink<?, CommT, ?, ?>, CommT> {
        @Override
        public CommitterHandler<CommT> create(Sink<?, CommT, ?, ?> commTSink) throws IOException {
            return new ForwardCommittingHandler<>();
        }
    }
}
