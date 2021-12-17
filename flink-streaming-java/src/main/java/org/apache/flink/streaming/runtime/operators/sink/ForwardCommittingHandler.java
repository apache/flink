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

import java.io.IOException;
import java.util.List;

/**
 * This committer handler simply forwards all committables downstream. It's used in {@link
 * SinkOperator} without committers but with downstream operators (in streaming, only global
 * committer on sink; in batch, committer or global committer present).
 */
class ForwardCommittingHandler<CommT> extends AbstractCommitterHandler<CommT, CommT, CommT> {
    ForwardCommittingHandler() {}

    @Override
    public List<CommT> processCommittables(List<CommT> committables) {
        return committables;
    }

    @Override
    protected void retry(List<CommT> recoveredCommittables)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException(
                "This handler should never receive recovered commits");
    }
}
