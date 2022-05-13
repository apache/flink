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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * A buffer which encapsulates the logic of dealing with the response from the {@link
 * CollectSinkFunction}. It will consider the checkpoint related fields in the response. See Java
 * doc of {@link CollectSinkFunction} for explanation of this communication protocol.
 */
public class CheckpointedCollectResultBuffer<T> extends AbstractCollectResultBuffer<T> {

    public CheckpointedCollectResultBuffer(TypeSerializer<T> serializer) {
        super(serializer);
    }

    @Override
    protected void sinkRestarted(long lastCheckpointedOffset) {
        // sink restarted, we revert back to where the sink tells us
        revert(lastCheckpointedOffset);
    }

    @Override
    protected void maintainVisibility(long currentVisiblePos, long lastCheckpointedOffset) {
        if (currentVisiblePos < lastCheckpointedOffset) {
            // lastCheckpointedOffset increases, this means that more results have been
            // checkpointed, and we can give these results to the user
            makeResultsVisible(lastCheckpointedOffset);
        }
    }
}
