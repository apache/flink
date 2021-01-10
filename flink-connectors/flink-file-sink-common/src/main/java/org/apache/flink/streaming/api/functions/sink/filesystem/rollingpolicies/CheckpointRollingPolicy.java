/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

import java.io.IOException;

/** An abstract {@link RollingPolicy} which rolls on every checkpoint. */
@PublicEvolving
public abstract class CheckpointRollingPolicy<IN, BucketID> implements RollingPolicy<IN, BucketID> {
    public boolean shouldRollOnCheckpoint(PartFileInfo<BucketID> partFileState) {
        return true;
    }

    public abstract boolean shouldRollOnEvent(
            final PartFileInfo<BucketID> partFileState, IN element) throws IOException;

    public abstract boolean shouldRollOnProcessingTime(
            final PartFileInfo<BucketID> partFileState, final long currentTime) throws IOException;

    /** The base abstract builder class for {@link CheckpointRollingPolicy}. */
    public abstract static class PolicyBuilder<
            IN, BucketID, T extends PolicyBuilder<IN, BucketID, T>> {
        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }

        public abstract CheckpointRollingPolicy<IN, BucketID> build();
    }
}
