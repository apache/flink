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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Experimental;

/** The message send from {@code SinkWriter} to {@code Committer}. */
@Experimental
public interface CommittableMessage<CommT> {
    /**
     * Special value for checkpointId for the end of input in case of batch commit or final
     * checkpoint.
     */
    long EOI = Long.MAX_VALUE;

    /** The subtask that created this committable. */
    int getSubtaskId();

    /**
     * Returns the checkpoint id or EOI if this message belong to the final checkpoint or the batch
     * commit.
     */
    long getCheckpointIdOrEOI();
}
