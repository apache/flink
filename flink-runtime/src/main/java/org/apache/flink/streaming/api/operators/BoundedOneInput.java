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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface for one-input operators that need to be notified about the logical/semantical end of
 * input.
 *
 * <p><b>NOTE:</b> Classes should not implement both {@link BoundedOneInput} and {@link
 * BoundedMultiInput} at the same time!
 *
 * @see BoundedMultiInput
 * @see StreamOperator#finish()
 */
@PublicEvolving
public interface BoundedOneInput {

    /**
     * It is notified that no more data will arrive from the input.
     *
     * <p>Stateful operators need to be aware that a restart with rescaling may occur after
     * receiving this notification. A changed source split assignment may imply that the same
     * subtask of this operator that received endInput, has its state after endInput snapshotted,
     * and will receive new data after restart. Hence, the state should not contain any finalization
     * that would make it impossible to process new data.
     *
     * <p><b>WARNING:</b> It is not safe to use this method to commit any transactions or other side
     * effects! You can use this method to flush any buffered data that can later on be committed
     * e.g. in a {@link StreamOperator#notifyCheckpointComplete(long)}.
     *
     * <p><b>NOTE:</b> Given it is semantically very similar to the {@link StreamOperator#finish()}
     * method. It might be dropped in favour of the other method at some point in time.
     */
    void endInput() throws Exception;
}
