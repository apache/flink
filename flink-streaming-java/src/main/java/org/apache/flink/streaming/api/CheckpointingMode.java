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

package org.apache.flink.streaming.api;

import org.apache.flink.annotation.Public;

/**
 * The checkpointing mode defines what consistency guarantees the system gives in the presence of
 * failures.
 *
 * <p>When checkpointing is activated, the data streams are replayed such that lost parts of the
 * processing are repeated. For stateful operations and functions, the checkpointing mode defines
 * whether the system draws checkpoints such that a recovery behaves as if the operators/functions
 * see each record "exactly once" ({@link #EXACTLY_ONCE}), or whether the checkpoints are drawn in a
 * simpler fashion that typically encounters some duplicates upon recovery ({@link #AT_LEAST_ONCE})
 */
@Public
public enum CheckpointingMode {

    /**
     * Sets the checkpointing mode to "exactly once". This mode means that the system will
     * checkpoint the operator and user function state in such a way that, upon recovery, every
     * record will be reflected exactly once in the operator state.
     *
     * <p>For example, if a user function counts the number of elements in a stream, this number
     * will consistently be equal to the number of actual elements in the stream, regardless of
     * failures and recovery.
     *
     * <p>Note that this does not mean that each record flows through the streaming data flow only
     * once. It means that upon recovery, the state of operators/functions is restored such that the
     * resumed data streams pick up exactly at after the last modification to the state.
     *
     * <p>Note that this mode does not guarantee exactly-once behavior in the interaction with
     * external systems (only state in Flink's operators and user functions). The reason for that is
     * that a certain level of "collaboration" is required between two systems to achieve
     * exactly-once guarantees. However, for certain systems, connectors can be written that
     * facilitate this collaboration.
     *
     * <p>This mode sustains high throughput. Depending on the data flow graph and operations, this
     * mode may increase the record latency, because operators need to align their input streams, in
     * order to create a consistent snapshot point. The latency increase for simple dataflows (no
     * repartitioning) is negligible. For simple dataflows with repartitioning, the average latency
     * remains small, but the slowest records typically have an increased latency.
     */
    EXACTLY_ONCE,

    /**
     * Sets the checkpointing mode to "at least once". This mode means that the system will
     * checkpoint the operator and user function state in a simpler way. Upon failure and recovery,
     * some records may be reflected multiple times in the operator state.
     *
     * <p>For example, if a user function counts the number of elements in a stream, this number
     * will equal to, or larger, than the actual number of elements in the stream, in the presence
     * of failure and recovery.
     *
     * <p>This mode has minimal impact on latency and may be preferable in very-low latency
     * scenarios, where a sustained very-low latency (such as few milliseconds) is needed, and where
     * occasional duplicate messages (on recovery) do not matter.
     */
    AT_LEAST_ONCE
}
