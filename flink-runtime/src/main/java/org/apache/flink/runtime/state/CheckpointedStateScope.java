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

package org.apache.flink.runtime.state;

/**
 * The scope for a chunk of checkpointed state. Defines whether state is owned by one checkpoint, or
 * whether it is shared by multiple checkpoints.
 *
 * <p>Different checkpoint storage implementations may treat checkpointed state of different scopes
 * differently, for example put it into different folders or tables.
 */
public enum CheckpointedStateScope {

    /** Exclusive state belongs exclusively to one specific checkpoint / savepoint. */
    EXCLUSIVE,

    /**
     * Shared state may belong to more than one checkpoint.
     *
     * <p>Shared state is typically used for incremental or differential checkpointing methods,
     * where only deltas are written, and state from prior checkpoints is referenced in newer
     * checkpoints as well.
     */
    SHARED
}
