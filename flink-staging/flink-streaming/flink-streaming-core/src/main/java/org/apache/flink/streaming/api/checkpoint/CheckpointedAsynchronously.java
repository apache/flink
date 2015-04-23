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

package org.apache.flink.streaming.api.checkpoint;

import java.io.Serializable;

/**
 * This interface marks a function/operator as <i>asynchronously checkpointed</i>.
 * Similar to the {@link Checkpointed} interface, the function must produce a
 * snapshot of its state. However, the function must be able to continue working
 * and mutating its state without mutating the returned state snapshot.
 * 
 * <p>Asynchronous checkpoints are desirable, because they allow the data streams at the
 * point of the checkpointed function/operator to continue running while the checkpoint
 * is in progress.</p>
 * 
 * <p>To be able to support asynchronous snapshots, the state returned by the
 * {@link #snapshotState(long, long)} method is typically a copy or shadow copy
 * of the actual state.</p>
 */
public interface CheckpointedAsynchronously<T extends Serializable> extends Checkpointed<T> {}
