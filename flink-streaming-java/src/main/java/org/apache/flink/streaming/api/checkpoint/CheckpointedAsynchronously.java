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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * This interface marks a function/operator as checkpointed similar to the
 * {@link Checkpointed} interface, but gives the Flink framework the option to
 * perform the checkpoint asynchronously. Note that asynchronous checkpointing for 
 * this interface has not been implemented.
 * 
 * <h1>Deprecation and Replacement</h1>
 * 
 * The shortcut replacement for this interface is via {@link ListCheckpointed} and works
 * as shown in the example below. Please refer to the JavaDocs of {@link ListCheckpointed} for
 * a more detailed description of how to use the new interface.
 * 
 * <pre>{@code
 * public class ExampleFunction<T> implements MapFunction<T, T>, ListCheckpointed<Integer> {
 * 
 *     private int count;
 *
 *     public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
 *         return Collections.singletonList(this.count);
 *     }
 *
 *     public void restoreState(List<Integer> state) throws Exception {
 *         this.value = state.count.isEmpty() ? 0 : state.get(0);
 *     }
 * 
 *     public T map(T value) {
 *         count++;
 *         return value;
 *     }
 * }
 * }</pre>
 *
 * @deprecated Please use {@link ListCheckpointed} and {@link CheckpointedFunction} instead,
 *             as illustrated in the example above.
 */
@Deprecated
@PublicEvolving
public interface CheckpointedAsynchronously<T extends Serializable> extends Checkpointed<T> {}
