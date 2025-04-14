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

package org.apache.flink.runtime.asyncprocessing;

/**
 * A container which is used to hold {@link AsyncRequest}s. The role of {@code
 * AsyncRequestContainer} is to serve as an intermediary carrier for data transmission between the
 * runtime layer and the state layer. It stores the stateRequest from the runtime layer, which is
 * then processed by the state layer.
 *
 * <p>Notice that the {@code AsyncRequestContainer} may not be thread-safe.
 */
public interface AsyncRequestContainer<REQUEST extends AsyncRequest<?>> {

    /** Preserve a stateRequest into the {@code AsyncRequestContainer}. */
    void offer(REQUEST stateRequest);

    /** Returns whether the container is empty. */
    boolean isEmpty();
}
