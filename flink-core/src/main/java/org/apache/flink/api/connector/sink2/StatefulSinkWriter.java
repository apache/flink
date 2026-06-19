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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Public;

import java.io.IOException;
import java.util.List;

/**
 * A {@link SinkWriter} whose state needs to be checkpointed.
 *
 * @param <InputT> The type of the sink writer's input
 * @param <WriterStateT> The type of the writer's state
 */
@Public
public interface StatefulSinkWriter<InputT, WriterStateT> extends SinkWriter<InputT> {
    /**
     * @return The writer's state.
     * @throws IOException if fail to snapshot writer's state.
     */
    List<WriterStateT> snapshotState(long checkpointId) throws IOException;
}
