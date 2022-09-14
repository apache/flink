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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/** An empty implementation of {@link CheckpointStateToolset}. */
public final class NotDuplicatingCheckpointStateToolset implements CheckpointStateToolset {
    @Override
    public boolean canFastDuplicate(StreamStateHandle stateHandle) throws IOException {
        return false;
    }

    @Override
    public List<StreamStateHandle> duplicate(List<StreamStateHandle> stateHandle)
            throws IOException {
        throw new UnsupportedEncodingException("The toolset does not support duplication");
    }
}
