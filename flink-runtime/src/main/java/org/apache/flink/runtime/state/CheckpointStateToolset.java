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

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.util.List;

/**
 * A toolset of operations that can be performed on a location embedded within the class. Created in
 * {@link CheckpointStorageWorkerView}.
 */
@Internal
public interface CheckpointStateToolset {

    /**
     * Tells if we can duplicate the given {@link StreamStateHandle}.
     *
     * <p>This should be a rather cheap operation, preferably not involving any remote accesses.
     *
     * @param stateHandle The handle to duplicate
     * @return true, if we can perform the duplication
     */
    boolean canFastDuplicate(StreamStateHandle stateHandle) throws IOException;

    /**
     * Duplicates {@link StreamStateHandle StreamStateHandles} into the path embedded inside of the
     * class.
     *
     * <p>You should first check if you can duplicate with {@link
     * #canFastDuplicate(StreamStateHandle)}.
     *
     * @param stateHandle The handles to duplicate
     * @return The duplicated handles
     */
    List<StreamStateHandle> duplicate(List<StreamStateHandle> stateHandle) throws IOException;
}
