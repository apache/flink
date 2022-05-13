/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.core.memory.DataInputView;

import javax.annotation.Nonnull;

import java.io.IOException;

/** Functional interface to read {@link StateMetaInfoSnapshot}. */
@FunctionalInterface
public interface StateMetaInfoReader {

    /**
     * Reads a snapshot from the given input view.
     *
     * @param inputView the input to read from.
     * @param userCodeClassLoader user classloader to deserialize the objects in the snapshot.
     * @return the deserialized snapshot.
     * @throws IOException on deserialization problems.
     */
    @Nonnull
    StateMetaInfoSnapshot readStateMetaInfoSnapshot(
            @Nonnull DataInputView inputView, @Nonnull ClassLoader userCodeClassLoader)
            throws IOException;
}
