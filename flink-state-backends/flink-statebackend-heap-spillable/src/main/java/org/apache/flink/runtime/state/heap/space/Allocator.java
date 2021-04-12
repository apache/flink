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

package org.apache.flink.runtime.state.heap.space;

import java.io.Closeable;

/** Implementations are responsible for allocate space. */
public interface Allocator extends Closeable {

    /**
     * Allocate space with the given size.
     *
     * @param size size of space to allocate.
     * @return address of the allocated space.
     * @throws Exception This method will throw exception if failed to allocate space.
     */
    long allocate(int size) throws Exception;

    /**
     * Free the space with the given address.
     *
     * @param address address of the space to free.
     */
    void free(long address);

    /**
     * Returns the chunk with the given chunk id.
     *
     * @param chunkId id of the chunk.
     * @return chunk with the given id.
     */
    Chunk getChunkById(int chunkId);
}
