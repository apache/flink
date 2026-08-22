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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.Immutable;

/**
 * Context passed to {@link InputStreamOpener} describing the desired read position.
 *
 * @see InputStreamOpener
 */
@Internal
@Experimental
@Immutable
public interface ReadContext {

    /**
     * Returns the byte offset at which the stream should start.
     *
     * @return byte offset; must be &ge; 0
     */
    long getPos();

    /**
     * Creates a {@link ReadContext} for the given byte position.
     *
     * @param pos byte offset; must be &ge; 0
     * @return a {@link ReadContext} that returns {@code pos}
     * @throws IllegalArgumentException if {@code pos} is negative
     */
    static ReadContext of(final long pos) {
        Preconditions.checkArgument(pos >= 0, "pos must be >= 0");
        return () -> pos;
    }
}
