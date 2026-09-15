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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Context passed to {@link OutputStreamOpener} describing the write operation, including metadata
 * to be persisted with the cloud object.
 *
 * @see OutputStreamOpener
 */
@Internal
@Experimental
@Immutable
public interface WriteContext {

    /**
     * Returns the metadata to attach to the cloud object (e.g., encryption headers).
     *
     * @return key-value metadata map; never {@code null}
     */
    Map<String, String> getMetadata();

    /** Shared empty context — avoids allocation for plain writes without metadata. */
    WriteContext EMPTY_WRITE_CONTEXT = Collections::emptyMap;

    /**
     * Creates a {@link WriteContext} backed by a defensive copy of the given metadata map.
     *
     * @param metadata the metadata to expose; must not be {@code null}
     * @return a {@link WriteContext} that returns an unmodifiable copy of {@code metadata}
     * @throws NullPointerException if {@code metadata} is {@code null}
     */
    static WriteContext of(final Map<String, String> metadata) {
        Preconditions.checkNotNull(metadata, "metadata");
        final Map<String, String> copy = Collections.unmodifiableMap(new HashMap<>(metadata));
        return () -> copy;
    }
}
