/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * Sealed container for fetched recovered channel-state data.
 *
 * <p>FLINK-38544 transitional in-memory placeholder: the in-memory recovery backend keeps all
 * recovered buffers inside the physical channels' own queues (pushed in one shot at conversion
 * time), so there is no spill file and nothing to hand out or clean up here. This container carries
 * only the "there is state to recover" signal that {@link
 * SequentialChannelStateReader#readInputData} returns to the {@code StreamTask} recovery link; the
 * lifecycle and file APIs are no-ops until the spilling backend lands and replaces this with a
 * real, file-backed container.
 */
@Internal
public final class FetchedChannelState implements Closeable {

    private volatile boolean closed = false;

    FetchedChannelState() {}

    /** Returns the ordered list of spill file paths; empty for the in-memory backend. */
    public List<Path> files() {
        return Collections.emptyList();
    }

    /** Acquires a lifecycle grant; no-op for the in-memory backend (no files to keep alive). */
    public void acquire() {}

    /** Releases a lifecycle grant; no-op for the in-memory backend (no files to delete). */
    public void release() throws IOException {}

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @VisibleForTesting
    public boolean isClosed() {
        return closed;
    }
}
