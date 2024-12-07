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
package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;

/** A wrapper that wraps {@link FileMergingSnapshotManager} and a {@link Closeable}. */
public class FileMergingSnapshotManagerClosableWrapper implements Closeable {

    private final FileMergingSnapshotManager snapshotManager;

    private final Closeable closeable;

    private boolean closed = false;

    private FileMergingSnapshotManagerClosableWrapper(
            @Nonnull FileMergingSnapshotManager snapshotManager, @Nonnull Closeable closeable) {
        this.snapshotManager = snapshotManager;
        this.closeable = closeable;
    }

    public static FileMergingSnapshotManagerClosableWrapper of(
            @Nonnull FileMergingSnapshotManager snapshotManager, @Nonnull Closeable closeable) {
        return new FileMergingSnapshotManagerClosableWrapper(snapshotManager, closeable);
    }

    public FileMergingSnapshotManager get() {
        return snapshotManager;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            closeable.close();
        }
    }
}
