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

import java.util.Objects;

/**
 * Position of a spill entry inside a {@link FilteredSpillFile}: the index of the owning physical
 * file in the spill file's {@code readers} list and the absolute byte offset within that file.
 *
 * <p>Total ordering is lexicographic on {@code (fileIndex, offset)} — files are drained in list
 * order, and entries within a file are drained in offset-ascending order, so this matches the
 * actual FIFO drain sequence. {@link #END} is the sentinel that compares strictly greater than any
 * real position; it is used as the {@code drainHead} once every entry has been drained.
 */
@Internal
public final class EntryPosition implements Comparable<EntryPosition> {

    /** Sentinel position that compares greater than every real entry position. */
    public static final EntryPosition END = new EntryPosition(Integer.MAX_VALUE, Long.MAX_VALUE);

    private final int fileIndex;
    private final long offset;

    public EntryPosition(int fileIndex, long offset) {
        this.fileIndex = fileIndex;
        this.offset = offset;
    }

    public int getFileIndex() {
        return fileIndex;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public int compareTo(EntryPosition other) {
        int byFile = Integer.compare(this.fileIndex, other.fileIndex);
        if (byFile != 0) {
            return byFile;
        }
        return Long.compare(this.offset, other.offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EntryPosition)) {
            return false;
        }
        EntryPosition that = (EntryPosition) o;
        return fileIndex == that.fileIndex && offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileIndex, offset);
    }

    @Override
    public String toString() {
        if (this == END) {
            return "EntryPosition{END}";
        }
        return "EntryPosition{fileIndex=" + fileIndex + ", offset=" + offset + "}";
    }
}
