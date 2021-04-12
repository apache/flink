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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * Simple reader for integers, that is both a {@link StreamFormat.Reader} and a {@link
 * FileRecordFormat.Reader}.
 */
class TestIntReader implements StreamFormat.Reader<Integer>, FileRecordFormat.Reader<Integer> {

    private static final int SKIPS_PER_OFFSET = 7;

    private final FSDataInputStream in;
    private final DataInputStream din;

    private final long endOffset;
    private long currentOffset;
    private long currentSkipCount;

    private final boolean checkpointed;

    TestIntReader(FSDataInputStream in, long endOffset, boolean checkpointsPosition)
            throws IOException {
        this.in = in;
        this.endOffset = endOffset;
        this.currentOffset = in.getPos();
        this.din = new DataInputStream(in);
        this.checkpointed = checkpointsPosition;
    }

    @Nullable
    @Override
    public Integer read() throws IOException {
        if (in.getPos() >= endOffset) {
            return null;
        }

        try {
            final int next = din.readInt();
            incrementPosition();
            return next;
        } catch (EOFException e) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Nullable
    @Override
    public CheckpointedPosition getCheckpointedPosition() {
        return checkpointed ? new CheckpointedPosition(currentOffset, currentSkipCount) : null;
    }

    private void incrementPosition() {
        currentSkipCount++;
        if (currentSkipCount >= SKIPS_PER_OFFSET) {
            currentOffset += 4 * currentSkipCount;
            currentSkipCount = 0;
        }
    }
}
