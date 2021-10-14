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

package org.apache.flink.table.runtime.io;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple iterator over the input read though an I/O channel. Use {@link
 * BinaryRowDataSerializer#deserializeFromPages}.
 */
public class BinaryRowChannelInputViewIterator implements MutableObjectIterator<BinaryRowData> {
    private final ChannelReaderInputView inView;

    private final BinaryRowDataSerializer serializer;

    private final List<MemorySegment> freeMemTarget;

    public BinaryRowChannelInputViewIterator(
            ChannelReaderInputView inView, BinaryRowDataSerializer serializer) {
        this(inView, new ArrayList<>(), serializer);
    }

    public BinaryRowChannelInputViewIterator(
            ChannelReaderInputView inView,
            List<MemorySegment> freeMemTarget,
            BinaryRowDataSerializer serializer) {
        this.inView = inView;
        this.freeMemTarget = freeMemTarget;
        this.serializer = serializer;
    }

    @Override
    public BinaryRowData next(BinaryRowData reuse) throws IOException {
        try {
            return this.serializer.deserializeFromPages(reuse, this.inView);
        } catch (EOFException eofex) {
            final List<MemorySegment> freeMem = this.inView.close();
            if (this.freeMemTarget != null) {
                this.freeMemTarget.addAll(freeMem);
            }
            return null;
        }
    }

    @Override
    public BinaryRowData next() throws IOException {
        throw new UnsupportedOperationException(
                "This method is disabled due to performance issue!");
    }
}
