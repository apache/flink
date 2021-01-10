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

package org.apache.flink.runtime.operators.resettable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SpillingBuffer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.util.ResettableMutableObjectIterator;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of a resettable iterator. While iterating the first time over the data, the
 * iterator writes the records to a spillable buffer. Any subsequent iteration re-reads the data
 * from that buffer.
 *
 * @param <T> The type of record that the iterator handles.
 */
public class SpillingResettableMutableObjectIterator<T>
        implements ResettableMutableObjectIterator<T> {

    private static final Logger LOG =
            LoggerFactory.getLogger(SpillingResettableMutableObjectIterator.class);

    // ------------------------------------------------------------------------

    protected DataInputView inView;

    protected final TypeSerializer<T> serializer;

    private long elementCount;

    private long currentElementNum;

    protected final SpillingBuffer buffer;

    protected final MutableObjectIterator<T> input;

    protected final MemoryManager memoryManager;

    private final List<MemorySegment> memorySegments;

    private final boolean releaseMemoryOnClose;

    // ------------------------------------------------------------------------

    public SpillingResettableMutableObjectIterator(
            MutableObjectIterator<T> input,
            TypeSerializer<T> serializer,
            MemoryManager memoryManager,
            IOManager ioManager,
            int numPages,
            AbstractInvokable parentTask)
            throws MemoryAllocationException {
        this(
                input,
                serializer,
                memoryManager,
                ioManager,
                memoryManager.allocatePages(parentTask, numPages),
                true);
    }

    public SpillingResettableMutableObjectIterator(
            MutableObjectIterator<T> input,
            TypeSerializer<T> serializer,
            MemoryManager memoryManager,
            IOManager ioManager,
            List<MemorySegment> memory) {
        this(input, serializer, memoryManager, ioManager, memory, false);
    }

    private SpillingResettableMutableObjectIterator(
            MutableObjectIterator<T> input,
            TypeSerializer<T> serializer,
            MemoryManager memoryManager,
            IOManager ioManager,
            List<MemorySegment> memory,
            boolean releaseMemOnClose) {
        this.memoryManager = memoryManager;
        this.input = input;
        this.serializer = serializer;
        this.memorySegments = memory;
        this.releaseMemoryOnClose = releaseMemOnClose;

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating spilling resettable iterator with "
                            + memory.size()
                            + " pages of memory.");
        }

        this.buffer =
                new SpillingBuffer(
                        ioManager,
                        new ListMemorySegmentSource(memory),
                        memoryManager.getPageSize());
    }

    public void open() {}

    @Override
    public void reset() throws IOException {
        this.inView = this.buffer.flip();
        this.currentElementNum = 0;
    }

    public List<MemorySegment> close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Spilling Resettable Iterator closing. Stored "
                            + this.elementCount
                            + " records.");
        }

        this.inView = null;

        final List<MemorySegment> memory = this.buffer.close();
        memory.addAll(this.memorySegments);
        this.memorySegments.clear();

        if (this.releaseMemoryOnClose) {
            this.memoryManager.release(memory);
            return Collections.emptyList();
        } else {
            return memory;
        }
    }

    @Override
    public T next(T reuse) throws IOException {
        if (this.inView != null) {
            // reading, any subsequent pass
            if (this.currentElementNum < this.elementCount) {
                try {
                    reuse = this.serializer.deserialize(reuse, this.inView);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "SpillingIterator: Error reading element from buffer.", e);
                }
                this.currentElementNum++;
                return reuse;
            } else {
                return null;
            }
        } else {
            // writing pass (first)
            if ((reuse = this.input.next(reuse)) != null) {
                try {
                    this.serializer.serialize(reuse, this.buffer);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "SpillingIterator: Error writing element to buffer.", e);
                }
                this.elementCount++;
                return reuse;
            } else {
                return null;
            }
        }
    }

    @Override
    public T next() throws IOException {
        T result = null;
        if (this.inView != null) {
            // reading, any subsequent pass
            if (this.currentElementNum < this.elementCount) {
                try {
                    result = this.serializer.deserialize(this.inView);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "SpillingIterator: Error reading element from buffer.", e);
                }
                this.currentElementNum++;
                return result;
            } else {
                return null;
            }
        } else {
            // writing pass (first)
            if ((result = this.input.next()) != null) {
                try {
                    this.serializer.serialize(result, this.buffer);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "SpillingIterator: Error writing element to buffer.", e);
                }
                this.elementCount++;
                return result;
            } else {
                return null;
            }
        }
    }

    public void consumeAndCacheRemainingData() throws IOException {
        // check that we are in the first pass and that more input data is left
        if (this.inView == null) {
            T holder = this.serializer.createInstance();

            while ((holder = this.input.next(holder)) != null) {
                try {
                    this.serializer.serialize(holder, this.buffer);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "SpillingIterator: Error writing element to buffer.", e);
                }
                this.elementCount++;
            }
        }
    }
}
