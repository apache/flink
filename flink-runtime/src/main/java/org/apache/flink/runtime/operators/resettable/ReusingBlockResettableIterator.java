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

package org.apache.flink.runtime.operators.resettable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Implementation of an iterator that fetches a block of data into main memory and offers resettable
 * access to the data in that block.
 */
public class ReusingBlockResettableIterator<T> extends NonReusingBlockResettableIterator<T> {

    public static final Logger LOG = LoggerFactory.getLogger(ReusingBlockResettableIterator.class);

    private final T reuseElement;

    // ------------------------------------------------------------------------

    public ReusingBlockResettableIterator(
            MemoryManager memoryManager,
            Iterator<T> input,
            TypeSerializer<T> serializer,
            int numPages,
            AbstractInvokable ownerTask)
            throws MemoryAllocationException {
        this(memoryManager, serializer, numPages, ownerTask);
        this.input = input;
    }

    public ReusingBlockResettableIterator(
            MemoryManager memoryManager,
            TypeSerializer<T> serializer,
            int numPages,
            AbstractInvokable ownerTask)
            throws MemoryAllocationException {
        super(memoryManager, serializer, numPages, ownerTask);

        this.reuseElement = serializer.createInstance();
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean hasNext() {
        try {
            if (this.nextElement == null) {
                if (this.readPhase) {
                    // read phase, get next element from buffer
                    T tmp = getNextRecord(this.reuseElement);
                    if (tmp != null) {
                        this.nextElement = tmp;
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    if (this.input.hasNext()) {
                        final T next = this.input.next();
                        if (writeNextRecord(next)) {
                            this.nextElement = next;
                            return true;
                        } else {
                            this.leftOverElement = next;
                            return false;
                        }
                    } else {
                        this.noMoreBlocks = true;
                        return false;
                    }
                }
            } else {
                return true;
            }
        } catch (IOException ioex) {
            throw new RuntimeException(
                    "Error (de)serializing record in block resettable iterator.", ioex);
        }
    }
}
