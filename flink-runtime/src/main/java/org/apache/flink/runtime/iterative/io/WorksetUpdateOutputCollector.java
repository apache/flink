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

package org.apache.flink.runtime.iterative.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * A {@link Collector} to update the iteration workset (partial solution for bulk iterations).
 *
 * <p>The records are written to a {@link DataOutputView} to allow in-memory data exchange.
 */
public class WorksetUpdateOutputCollector<T> implements Collector<T> {

    private final TypeSerializer<T> serializer;

    private final DataOutputView outputView;

    private long elementsCollected;

    private Collector<T> delegate;

    public WorksetUpdateOutputCollector(DataOutputView outputView, TypeSerializer<T> serializer) {
        this(outputView, serializer, null);
    }

    public WorksetUpdateOutputCollector(
            DataOutputView outputView, TypeSerializer<T> serializer, Collector<T> delegate) {
        this.outputView = outputView;
        this.serializer = serializer;
        this.delegate = delegate;

        this.elementsCollected = 0;
    }

    @Override
    public void collect(T record) {
        try {
            this.serializer.serialize(record, this.outputView);

            if (delegate != null) {
                delegate.collect(record);
            }

            this.elementsCollected++;
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize the record", e);
        }
    }

    public long getElementsCollectedAndReset() {
        long elementsCollectedToReturn = elementsCollected;
        elementsCollected = 0;
        return elementsCollectedToReturn;
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
