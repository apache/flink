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

package org.apache.flink.streaming.api.connector.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.util.serialization.CollectionSerializer;
import org.apache.flink.streaming.util.serialization.SimpleObjectSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * A Source that reads elements from an {@link Iterator} and emits them.
 *
 * <p>Note: Parallelism of this source must be 1.
 */
@Internal
public class IteratorSource<E>
        implements Source<E, IteratorSplit<E>, Collection<IteratorSplit<E>>> {
    private final Iterator<E> iterator;

    /** Creates bounded {@link IteratorSource} from specified iterator. */
    public IteratorSource(Iterator<E> iterator) {
        this.iterator = iterator;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<E, IteratorSplit<E>> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new IteratorSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<IteratorSplit<E>, Collection<IteratorSplit<E>>> createEnumerator(
            SplitEnumeratorContext<IteratorSplit<E>> enumContext) throws Exception {
        IteratorSplit<E> split = new IteratorSplit<>(iterator);
        return new IteratorSourceEnumerator<>(enumContext, Collections.singletonList(split));
    }

    @Override
    public SplitEnumerator<IteratorSplit<E>, Collection<IteratorSplit<E>>> restoreEnumerator(
            SplitEnumeratorContext<IteratorSplit<E>> enumContext,
            Collection<IteratorSplit<E>> restoredSplits)
            throws Exception {
        Preconditions.checkArgument(
                restoredSplits.size() <= 1, "Parallelism of IteratorSource should be 1");
        return new IteratorSourceEnumerator<>(enumContext, restoredSplits);
    }

    @Override
    public SimpleVersionedSerializer<IteratorSplit<E>> getSplitSerializer() {
        return new IteratorSplitSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<Collection<IteratorSplit<E>>>
            getEnumeratorCheckpointSerializer() {
        return new CollectionSerializer<>(new IteratorSplitSerializer<>());
    }

    /** {@link IteratorSplit} serializer. */
    @Internal
    public static class IteratorSplitSerializer<E>
            extends SimpleObjectSerializer<IteratorSplit<E>> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public void serialize(IteratorSplit<E> obj, ObjectOutputStream outputStream)
                throws IOException {
            outputStream.writeObject(obj.getIterator());
        }

        @Override
        @SuppressWarnings("unchecked")
        public IteratorSplit<E> deserialize(ObjectInputStream inputStream)
                throws IOException, ClassNotFoundException {
            Iterator<E> iterator = (Iterator<E>) inputStream.readObject();
            return new IteratorSplit<>(iterator);
        }
    }
}
