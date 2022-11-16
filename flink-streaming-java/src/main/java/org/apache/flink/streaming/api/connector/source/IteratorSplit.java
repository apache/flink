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
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;

import java.util.Iterator;

/** A split of the {@link IteratorSource}, backed by serializable iterator. */
@Internal
public class IteratorSplit<E> implements IteratorSourceSplit<E, Iterator<E>> {
    private static final String SPLIT_ID = "0";

    private final Iterator<E> iterator;

    public IteratorSplit(Iterator<E> iterator) {
        this.iterator = iterator;
    }

    @Override
    public String splitId() {
        return SPLIT_ID;
    }

    @Override
    public Iterator<E> getIterator() {
        return iterator;
    }

    @Override
    public IteratorSourceSplit<E, Iterator<E>> getUpdatedSplitForIterator(Iterator<E> iterator) {
        return new IteratorSplit<>(iterator);
    }
}
