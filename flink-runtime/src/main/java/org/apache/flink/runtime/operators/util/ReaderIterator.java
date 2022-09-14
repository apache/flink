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

package org.apache.flink.runtime.operators.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.plugable.ReusingDeserializationDelegate;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * A {@link MutableObjectIterator} that wraps a reader from an input channel and produces the
 * reader's records.
 *
 * <p>The reader supports reading objects with possible reuse of mutable types, and without reuse of
 * mutable types.
 */
public class ReaderIterator<T> implements MutableObjectIterator<T> {

    private final MutableReader<DeserializationDelegate<T>> reader; // the source

    private final ReusingDeserializationDelegate<T> reusingDelegate;
    private final NonReusingDeserializationDelegate<T> nonReusingDelegate;

    /**
     * Creates a new iterator, wrapping the given reader.
     *
     * @param reader The reader to wrap.
     */
    public ReaderIterator(
            MutableReader<DeserializationDelegate<T>> reader, TypeSerializer<T> serializer) {
        this.reader = reader;
        this.reusingDelegate = new ReusingDeserializationDelegate<T>(serializer);
        this.nonReusingDelegate = new NonReusingDeserializationDelegate<T>(serializer);
    }

    @Override
    public T next(T target) throws IOException {
        this.reusingDelegate.setInstance(target);
        try {
            if (this.reader.next(this.reusingDelegate)) {
                return this.reusingDelegate.getInstance();
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            throw new IOException("Reader interrupted.", e);
        }
    }

    @Override
    public T next() throws IOException {
        try {
            if (this.reader.next(this.nonReusingDelegate)) {
                return this.nonReusingDelegate.getInstance();
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            throw new IOException("Reader interrupted.", e);
        }
    }
}
