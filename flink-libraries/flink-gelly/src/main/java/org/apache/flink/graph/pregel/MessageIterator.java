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

package org.apache.flink.graph.pregel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Either;
import org.apache.flink.types.NullValue;

import java.util.Iterator;

/**
 * An iterator that returns messages. The iterator is {@link java.lang.Iterable} at the same time to
 * support the <i>foreach</i> syntax.
 */
public final class MessageIterator<Message>
        implements Iterator<Message>, Iterable<Message>, java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private transient Iterator<Tuple2<?, Either<NullValue, Message>>> source;
    private Message first = null;

    void setSource(Iterator<Tuple2<?, Either<NullValue, Message>>> source) {
        this.source = source;
    }

    void setFirst(Message msg) {
        this.first = msg;
    }

    @Override
    public final boolean hasNext() {
        if (first != null) {
            return true;
        } else {
            return ((this.source != null) && (this.source.hasNext()));
        }
    }

    @Override
    public final Message next() {
        if (first != null) {
            Message toReturn = first;
            first = null;
            return toReturn;
        }
        return this.source.next().f1.right();
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Message> iterator() {
        return this;
    }
}
