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

package org.apache.flink.connector.kafka.sink;

import java.io.Closeable;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class Recyclable<T> implements Closeable {
    private T object;
    private final Consumer<T> recycler;

    public Recyclable(T object, Consumer<T> recycler) {
        this.object = checkNotNull(object);
        this.recycler = checkNotNull(recycler);
    }

    public T getObject() {
        checkState(!isRecycled(), "Already recycled");
        return object;
    }

    boolean isRecycled() {
        return object == null;
    }

    @Override
    public void close() {
        recycler.accept(object);
        object = null;
    }
}
