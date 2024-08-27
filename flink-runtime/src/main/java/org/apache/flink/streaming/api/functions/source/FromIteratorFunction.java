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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Iterator;

/**
 * A {@link SourceFunction} that reads elements from an {@link Iterator} and emits them.
 *
 * @deprecated This class is based on the {@link
 *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
 *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
 */
@Deprecated
@PublicEvolving
public class FromIteratorFunction<T> implements SourceFunction<T> {

    private static final long serialVersionUID = 1L;

    private final Iterator<T> iterator;

    private volatile boolean isRunning = true;

    public FromIteratorFunction(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (isRunning && iterator.hasNext()) {
            ctx.collect(iterator.next());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
