/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.util.CloseableIterator;

import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A pair of an {@link Iterator} to receive results from a streaming application and a {@link
 * JobClient} to interact with the program.
 */
@Internal
public final class ClientAndIterator<E> implements AutoCloseable {

    public final JobClient client;
    public final CloseableIterator<E> iterator;

    public ClientAndIterator(JobClient client, CloseableIterator<E> iterator) {
        this.client = checkNotNull(client);
        this.iterator = checkNotNull(iterator);
    }

    @Override
    public void close() throws Exception {
        iterator.close();
    }
}
