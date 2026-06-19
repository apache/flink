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
package org.apache.flink.runtime.operators.util.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

public class CountingMutableObjectIterator<IN> implements MutableObjectIterator<IN> {
    private final MutableObjectIterator<IN> iterator;
    private final Counter numRecordsIn;

    public CountingMutableObjectIterator(MutableObjectIterator<IN> iterator, Counter numRecordsIn) {
        this.iterator = iterator;
        this.numRecordsIn = numRecordsIn;
    }

    @Override
    public IN next(IN reuse) throws IOException {
        IN next = iterator.next(reuse);
        if (next != null) {
            numRecordsIn.inc();
        }
        return next;
    }

    @Override
    public IN next() throws IOException {
        IN next = iterator.next();
        if (next != null) {
            numRecordsIn.inc();
        }
        return next;
    }
}
