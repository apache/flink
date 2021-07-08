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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Needed to wrap tuples to {@code Tuple3<groupKey, sortKey, value>} for combine method of group
 * reduce with key selector sorting.
 */
@Internal
public class Tuple3WrappingCollector<IN, K1, K2> implements Collector<IN>, java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final Tuple3UnwrappingIterator<IN, K1, K2> tui;
    private final Tuple3<K1, K2, IN> outTuple;

    private Collector<Tuple3<K1, K2, IN>> wrappedCollector;

    public Tuple3WrappingCollector(Tuple3UnwrappingIterator<IN, K1, K2> tui) {
        this.tui = tui;
        this.outTuple = new Tuple3<K1, K2, IN>();
    }

    public void set(Collector<Tuple3<K1, K2, IN>> wrappedCollector) {
        this.wrappedCollector = wrappedCollector;
    }

    @Override
    public void close() {
        this.wrappedCollector.close();
    }

    @Override
    public void collect(IN record) {
        this.outTuple.f0 = this.tui.getLastGroupKey();
        this.outTuple.f1 = this.tui.getLastSortKey();
        this.outTuple.f2 = record;
        this.wrappedCollector.collect(outTuple);
    }
}
