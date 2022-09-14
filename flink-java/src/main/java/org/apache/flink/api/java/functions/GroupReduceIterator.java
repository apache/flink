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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Base class that simplifies reducing all values provided as {@link Iterable}.
 *
 * @param <IN>
 * @param <OUT>
 */
@PublicEvolving
public abstract class GroupReduceIterator<IN, OUT> extends RichGroupReduceFunction<IN, OUT> {

    private static final long serialVersionUID = 1L;

    public abstract Iterator<OUT> reduceGroup(Iterable<IN> values) throws Exception;

    // -------------------------------------------------------------------------------------------

    @Override
    public final void reduce(Iterable<IN> values, Collector<OUT> out) throws Exception {
        for (Iterator<OUT> iter = reduceGroup(values); iter.hasNext(); ) {
            out.collect(iter.next());
        }
    }
}
