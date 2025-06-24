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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Rich variant of the {@link AggregateFunction}. As a {@link RichFunction}, it gives access to the
 * {@link RuntimeContext} and provides setup and teardown methods: {@link
 * RichFunction#open(OpenContext)} and {@link RichFunction#close()}.
 *
 * @see AggregateFunction
 * @param <IN> The type of the values that are aggregated (input values)
 * @param <ACC> The type of the accumulator (intermediate aggregate state).
 * @param <OUT> The type of the aggregated result
 */
@PublicEvolving
public abstract class RichAggregateFunction<IN, ACC, OUT> extends AbstractRichFunction
        implements AggregateFunction<IN, ACC, OUT> {

    private static final long serialVersionUID = 1L;

    @Override
    public abstract ACC createAccumulator();

    @Override
    public abstract ACC add(IN value, ACC accumulator);

    @Override
    public abstract OUT getResult(ACC accumulator);

    @Override
    public abstract ACC merge(ACC a, ACC b);
}
