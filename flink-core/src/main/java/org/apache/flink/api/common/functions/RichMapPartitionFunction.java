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

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Collector;

/**
 * Rich variant of the {@link MapPartitionFunction}. As a {@link RichFunction}, it gives access to
 * the {@link org.apache.flink.api.common.functions.RuntimeContext} and provides setup and teardown
 * methods: {@link RichFunction#open(org.apache.flink.configuration.Configuration)} and {@link
 * RichFunction#close()}.
 *
 * @param <I> Type of the input elements.
 * @param <O> Type of the returned elements.
 */
@Public
public abstract class RichMapPartitionFunction<I, O> extends AbstractRichFunction
        implements MapPartitionFunction<I, O> {

    private static final long serialVersionUID = 1L;

    @Override
    public abstract void mapPartition(Iterable<I> values, Collector<O> out) throws Exception;
}
