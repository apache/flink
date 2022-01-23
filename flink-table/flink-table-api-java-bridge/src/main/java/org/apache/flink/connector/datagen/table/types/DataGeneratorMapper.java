/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.datagen.table.types;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.util.function.SerializableFunction;

/** Utility for mapping the output of a {@link DataGenerator}. */
@Internal
public class DataGeneratorMapper<A, B> implements DataGenerator<B> {

    private final DataGenerator<A> generator;

    private final SerializableFunction<A, B> mapper;

    public DataGeneratorMapper(DataGenerator<A> generator, SerializableFunction<A, B> mapper) {
        this.generator = generator;
        this.mapper = mapper;
    }

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        generator.open(name, context, runtimeContext);
    }

    @Override
    public boolean hasNext() {
        return generator.hasNext();
    }

    @Override
    public B next() {
        return mapper.apply(generator.next());
    }
}
