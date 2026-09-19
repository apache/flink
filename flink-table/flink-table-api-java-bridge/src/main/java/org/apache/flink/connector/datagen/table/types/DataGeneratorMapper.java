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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.util.function.SerializableFunction;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Maps the output of an upstream {@link GeneratorFunction} through a {@link SerializableFunction}.
 */
@Internal
public class DataGeneratorMapper<A, B> implements GeneratorFunction<Long, B> {

    private static final long serialVersionUID = 1L;

    private final GeneratorFunction<Long, A> generator;

    private final SerializableFunction<A, B> mapper;

    private final float nullRate;

    public DataGeneratorMapper(
            GeneratorFunction<Long, A> generator,
            SerializableFunction<A, B> mapper,
            float nullRate) {
        this.generator = generator;
        this.mapper = mapper;
        this.nullRate = nullRate;
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        generator.open(readerContext);
    }

    @Override
    public B map(Long value) throws Exception {
        if (nullRate == 0f || ThreadLocalRandom.current().nextFloat() > nullRate) {
            return mapper.apply(generator.map(value));
        }
        return null;
    }
}
