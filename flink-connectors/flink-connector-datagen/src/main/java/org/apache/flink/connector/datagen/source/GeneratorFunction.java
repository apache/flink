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

package org.apache.flink.connector.datagen.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.connector.source.SourceReaderContext;

/**
 * Base interface for data generator functions. Data generator functions take elements and transform
 * them, element-wise. They are the core building block of the {@link DataGeneratorSource} that
 * drives the data generation process by supplying "index" values of type Long. It makes it possible
 * to produce specific elements at concrete positions of the generated data stream.
 *
 * <p>Example:
 *
 * <pre>{@code
 * GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
 * DataGeneratorSource<String> source =
 *         new DataGeneratorSource<>(generatorFunction, 1000, Types.STRING);
 * }</pre>
 *
 * @param <T> Type of the input elements.
 * @param <O> Type of the returned elements.
 */
@Experimental
public interface GeneratorFunction<T, O> extends Function {

    /**
     * Initialization method for the function. It is called once before the actual data mapping
     * methods.
     */
    default void open(SourceReaderContext readerContext) throws Exception {}

    /** Tear-down method for the function. */
    default void close() throws Exception {}

    O map(T value) throws Exception;
}
