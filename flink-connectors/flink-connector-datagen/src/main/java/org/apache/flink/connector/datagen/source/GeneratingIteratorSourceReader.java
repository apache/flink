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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReaderBase;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code SourceReader} that takes the values of an iterator, supplied via an {@link
 * IteratorSourceSplit}, and applies a {@link GeneratorFunction} to them to perform arbitrary
 * transformations.
 */
@Experimental
public class GeneratingIteratorSourceReader<
                E, O, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
        extends IteratorSourceReaderBase<E, O, IterT, SplitT> {

    private final GeneratorFunction<E, O> generatorFunction;

    public GeneratingIteratorSourceReader(
            SourceReaderContext context, GeneratorFunction<E, O> generatorFunction) {
        super(context);
        this.generatorFunction = checkNotNull(generatorFunction);
    }

    // ------------------------------------------------------------------------

    @Override
    protected O convert(E value) {
        try {
            return generatorFunction.map(value);
        } catch (Exception e) {
            String message =
                    String.format(
                            "A user-provided generator function threw an exception on this input: %s",
                            value.toString());
            throw new FlinkRuntimeException(message, e);
        }
    }

    @Override
    public void start(SourceReaderContext context) {
        try {
            generatorFunction.open(context);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the GeneratorFunction", e);
        }
    }

    @Override
    public void close() throws Exception {
        generatorFunction.close();
        super.close();
    }
}
