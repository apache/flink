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

package org.apache.flink.streaming.examples.statemachine.generator;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.examples.statemachine.event.Event;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A generator function that produces the events on the fly. Useful for self-contained demos. */
@SuppressWarnings("serial")
public class EventsGeneratorFunction implements GeneratorFunction<Long, Event> {

    private final double errorProbability;

    transient EventsGenerator generator;
    private int min;
    private int max;

    public EventsGeneratorFunction(double errorProbability) {
        checkArgument(
                errorProbability >= 0.0 && errorProbability <= 1.0,
                "error probability must be in [0.0, 1.0]");

        this.errorProbability = errorProbability;
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        final int range = Integer.MAX_VALUE / readerContext.currentParallelism();
        min = range * readerContext.getIndexOfSubtask();
        max = min + range;
        generator = new EventsGenerator(errorProbability);
    }

    @Override
    public Event map(Long value) throws Exception {
        return generator.next(min, max);
    }
}
