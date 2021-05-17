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

package org.apache.flink.test.windowing.sessionwindows;

import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Generator that employs several (sub-) event generators to produce events for multiple sessions in
 * parallel, i.e. events are emitted in an interleaved way.
 *
 * <p>Even events that belong to different sessions for the same key can be generated in parallel.
 *
 * <p>The watermark is computed as the minimum of watermarks among all current sub-generators.
 *
 * @param <K> session key type
 * @param <E> session event type
 */
public class ParallelSessionsEventGenerator<K, E> {

    // set of all possible keys for generated sessions
    private final Set<K> sessionKeys;

    // factory for generators that generate exactly one session
    private final EventGeneratorFactory<K, E> generatorFactory;

    // list of sub-generators for the current sessions
    private final List<EventGenerator<K, E>> subGeneratorLists;

    // pseudo random engine
    private final LongRandomGenerator randomGenerator;

    // maximum number of sessions this will generate
    private final long sessionCountLimit;

    public ParallelSessionsEventGenerator(
            Set<K> keys,
            EventGeneratorFactory<K, E> generatorFactory,
            int parallelSessions,
            long sessionCountLimit,
            LongRandomGenerator randomGenerator) {

        Preconditions.checkNotNull(keys);
        Preconditions.checkNotNull(generatorFactory);
        Preconditions.checkArgument(parallelSessions > 0);
        Preconditions.checkArgument(!keys.isEmpty());
        Preconditions.checkNotNull(randomGenerator);

        this.sessionKeys = keys;
        this.randomGenerator = randomGenerator;
        this.generatorFactory = generatorFactory;
        this.sessionCountLimit = sessionCountLimit;

        this.subGeneratorLists = new ArrayList<>(parallelSessions);
        initParallelSessionGenerators(parallelSessions);
    }

    /** @return the next generated event */
    public E nextEvent() {

        // the session limit is reached and all generators are exhausted
        if (subGeneratorLists.isEmpty()) {
            return null;
        }

        final long globalWatermark = getWatermark();

        // iterates at most once over all sub-generators, starting at a randomly chosen index to
        // find one that can
        // currently produce events
        final int choice = randomGenerator.choseRandomIndex(subGeneratorLists);

        for (int i = choice; i < choice + subGeneratorLists.size(); ++i) {

            final int index = i % subGeneratorLists.size();
            EventGenerator<K, E> subGenerator = subGeneratorLists.get(index);

            // check if the sub-generator can produce an event under the current global watermark
            if (subGenerator.canGenerateEventAtWatermark(globalWatermark)) {

                E event = subGenerator.generateEvent(globalWatermark);

                // check if the sub-generator produced it's last event
                if (!subGenerator.hasMoreEvents()) {

                    // replaces exhausted generator if the session limit is not met
                    if (generatorFactory.getProducedGeneratorsCount() < sessionCountLimit) {
                        subGeneratorLists.set(
                                index,
                                generatorFactory.newSessionGeneratorForKey(
                                        randomGenerator.chooseRandomElement(sessionKeys),
                                        getWatermark()));
                    } else {
                        // otherwise removes the sub-generator and shrinks the list of open sessions
                        // permanently
                        subGeneratorLists.remove(index);
                    }
                }
                return event;
            }
        }

        // if everything works correctly, this should never happen
        throw new IllegalStateException(
                "Unable to find an open sub-generator that can produce events");
    }

    /**
     * @return a global watermark that is the minimum of all individual watermarks of the
     *     sub-generators
     */
    public long getWatermark() {
        long watermark = Long.MAX_VALUE;

        for (EventGenerator<K, E> eventGenerator : subGeneratorLists) {
            watermark = Math.min(watermark, eventGenerator.getLocalWatermark());
        }
        return watermark;
    }

    /** @param parallelSessions the number of parallel sessions to initialize */
    private void initParallelSessionGenerators(int parallelSessions) {
        for (int i = 0;
                i < parallelSessions
                        && generatorFactory.getProducedGeneratorsCount() < sessionCountLimit;
                ++i) {
            subGeneratorLists.add(
                    generatorFactory.newSessionGeneratorForKey(
                            randomGenerator.chooseRandomElement(sessionKeys), 0L));
        }
    }
}
