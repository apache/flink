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

package org.apache.flink.cep.nfa.aftermatch;

import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

abstract class SkipToElementStrategy extends AfterMatchSkipStrategy {
    private static final long serialVersionUID = 7127107527654629026L;
    private final String patternName;
    private final boolean shouldThrowException;

    SkipToElementStrategy(String patternName, boolean shouldThrowException) {
        this.patternName = checkNotNull(patternName);
        this.shouldThrowException = shouldThrowException;
    }

    @Override
    public boolean isSkipStrategy() {
        return true;
    }

    @Override
    protected boolean shouldPrune(EventId startEventID, EventId pruningId) {
        return startEventID != null && startEventID.compareTo(pruningId) < 0;
    }

    @Override
    protected EventId getPruningId(Collection<Map<String, List<EventId>>> match) {
        EventId pruningId = null;
        for (Map<String, List<EventId>> resultMap : match) {
            List<EventId> pruningPattern = resultMap.get(patternName);
            if (pruningPattern == null || pruningPattern.isEmpty()) {
                if (shouldThrowException) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Could not skip to %s. No such element in the found match %s",
                                    patternName, resultMap));
                }
            } else {
                pruningId = max(pruningId, pruningPattern.get(getIndex(pruningPattern.size())));
            }

            if (shouldThrowException) {
                EventId startEvent =
                        resultMap.values().stream()
                                .flatMap(Collection::stream)
                                .min(EventId::compareTo)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Cannot prune based on empty match"));

                if (pruningId != null && pruningId.equals(startEvent)) {
                    throw new FlinkRuntimeException("Could not skip to first element of a match.");
                }
            }
        }

        return pruningId;
    }

    @Override
    public Optional<String> getPatternName() {
        return Optional.of(patternName);
    }

    /**
     * Tells which element from the list of events mapped to *PatternName* to use.
     *
     * @param size number of elements mapped to the *PatternName*
     * @return index of event mapped to *PatternName* to use for pruning
     */
    abstract int getIndex(int size);

    /**
     * Enables throwing exception if no events mapped to the *PatternName*. If not enabled and no
     * events were mapped, {@link NoSkipStrategy} will be used
     */
    public abstract SkipToElementStrategy throwExceptionOnMiss();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SkipToElementStrategy that = (SkipToElementStrategy) o;
        return shouldThrowException == that.shouldThrowException
                && Objects.equals(patternName, that.patternName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(patternName, shouldThrowException);
    }
}
