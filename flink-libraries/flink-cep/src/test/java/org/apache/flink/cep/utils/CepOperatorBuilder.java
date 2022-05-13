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

package org.apache.flink.cep.utils;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.operator.CepOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Test builder for cep operator that accepts {@link Event} as input elements.
 *
 * @param <OUT> type of output elements
 */
public class CepOperatorBuilder<OUT> {

    private final boolean isProcessingTime;
    private final NFACompiler.NFAFactory<Event> nfaFactory;
    private final EventComparator<Event> comparator;
    private final AfterMatchSkipStrategy skipStrategy;
    private final PatternProcessFunction<Event, OUT> function;
    private final OutputTag<Event> lateDataOutputTag;

    public static CepOperatorBuilder<Map<String, List<Event>>> createOperatorForNFA(
            NFA<Event> nfa) {
        return new CepOperatorBuilder<>(
                true,
                new NFACompiler.NFAFactory<Event>() {
                    @Override
                    public NFA<Event> createNFA() {
                        return nfa;
                    }
                },
                null,
                null,
                new PatternProcessFunction<Event, Map<String, List<Event>>>() {
                    private static final long serialVersionUID = -7143807777582726991L;

                    @Override
                    public void processMatch(
                            Map<String, List<Event>> match,
                            Context ctx,
                            Collector<Map<String, List<Event>>> out)
                            throws Exception {
                        out.collect(match);
                    }
                },
                null);
    }

    public static CepOperatorBuilder<Map<String, List<Event>>> createOperatorForNFAFactory(
            NFACompiler.NFAFactory<Event> nfaFactory) {
        return new CepOperatorBuilder<>(
                true,
                nfaFactory,
                null,
                null,
                new PatternProcessFunction<Event, Map<String, List<Event>>>() {
                    private static final long serialVersionUID = -7143807777582726991L;

                    @Override
                    public void processMatch(
                            Map<String, List<Event>> match,
                            Context ctx,
                            Collector<Map<String, List<Event>>> out)
                            throws Exception {
                        out.collect(match);
                    }
                },
                null);
    }

    private CepOperatorBuilder(
            boolean isProcessingTime,
            NFACompiler.NFAFactory<Event> nfaFactory,
            EventComparator<Event> comparator,
            AfterMatchSkipStrategy skipStrategy,
            PatternProcessFunction<Event, OUT> processFunction,
            OutputTag<Event> lateDataOutputTag) {
        this.isProcessingTime = isProcessingTime;
        this.nfaFactory = nfaFactory;
        this.comparator = comparator;
        this.skipStrategy = skipStrategy;
        function = processFunction;
        this.lateDataOutputTag = lateDataOutputTag;
    }

    public CepOperatorBuilder<OUT> inProcessingTime() {
        return new CepOperatorBuilder<>(
                true, nfaFactory, comparator, skipStrategy, function, lateDataOutputTag);
    }

    public CepOperatorBuilder<OUT> inEventTime() {
        return new CepOperatorBuilder<>(
                false, nfaFactory, comparator, skipStrategy, function, lateDataOutputTag);
    }

    public CepOperatorBuilder<OUT> withComparator(EventComparator<Event> comparator) {
        return new CepOperatorBuilder<>(
                false, nfaFactory, comparator, skipStrategy, function, lateDataOutputTag);
    }

    public CepOperatorBuilder<OUT> withSkipStrategy(AfterMatchSkipStrategy skipStrategy) {
        return new CepOperatorBuilder<>(
                false, nfaFactory, comparator, skipStrategy, function, lateDataOutputTag);
    }

    public CepOperatorBuilder<OUT> withLateDataOutputTag(OutputTag<Event> lateDataOutputTag) {
        return new CepOperatorBuilder<>(
                false, nfaFactory, comparator, skipStrategy, function, lateDataOutputTag);
    }

    public CepOperatorBuilder<OUT> withNFA(NFA<Event> nfa) {
        return new CepOperatorBuilder<>(
                false,
                new NFACompiler.NFAFactory<Event>() {
                    @Override
                    public NFA<Event> createNFA() {
                        return nfa;
                    }
                },
                comparator,
                skipStrategy,
                function,
                lateDataOutputTag);
    }

    public <T> CepOperatorBuilder<T> withFunction(
            PatternProcessFunction<Event, T> processFunction) {
        return new CepOperatorBuilder<>(
                isProcessingTime,
                nfaFactory,
                comparator,
                skipStrategy,
                processFunction,
                lateDataOutputTag);
    }

    public <K> CepOperator<Event, K, OUT> build() {
        return new CepOperator<>(
                Event.createTypeSerializer(),
                isProcessingTime,
                nfaFactory,
                comparator,
                skipStrategy,
                function,
                lateDataOutputTag);
    }
}
