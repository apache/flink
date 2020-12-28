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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/** Utility methods for creating test {@link CepOperator}. */
public class CepOperatorTestUtilities {

    private static class TestKeySelector implements KeySelector<Event, Integer> {

        private static final long serialVersionUID = -4873366487571254798L;

        @Override
        public Integer getKey(Event value) throws Exception {
            return value.getId();
        }
    }

    public static <T> OneInputStreamOperatorTestHarness<Event, T> getCepTestHarness(
            CepOperator<Event, Integer, T> cepOperator) throws Exception {
        KeySelector<Event, Integer> keySelector = new TestKeySelector();

        return new KeyedOneInputStreamOperatorTestHarness<>(
                cepOperator, keySelector, BasicTypeInfo.INT_TYPE_INFO);
    }

    public static <K> CepOperator<Event, K, Map<String, List<Event>>> getKeyedCepOpearator(
            boolean isProcessingTime, NFACompiler.NFAFactory<Event> nfaFactory) {

        return getKeyedCepOpearator(isProcessingTime, nfaFactory, null);
    }

    public static <K> CepOperator<Event, K, Map<String, List<Event>>> getKeyedCepOpearator(
            boolean isProcessingTime,
            NFACompiler.NFAFactory<Event> nfaFactory,
            EventComparator<Event> comparator) {

        return getKeyedCepOpearator(isProcessingTime, nfaFactory, comparator, null);
    }

    public static <K> CepOperator<Event, K, Map<String, List<Event>>> getKeyedCepOpearator(
            boolean isProcessingTime,
            NFACompiler.NFAFactory<Event> nfaFactory,
            EventComparator<Event> comparator,
            OutputTag<Event> outputTag) {

        return new CepOperator<>(
                Event.createTypeSerializer(),
                isProcessingTime,
                nfaFactory,
                comparator,
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
                outputTag);
    }

    private CepOperatorTestUtilities() {}
}
