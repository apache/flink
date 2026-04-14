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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedProcessTableRunner;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;

import java.util.Arrays;
import java.util.List;

/** The factory for subclasses of {@link AbstractProcessTableOperator}. */
public class ProcessTableOperatorFactory extends AbstractStreamOperatorFactory<RowData> {

    private static final long serialVersionUID = 1L;

    private final List<RuntimeTableSemantics> tableSemantics;
    private final List<RuntimeStateInfo> stateInfos;
    private final GeneratedRecordComparator[] generatedOrderByComparators;
    private final GeneratedProcessTableRunner generatedProcessTableRunner;
    private final GeneratedHashFunction[] generatedStateHashCode;
    private final GeneratedRecordEqualiser[] generatedStateEquals;
    private final RuntimeChangelogMode producedChangelogMode;
    private final List<StateTtlConfig> inputBufferTtlConfigs;

    public ProcessTableOperatorFactory(
            List<RuntimeTableSemantics> tableSemantics,
            List<RuntimeStateInfo> stateInfos,
            GeneratedRecordComparator[] generatedOrderByComparators,
            GeneratedProcessTableRunner generatedProcessTableRunner,
            GeneratedHashFunction[] generatedStateHashCode,
            GeneratedRecordEqualiser[] generatedStateEquals,
            RuntimeChangelogMode producedChangelogMode,
            List<StateTtlConfig> inputBufferTtlConfigs) {
        this.tableSemantics = tableSemantics;
        this.stateInfos = stateInfos;
        this.generatedOrderByComparators = generatedOrderByComparators;
        this.generatedProcessTableRunner = generatedProcessTableRunner;
        this.generatedStateHashCode = generatedStateHashCode;
        this.generatedStateEquals = generatedStateEquals;
        this.producedChangelogMode = producedChangelogMode;
        this.inputBufferTtlConfigs = inputBufferTtlConfigs;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public StreamOperator createStreamOperator(StreamOperatorParameters parameters) {
        final ClassLoader classLoader = parameters.getContainingTask().getUserCodeClassLoader();
        final RecordComparator[] orderByComparators =
                Arrays.stream(generatedOrderByComparators)
                        .map(g -> g == null ? null : g.newInstance(classLoader))
                        .toArray(RecordComparator[]::new);
        final ProcessTableRunner runner = generatedProcessTableRunner.newInstance(classLoader);
        final HashFunction[] stateHashCode =
                Arrays.stream(generatedStateHashCode)
                        .map(g -> g.newInstance(classLoader))
                        .toArray(HashFunction[]::new);
        final RecordEqualiser[] stateEquals =
                Arrays.stream(generatedStateEquals)
                        .map(g -> g.newInstance(classLoader))
                        .toArray(RecordEqualiser[]::new);
        if (tableSemantics.stream().anyMatch(RuntimeTableSemantics::hasSetSemantics)) {
            return new ProcessSetTableOperator(
                    parameters,
                    tableSemantics,
                    stateInfos,
                    orderByComparators,
                    runner,
                    stateHashCode,
                    stateEquals,
                    producedChangelogMode,
                    inputBufferTtlConfigs);
        } else {
            return new ProcessRowTableOperator(
                    parameters,
                    tableSemantics,
                    stateInfos,
                    runner,
                    stateHashCode,
                    stateEquals,
                    producedChangelogMode);
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        if (tableSemantics.stream().anyMatch(RuntimeTableSemantics::hasSetSemantics)) {
            return ProcessSetTableOperator.class;
        } else {
            return ProcessRowTableOperator.class;
        }
    }
}
