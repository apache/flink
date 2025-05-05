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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedProcessTableRunner;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordEqualiser;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

/** The factory of {@link ProcessTableOperator}. */
public class ProcessTableOperatorFactory extends AbstractStreamOperatorFactory<RowData>
        implements OneInputStreamOperatorFactory<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final @Nullable RuntimeTableSemantics tableSemantics;
    private final List<RuntimeStateInfo> stateInfos;
    private final GeneratedProcessTableRunner generatedProcessTableRunner;
    private final GeneratedHashFunction[] generatedStateHashCode;
    private final GeneratedRecordEqualiser[] generatedStateEquals;
    private final RuntimeChangelogMode producedChangelogMode;

    public ProcessTableOperatorFactory(
            @Nullable RuntimeTableSemantics tableSemantics,
            List<RuntimeStateInfo> stateInfos,
            GeneratedProcessTableRunner generatedProcessTableRunner,
            GeneratedHashFunction[] generatedStateHashCode,
            GeneratedRecordEqualiser[] generatedStateEquals,
            RuntimeChangelogMode producedChangelogMode) {
        this.tableSemantics = tableSemantics;
        this.stateInfos = stateInfos;
        this.generatedProcessTableRunner = generatedProcessTableRunner;
        this.generatedStateHashCode = generatedStateHashCode;
        this.generatedStateEquals = generatedStateEquals;
        this.producedChangelogMode = producedChangelogMode;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public StreamOperator createStreamOperator(StreamOperatorParameters parameters) {
        final ClassLoader classLoader = parameters.getContainingTask().getUserCodeClassLoader();
        final ProcessTableRunner runner = generatedProcessTableRunner.newInstance(classLoader);
        final HashFunction[] stateHashCode =
                Arrays.stream(generatedStateHashCode)
                        .map(g -> g.newInstance(classLoader))
                        .toArray(HashFunction[]::new);
        final RecordEqualiser[] stateEquals =
                Arrays.stream(generatedStateEquals)
                        .map(g -> g.newInstance(classLoader))
                        .toArray(RecordEqualiser[]::new);
        return new ProcessTableOperator(
                parameters,
                tableSemantics,
                stateInfos,
                runner,
                stateHashCode,
                stateEquals,
                producedChangelogMode);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return ProcessTableOperator.class;
    }
}
