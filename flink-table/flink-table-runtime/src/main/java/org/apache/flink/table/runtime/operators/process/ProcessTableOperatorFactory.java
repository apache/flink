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
import org.apache.flink.table.runtime.generated.GeneratedProcessTableRunner;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;

import javax.annotation.Nullable;

/** The factory of {@link ProcessTableOperator}. */
public class ProcessTableOperatorFactory extends AbstractStreamOperatorFactory<RowData>
        implements OneInputStreamOperatorFactory<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final @Nullable RuntimeTableSemantics tableSemantics;
    private final GeneratedProcessTableRunner generatedProcessTableRunner;

    public ProcessTableOperatorFactory(
            @Nullable RuntimeTableSemantics tableSemantics,
            GeneratedProcessTableRunner generatedProcessTableRunner) {
        this.tableSemantics = tableSemantics;
        this.generatedProcessTableRunner = generatedProcessTableRunner;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public StreamOperator createStreamOperator(StreamOperatorParameters parameters) {
        final ProcessTableRunner runner =
                generatedProcessTableRunner.newInstance(
                        parameters.getContainingTask().getUserCodeClassLoader());
        return new ProcessTableOperator(parameters, tableSemantics, runner);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return ProcessTableOperator.class;
    }
}
