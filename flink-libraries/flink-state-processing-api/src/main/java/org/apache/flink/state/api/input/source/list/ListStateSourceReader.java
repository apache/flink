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

package org.apache.flink.state.api.input.source.list;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.source.RichSourceReaderContext;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.input.source.common.OperatorStateSourceReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link SourceReader} implementation that reads data from list state. */
public class ListStateSourceReader<OUT> extends OperatorStateSourceReader<OUT> {

    private final ListStateDescriptor<OUT> descriptor;

    public ListStateSourceReader(
            RichSourceReaderContext sourceReaderContext,
            @Nullable StateBackend stateBackend,
            OperatorState operatorState,
            Configuration configuration,
            ExecutionConfig executionConfig,
            ListStateDescriptor<OUT> descriptor)
            throws IOException {
        super(sourceReaderContext, stateBackend, operatorState, configuration, executionConfig);
        this.descriptor = descriptor;
    }

    @Override
    protected Iterable<OUT> getElements(OperatorStateBackend restoredBackend) throws Exception {
        return restoredBackend.getListState(descriptor).get();
    }
}
